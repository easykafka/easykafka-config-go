package easykafkaconfig

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
)

// loadProgressEvery is how many records are applied between OnLoadProgress
// reports during warm-up. A large topic is worth reporting on; every record is
// not.
const loadProgressEvery = 100_000

// Loader owns the consumers, the warm-up and the shutdown for a set of bound
// topics.
//
// The usual sequence is: NewLoader, one Bind per topic, Start, then lookups
// against the returned stores for the life of the process. Start blocks until
// every topic has been read whole, so a service that returns from Start has
// complete configuration in memory. To stop, cancel the context given to Start
// and call WaitUntilStopped.
//
// A Loader is safe for concurrent use. Bind is the exception: it must be called
// before Start, from the goroutine doing the wiring.
//
// # How the lifecycle is coordinated
//
// Warm-up finishes before serving starts, and that single property is what
// keeps the coordination small. There are two generations of goroutine, one per
// binding each, and they never overlap:
//
//   - warm-up, transient: each reads one topic to its end and returns its
//     outcome. Start waits for all of them with a local WaitGroup and collects
//     the outcomes from a pre-sized slice, so no channel is involved.
//   - serving, long-lived: each applies live changes until the loader stops.
//     These outlive Start and are counted by the serving WaitGroup, which is
//     what WaitUntilStopped waits on.
//
// Because generation one has exited before generation two exists, a consumer
// can be handed from one to the other with no lock, and the phase change needs
// no signal — it is the return of a WaitGroup's Wait.
//
// Stopping is likewise the caller's: there is no cancellation inside the loader
// and nothing to close. Cancelling the context passed to Start stops every
// binding, as does a fatal Kafka error, which stops them by recording itself in
// err.
type Loader struct {
	cfg loaderConfig

	// mu guards the binding set and the started flag. Held only briefly — for
	// the claim in Start, and for Bind, Stats and LookupRaw — and never across
	// a poll.
	mu      sync.Mutex
	regs    []*registration
	byName  map[string]*registration
	started bool

	// err does double duty. It is the first reason a binding died, reported by
	// Err; and it is the stop signal, because every serving goroutine tests it
	// in the between-poll check it already performs. An atomic rather than a
	// field under mu because it is read once per poll by every binding and by
	// liveness probes, so it must not contend with Bind or Stats. First writer
	// wins, so the reason kept is the one that explains the shutdown.
	err atomic.Pointer[error]

	// serving counts the serving goroutines, so WaitUntilStopped can tell when
	// the last one has exited and closed its consumer.
	serving sync.WaitGroup
}

// NewLoader builds a loader from the given options. It contacts no broker;
// that happens in Start.
func NewLoader(opts ...Option) (*Loader, error) {
	cfg, err := newLoaderConfig(opts...)
	if err != nil {
		return nil, fmt.Errorf("easykafkaconfig: %w", err)
	}

	return &Loader{
		cfg:    cfg,
		byName: make(map[string]*registration),
	}, nil
}

// add registers a binding, rejecting the two ways that can be wrong.
func (l *Loader) add(reg *registration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started {
		panic(fmt.Sprintf("easykafkaconfig: Bind(%q) after Start: %v", reg.name, ErrBindAfterStart))
	}
	if _, exists := l.byName[reg.name]; exists {
		panic(fmt.Sprintf("easykafkaconfig: duplicate binding name %q", reg.name))
	}

	l.regs = append(l.regs, reg)
	l.byName[reg.name] = reg
}

// Start builds one consumer per binding, reads every topic to its end, and
// returns once all of them are done.
//
// On success the stores are fully populated and the consumers keep running in
// the background applying live changes — Start returning is not the loader
// stopping. On failure it returns every binding's error joined together, having
// closed every consumer and started nothing, so one call reports all the
// misconfigured topics rather than the first.
//
// ctx must be the process context — the one cancelled on SIGINT/SIGTERM — and
// must not carry a deadline of its own. Wrapping it in a WithTimeout to guard
// startup stops every consumer when that timeout fires, freezing the stores at
// whatever they then held for the rest of the process's life. Use
// WithWarmupTimeout for that instead: it bounds warm-up alone and leaves
// serving on the caller's context.
//
// It can be called only once.
func (l *Loader) Start(ctx context.Context) error {
	// Check the loader has not been started, then mark it started, so that of
	// two concurrent Start calls exactly one proceeds and the other gets
	// ErrAlreadyStarted. Both steps have to happen under one lock or both calls
	// could pass the check before either sets the flag.
	//
	// Unlocking is deliberately explicit rather than deferred: the work below
	// blocks for as long as the topics take to read, and Bind, Stats and
	// LookupRaw take this same lock. A deferred unlock would hold it for the
	// whole of Start and stall all three. Mind this when adding a return
	// anywhere in the locked section.
	l.mu.Lock()

	if l.started {
		l.mu.Unlock()

		return fmt.Errorf("easykafkaconfig: %w", ErrAlreadyStarted)
	}
	if len(l.regs) == 0 {
		l.mu.Unlock()

		return fmt.Errorf("easykafkaconfig: %w", ErrNoBindings)
	}
	l.started = true

	// Copy the slice, not the registrations: the pointers still refer to the
	// same registrations, which every binding's goroutine keeps mutating —
	// hence the atomics on their counters. Only the pointer list is private.
	//
	// The copy is belt-and-braces today, since started == true makes add panic
	// and l.regs can no longer change. It keeps this correct anyway if binding
	// after Start is ever allowed, or made to return an error instead of
	// panicking.
	regs := slices.Clone(l.regs)

	l.mu.Unlock()

	// Build every consumer before starting anything. Construction is local —
	// librdkafka connects lazily, so nothing here blocks on a broker — which is
	// why it can be a plain sequential step, and why a bad configuration is
	// reported before a single goroutine or poll exists.
	consumers, err := l.newConsumers(regs)
	if err != nil {
		return fmt.Errorf("easykafkaconfig: %w", err)
	}

	// Bound warm-up, and warm-up only. Any deadline on this context is this
	// one, since Start requires a caller's ctx to carry none, which is what
	// lets warmUpBinding report a DeadlineExceeded as ErrWarmupTimeout.
	//
	// Zero means no bound, which is the default: the runtime is expected to
	// bound startup — a Kubernetes startup probe, say — and a library cannot
	// guess that budget.
	warmCtx := ctx
	if l.cfg.warmupTimeout > 0 {
		var cancel context.CancelFunc

		warmCtx, cancel = context.WithTimeout(ctx, l.cfg.warmupTimeout)

		// Nothing observes warmCtx by the time this runs — warm-up has finished
		// and serving polls under ctx — so this cancels no work. It releases
		// resources: the timer stays armed, and ctx keeps a reference to this
		// child, until the deadline passes or cancel is called. go vet's
		// lostcancel also requires it.
		defer cancel()
	}

	// Warm up every topic at once. Each goroutine writes its own slot, so the
	// writes touch disjoint memory and need no lock; Wait is the only
	// synchronisation, and it also publishes those writes — and everything the
	// goroutines did to their consumers and stores — to this goroutine, and so
	// to the serving goroutines started below.
	//
	// Every binding is heard out before anything is decided, rather than
	// returning on the first failure, so one Start reports every misconfigured
	// topic instead of whichever failed soonest.
	results := make([]error, len(regs))

	var warmup sync.WaitGroup
	for i := range regs {
		warmup.Go(func() { results[i] = l.warmUpBinding(warmCtx, regs[i], consumers[i]) })
	}
	warmup.Wait()

	// errors.Join returns nil for an all-nil slice, so this covers both exits.
	// Warm-up is all-or-nothing: one binding failing closes every consumer,
	// including those that loaded their topic perfectly, because a partially
	// loaded configuration is not something a caller can reason about.
	if err := errors.Join(results...); err != nil {
		l.closeAll(consumers)

		return fmt.Errorf("easykafkaconfig: warm-up failed: %w", err)
	}

	l.cfg.logger.Info().Int("bindings", len(regs)).Msg("configuration loaded, serving live updates")

	// Hand each consumer to a long-lived goroutine, now under the caller's
	// context rather than the warm-up one: these outlive Start and stop when
	// the application stops.
	for i := range regs {
		l.serving.Go(func() { l.serveBinding(ctx, regs[i], consumers[i]) })
	}

	return nil
}

// newConsumers builds one consumer per binding, reporting every construction
// failure rather than only the first.
//
// Nothing is polled and no goroutine exists yet, so on failure these consumers
// have no owner to close them — this does it, or they would leak their
// librdkafka threads for the life of the process.
func (l *Loader) newConsumers(regs []*registration) ([]driver.Consumer, error) {
	consumers := make([]driver.Consumer, 0, len(regs))

	var errs []error
	for _, reg := range regs {
		consumer, err := l.cfg.consumerFactory(l.cfg.consumerConfig(reg.topic))
		if err != nil {
			errs = append(errs, fmt.Errorf("binding %q: %w", reg.name, err))

			continue
		}
		consumers = append(consumers, consumer)
	}

	if err := errors.Join(errs...); err != nil {
		l.closeAll(consumers)

		return nil, err
	}

	return consumers, nil
}

// closeAll closes consumers that no goroutine owns, which is the case only
// before the handover in Start: either construction failed part way, or warm-up
// failed and generation one has already exited. Once a serving goroutine owns a
// consumer, that goroutine closes it and this must not.
func (l *Loader) closeAll(consumers []driver.Consumer) {
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			l.cfg.logger.Warn().Err(err).Msg("closing consumer after a failed start")
		}
	}
}

// warmUpBinding reads one topic to its end, as decided by the configured
// detector. It returns nil once the store is fully populated.
//
// It does not close the consumer on either path: until the handover in Start
// the consumers belong to Start, which closes them if any binding fails. This
// goroutine only borrows one.
func (l *Loader) warmUpBinding(ctx context.Context, reg *registration, c driver.Consumer) error {
	started := time.Now()
	logger := l.cfg.logger.With().Str("binding", reg.name).Str("topic", reg.topic).Logger()

	partitions, err := c.AssignAll(ctx)
	if err != nil {
		return fmt.Errorf("binding %q: %w", reg.name, l.nameTimeout(err))
	}

	// The detector may query the broker here, hence the error. It cannot happen
	// earlier: the partition list does not exist until assignment.
	//
	// detectionProgress rather than detection, because detection is the name of
	// the interface type in detect.go and a variable shadowing a type makes the
	// loop below harder to read than it needs to be.
	detectionProgress, err := l.cfg.detector.begin(c, partitions)
	if err != nil {
		return fmt.Errorf("binding %q: preparing load detection: %w", reg.name, err)
	}

	progressAt := loadProgressEvery

	for {
		// Cancellation is checked between polls, never waited for. It has to
		// happen here because Poll takes no context — it is a blocking call
		// into librdkafka bounded only by its own timeout, so nothing can
		// interrupt one already in flight. Shutdown latency is therefore up to
		// one poll timeout, which is why the timeouts are short.
		//
		// nameTimeout turns a deadline into ErrWarmupTimeout and leaves every
		// other error, cancellation included, as it is.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("binding %q: %w", reg.name, l.nameTimeout(err))
		}

		event := c.Poll(l.cfg.detector.pollTimeout())

		if failure, fatal := l.classify(reg, event); fatal {
			return fmt.Errorf("binding %q: %w", reg.name, failure)
		}

		if rec, ok := event.(*driver.Record); ok {
			reg.apply(rec)

			if applied := int(reg.warmupApplied.Load()); applied >= progressAt {
				progressAt = applied + loadProgressEvery
				l.cfg.observer.OnLoadProgress(reg.name, applied)
			}
		}

		if detectionProgress.observe(event) {
			break
		}
	}

	applied := int(reg.warmupApplied.Load())
	if applied == 0 && !reg.allowEmpty {
		// Almost always a deployment fault — the wrong topic name, or a topic
		// never populated — so it is reported rather than accepted. A topic
		// that may legitimately be empty sets AllowEmpty.
		return fmt.Errorf("binding %q on topic %q: %w", reg.name, reg.topic, ErrEmptyTopic)
	}

	took := time.Since(started)
	reg.warmupTookNs.Store(int64(took))

	// This store is what stops warmupApplied counting — noteRecord only
	// increments it while the phase is warm-up — so it has to happen after the
	// count above has been read.
	reg.phase.Store(phaseSteady)

	l.cfg.observer.OnPhase(reg.name, PhaseSteady, applied, took)
	logger.Info().Int("records", applied).Dur("took", took).Int("size", reg.size()).
		Msg("topic read to end")

	return nil
}

// nameTimeout reports the library's own error for a warm-up deadline, so a
// caller can test errors.Is(err, ErrWarmupTimeout) rather than matching on
// context.DeadlineExceeded.
//
// It supplies the identity; the layers above add the context, and %w at each
// step is what lets one error carry both:
//
//	easykafkaconfig: warm-up failed: binding "PlayerConfig": warm-up timed out after 200ms
//	                                 └─ context              └─ identity
//
// Start joins one of these per failing binding, so a single returned error can
// report several timeouts and, say, an empty topic at once, and errors.Is is
// true for each sentinel present in it.
//
// Any DeadlineExceeded reaching this point is the deadline Start derived,
// because Start requires the context it is given to carry none of its own. A
// caller who passes context.WithTimeout anyway gets their deadline reported as
// ErrWarmupTimeout, which is a mislabel — narrow enough to accept, given how
// much clearer this is than carrying a cause through driver calls that only
// ever return ctx.Err().
//
// Every other error, cancellation included, is returned untouched.
func (l *Loader) nameTimeout(err error) error {
	if l.cfg.warmupTimeout > 0 && errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s", ErrWarmupTimeout, l.cfg.warmupTimeout)
	}

	return err
}

// serveBinding applies live changes until the loader stops.
//
// It returns nothing: Start is long gone by now, so there is nobody to return
// to. A fatal error is recorded on the loader instead, which is also what stops
// the other bindings.
func (l *Loader) serveBinding(ctx context.Context, reg *registration, c driver.Consumer) {
	// This goroutine owns the consumer from here on, so it is the one that
	// closes it. Closing a consumer another goroutine may still be polling is
	// the one thing librdkafka handles worst.
	defer func() {
		reg.phase.Store(phaseStopped)
		if err := c.Close(); err != nil {
			l.cfg.logger.Warn().Err(err).Str("binding", reg.name).Msg("closing consumer")
		}
	}()

	for {
		// Two ways to stop, checked together because neither needs telling
		// apart here: the application is shutting down, or some binding —
		// possibly this one — hit a fatal error. Err distinguishes them
		// afterwards. This is why err is the stop signal as well as the reason.
		if ctx.Err() != nil || l.Err() != nil {
			return
		}

		event := c.Poll(l.cfg.steadyPollTimeout)

		if failure, fatal := l.classify(reg, event); fatal {
			l.bindingDied(fmt.Errorf("binding %q: %w", reg.name, failure))

			return
		}

		if rec, ok := event.(*driver.Record); ok {
			reg.apply(rec)
		}
	}
}

// bindingDied records the first fatal error and tells the application.
//
// Recording it is the whole mechanism: this cancels nothing and closes nothing,
// because every other binding tests Err between polls and follows this one down.
//
// Only the first caller gets past the compare-and-swap, so the reason kept is
// the one that explains the shutdown and the fatal handler runs exactly once
// however many bindings die together. Acting on the swap's result is the point —
// performing it and ignoring the answer would let the handler fire per binding.
func (l *Loader) bindingDied(err error) {
	if !l.err.CompareAndSwap(nil, &err) {
		return
	}

	l.cfg.logger.Error().Err(err).Msg("binding stopped after warm-up, configuration is now frozen")

	if l.cfg.onFatal != nil {
		l.cfg.onFatal(err)
	}
}

// classify reports Kafka errors to the observer and says whether the binding
// must stop. A non-fatal error is routine: librdkafka is reconnecting and the
// store keeps serving what it holds.
func (l *Loader) classify(reg *registration, event driver.Event) (error, bool) {
	failure, ok := event.(driver.Failure)
	if !ok {
		return nil, false
	}

	l.cfg.observer.OnKafkaError(reg.name, failure.Err)

	return failure.Err, failure.Fatal
}

// Err reports why the loader stopped: nil while serving and after a clean stop,
// the fatal error if a binding died once serving.
//
// A non-nil Err is what a liveness probe should fail on — the stores are frozen
// at whatever they last held, and a process serving configuration that can no
// longer change is worse than one that restarts. WithFatalHandler is the
// push-based equivalent.
func (l *Loader) Err() error {
	if err := l.err.Load(); err != nil {
		return *err
	}

	return nil
}

// WaitUntilStopped blocks until every consumer has stopped and been closed,
// then reports why the loader stopped: nil after a clean shutdown, the fatal
// error if Kafka took it down.
//
// It stops nothing. Cancelling the context passed to Start does that; this only
// observes that it happened, so calling it without cancelling blocks until the
// process ends. Returning establishes three things: no polling goroutine is
// running, every librdkafka handle has been released, and no store is being
// written any more — so shutdown cannot race a record still being applied.
//
// No timeout is needed or accepted: a serving goroutine notices cancellation
// between polls, so this returns within one steady poll timeout plus the
// consumer's own Close.
//
// Safe to call on a loader that was never started, or whose Start failed, in
// which case it returns immediately: nothing was ever running.
func (l *Loader) WaitUntilStopped() error {
	l.serving.Wait()

	return l.Err()
}

// Stats returns a snapshot of every binding, in the order they were bound.
func (l *Loader) Stats() []BindingStats {
	l.mu.Lock()
	regs := slices.Clone(l.regs)
	l.mu.Unlock()

	out := make([]BindingStats, 0, len(regs))
	for _, reg := range regs {
		out = append(out, reg.stats())
	}

	return out
}

// LookupRaw finds a value by binding name and undecoded key, returning it as
// any.
//
// Both halves of the name are literal: the key arrives raw, as a string from
// something like a URL query, and the value comes back raw rather than typed.
// It exists so a debug endpoint can serve every binding through one call
// instead of a switch over config types; business code uses its own typed
// store, where nothing is stringly typed and nothing is asserted.
func (l *Loader) LookupRaw(name, rawKey string) (any, bool, error) {
	l.mu.Lock()
	reg, ok := l.byName[name]
	l.mu.Unlock()

	if !ok {
		return nil, false, fmt.Errorf("%w: %q", ErrUnknownBinding, name)
	}

	return reg.lookup(rawKey)
}

// Bind registers a binding on the loader and returns the store it will fill.
//
// K and V are inferred from the Binding literal, so neither needs spelling out
// here. The returned store is usable immediately but empty until Start has
// warmed it up.
//
// Panics rather than returning an error, because every way this can fail is a
// programmer error that a running service cannot do anything about: an invalid
// binding, a duplicate name, or binding after Start. Panicking surfaces them at
// wiring time, where they are fixed.
func (l *Loader) Bind[K comparable, V any](b Binding[K, V]) *Store[K, V] {
	store := NewStore[K, V]()
	l.BindTo(b, store)

	return store
}

// BindTo is Bind against a store the caller already owns, for when a store is a
// field of an existing struct rather than a value to be returned.
func (l *Loader) BindTo[K comparable, V any](b Binding[K, V], store *Store[K, V]) {
	if store == nil {
		panic("easykafkaconfig: BindTo needs a non-nil store")
	}
	if err := b.Validate(); err != nil {
		panic(fmt.Sprintf("easykafkaconfig: %v", err))
	}

	reg := &registration{
		name:       b.Name,
		topic:      b.Topic,
		allowEmpty: b.AllowEmpty,
		size:       store.Len,
	}
	reg.apply = applyFunc(l.cfg.observer, b, store, reg)
	reg.lookup = lookupFunc(b, store)

	l.add(reg)
}
