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
	"github.com/rs/zerolog"
)

// loadProgressEvery is how many records are applied between OnLoadProgress
// reports during warm-up. A large topic is worth reporting on; every record is
// not.
const loadProgressEvery = 100_000

// Loader owns the consumers, the warm-up and the shutdown for a set of bound
// topics.
//
// The usual sequence is: NewLoader, one Bind per topic, Start, then lookups
// against the returned stores for the life of the process, and Close on the way
// out. Start blocks until every topic has been read whole, so a service that
// returns from Start has complete configuration in memory.
//
// A Loader is safe for concurrent use. Bind is the exception: it must be called
// before Start, from the goroutine doing the wiring.
type Loader struct {
	cfg loaderConfig

	// mu guards the binding set and the run state below. started and cancel are
	// set together when Start begins, which is why one lock covers both.
	mu      sync.Mutex
	regs    []*registration
	byName  map[string]*registration
	started bool
	cancel  context.CancelFunc // nil until Start; nil again never — Close is idempotent

	ready     chan struct{}
	readyOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once

	// err uses an atomic rather than mu for its compare-and-swap: the first
	// reason the loader stopped is the one that explains the shutdown, and
	// later failures must not overwrite it. Err is also read by liveness
	// probes, which should not contend with Bind, Stats or LookupRaw.
	err       atomic.Pointer[error]
	closeOnce sync.Once

	wg sync.WaitGroup
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
		ready:  make(chan struct{}),
		done:   make(chan struct{}),
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

// Start connects one consumer per binding, reads every topic to its end, and
// returns once all of them are done.
//
// On success the stores are fully populated and the consumers keep running in
// the background applying live changes — Start returning is not the loader
// stopping. On failure it returns every binding's error joined together, having
// stopped all consumers, so one call reports all the misconfigured topics rather
// than the first.
//
// It can be called only once.
func (l *Loader) Start(ctx context.Context) error {
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
	// hence the atomics on their counters. Only the pointer list is private
	// here.
	//
	// That private list is what lets the lock be released before the slow work
	// below, which matters most for Close: it takes this same mutex, so holding
	// it across warm-up would deadlock — warm-up would wait for a cancellation
	// only Close could deliver.
	//
	// The copy itself is belt-and-braces today, since started == true makes
	// add panic and l.regs can no longer change. It keeps this correct anyway
	// if binding after Start is ever allowed, or made to return an error
	// instead of panicking.
	regs := slices.Clone(l.regs)
	l.mu.Unlock()

	// Read inside out: WithoutCancel copies ctx keeping its values but never
	// cancelling with it, then WithCancel wraps that in a context only we can
	// cancel. So runCtx carries whatever the caller attached, while its
	// cancellation belongs solely to stopConsumers.
	//
	// The two contexts bound two different lifetimes. The caller's ctx bounds
	// warm-up, which awaitWarmup watches; runCtx bounds the consumers, which
	// outlive this call and run until Close. Polling under the caller's ctx
	// instead would conflate them, so a caller who scoped Start — say
	// context.WithTimeout(ctx, 30*time.Second) as a guard against a broker that
	// never answers — would have every consumer stop when that timeout expired,
	// freezing the stores at their warm-up contents with nothing in the logs.
	//
	// The consequence is that cancelling the caller's ctx does not stop the
	// loader: Close does, and it is needed regardless, since cancelling only
	// signals whereas Close waits for the consumers to finish.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	l.mu.Lock()
	l.cancel = cancel
	l.mu.Unlock()

	// One slot per binding, so no binding can ever block sending its outcome.
	// See awaitWarmup for why that is required rather than merely tidy.
	outcomes := make(chan error, len(regs))
	for _, reg := range regs {
		l.startBinding(runCtx, reg, outcomes)
	}

	if err := l.awaitWarmup(ctx, runCtx, regs, outcomes); err != nil {
		return err
	}

	l.readyOnce.Do(func() { close(l.ready) })
	l.cfg.logger.Info().Int("bindings", len(regs)).Msg("configuration loaded, serving live updates")

	// From here the bindings run on their own. When the last one exits — a
	// clean Close, or a binding dying — the loader is stopped.
	go l.awaitStop()

	return nil
}

// startBinding creates the consumer for one binding and launches its goroutine.
// A consumer that cannot even be constructed is reported as that binding's
// warm-up outcome, so the failure lands with the other results rather than
// aborting Start halfway through the loop.
func (l *Loader) startBinding(ctx context.Context, reg *registration, outcomes chan<- error) {
	consumer, err := l.cfg.consumerFactory(l.cfg.consumerConfig(reg.topic))
	if err != nil {
		outcomes <- fmt.Errorf("binding %q: %w", reg.name, err)

		return
	}

	l.wg.Go(func() { l.run(ctx, reg, consumer, outcomes) })
}

// awaitWarmup waits for every binding to report, and turns any failure into a
// stopped loader.
//
// Every binding sends exactly one outcome — nil once its topic has been read
// whole, or an error: a consumer that could not be built, a fatal Kafka error,
// or an empty topic the binding did not permit. A non-fatal Kafka error sends
// nothing, since librdkafka recovers from those on its own.
//
// The loop therefore runs exactly once per binding and accumulates errors
// rather than returning on the first. That is deliberate: waiting for every
// outcome is what lets one run report every misconfigured topic instead of
// whichever failed first. Only after the last binding has reported does a
// non-empty error list abort.
//
// Three things cut that short and abort without hearing from the rest: the
// caller's context being cancelled, the warm-up timeout expiring, or Close
// being called while warming up.
//
// Note that outcomes is buffered to one slot per binding, and that this matters
// on an early abort: bindings that had not yet reported will still send, while
// abortWarmup is in wg.Wait(). With an unbuffered channel those sends would
// block on a receiver that no longer exists, and the wait would deadlock
// against them. Sized to the maximum number of sends, no send can ever block.
func (l *Loader) awaitWarmup(
	callerCtx, runCtx context.Context,
	regs []*registration,
	outcomes <-chan error,
) error {

	var (
		errs    []error
		timeout <-chan time.Time
	)

	if l.cfg.warmupTimeout > 0 {
		timer := time.NewTimer(l.cfg.warmupTimeout)
		defer timer.Stop()
		timeout = timer.C
	}

	for range regs {
		select {
		case err := <-outcomes:
			if err != nil {
				errs = append(errs, err)
			}

		case <-callerCtx.Done():
			errs = append(errs, fmt.Errorf("warm-up interrupted: %w", callerCtx.Err()))

			return l.abortWarmup(errs)

		case <-timeout:
			errs = append(errs, fmt.Errorf("%w after %s", ErrWarmupTimeout, l.cfg.warmupTimeout))

			return l.abortWarmup(errs)

		case <-runCtx.Done():
			// Close was called while warming up.
			errs = append(errs, fmt.Errorf("warm-up interrupted: %w", ErrClosed))

			return l.abortWarmup(errs)
		}
	}

	if len(errs) > 0 {
		return l.abortWarmup(errs)
	}

	return nil
}

// abortWarmup stops every binding and reports the joined failure. Ready is
// deliberately left open: a readiness probe must never report ready on a
// configuration that did not load.
func (l *Loader) abortWarmup(errs []error) error {
	joined := errors.Join(errs...)
	l.setErr(joined)
	l.stopConsumers()
	l.wg.Wait()
	l.finish()

	return fmt.Errorf("easykafkaconfig: warm-up failed: %w", joined)
}

// awaitStop closes Done once every binding has exited.
func (l *Loader) awaitStop() {
	l.wg.Wait()
	l.finish()
}

// run drives one binding: assign, warm up, then serve.
func (l *Loader) run(ctx context.Context, reg *registration, c driver.Consumer, outcomes chan<- error) {
	// The consumer is closed by the goroutine that polls it, so no other
	// goroutine has to coordinate with librdkafka's teardown.
	defer func() {
		reg.phase.Store(phaseStopped)
		if err := c.Close(); err != nil {
			l.cfg.logger.Warn().Err(err).Str("binding", reg.name).Msg("closing consumer")
		}
	}()

	logger := l.cfg.logger.With().Str("binding", reg.name).Str("topic", reg.topic).Logger()

	if err := l.warmup(ctx, reg, c, logger); err != nil {
		outcomes <- err

		return
	}
	outcomes <- nil

	l.serve(ctx, reg, c)
}

// warmup reads the topic to its end, as decided by the configured detector.
func (l *Loader) warmup(ctx context.Context, reg *registration, c driver.Consumer, logger zerolog.Logger) error {
	started := time.Now()

	partitions, err := c.AssignAll(ctx)
	if err != nil {
		return fmt.Errorf("binding %q: %w", reg.name, err)
	}

	detectionProgress, err := l.cfg.detector.begin(c, partitions)
	if err != nil {
		return fmt.Errorf("binding %q: preparing load detection: %w", reg.name, err)
	}

	timeout := l.cfg.detector.pollTimeout()
	progressAt := loadProgressEvery

	for {
		// Check for cancellation without waiting for it. The default case is
		// what makes this a check rather than a wait: without it, the select
		// would block on a signal that in normal operation never arrives.
		//
		// It has to happen here, between polls, because Poll takes no context —
		// it is a blocking call into librdkafka bounded only by its own timeout,
		// so nothing can interrupt one already in flight. Shutdown latency is
		// therefore up to one poll timeout, which is why the timeouts are short
		// and why Close is documented as taking that long to return.
		//
		// context.Cause rather than ctx.Err so that a reason attached by a
		// future WithCancelCause would surface; today they are the same, since
		// the context is a plain WithCancel.
		select {
		case <-ctx.Done():
			return fmt.Errorf("binding %q: %w", reg.name, context.Cause(ctx))
		default:
		}

		event := c.Poll(timeout)

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
	reg.phase.Store(phaseSteady)
	l.cfg.observer.OnPhase(reg.name, PhaseSteady, applied, took)
	logger.Info().Int("records", applied).Dur("took", took).Int("size", reg.size()).
		Msg("topic read to end")

	return nil
}

// serve applies live changes until the context is cancelled or the binding dies.
func (l *Loader) serve(ctx context.Context, reg *registration, c driver.Consumer) {
	for {
		// Non-blocking cancellation check between polls, as in warmup — see
		// there for why it cannot be done any other way.
		select {
		case <-ctx.Done():
			return
		default:
		}

		event := c.Poll(l.cfg.steadyPollTimeout)

		if failure, fatal := l.classify(reg, event); fatal {
			// The binding cannot recover, so its store would silently freeze at
			// whatever it last held. Reported and propagated: the whole loader
			// stops, Done closes, and a liveness probe can see it.
			err := fmt.Errorf("binding %q: %w", reg.name, failure)
			l.setErr(err)
			l.cfg.logger.Error().Err(err).Str("binding", reg.name).
				Msg("binding stopped after warm-up, configuration is now frozen")

			if l.cfg.onFatal != nil {
				l.cfg.onFatal(err)
			}
			l.stopConsumers()

			return
		}
		if rec, ok := event.(*driver.Record); ok {
			reg.apply(rec)
		}
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

// Ready is closed once every topic has been read whole.
//
// It is the channel form of a nil return from Start, for code that did not call
// Start itself — a readiness probe, or a bootstrap that runs Start in a
// goroutine so it can serve health checks while warming up. It closes only on
// success: a failed warm-up leaves it open forever, so a probe never reports
// ready on configuration that did not load.
func (l *Loader) Ready() <-chan struct{} {
	return l.ready
}

// Done is closed once the loader has stopped, whether from Close, a failed
// warm-up, or a binding dying afterwards. Read Err for which.
func (l *Loader) Done() <-chan struct{} {
	return l.done
}

// Err reports why the loader stopped: nil while running or after a clean Close,
// the joined failure after a failed warm-up, or the fatal error if a binding
// died once serving.
//
// A non-nil Err with Done closed is what a liveness probe should fail on — the
// stores are frozen at whatever they last held, and a process serving
// configuration that can no longer change is worse than one that restarts.
func (l *Loader) Err() error {
	if err := l.err.Load(); err != nil {
		return *err
	}

	return nil
}

// Close stops every consumer and waits for them, bounded by ctx.
//
// Idempotent, and safe at any point in the lifecycle including mid-warm-up. If
// ctx expires first, Close returns its error while the consumers keep shutting
// down in the background.
func (l *Loader) Close(ctx context.Context) error {
	l.closeOnce.Do(l.stopConsumers)

	stopped := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
		l.finish()

		return nil

	case <-ctx.Done():
		return fmt.Errorf("easykafkaconfig: close timed out: %w", ctx.Err())
	}
}

// stopConsumers cancels the context every binding polls under. A no-op before
// Start, so Close is safe at any point in the lifecycle.
func (l *Loader) stopConsumers() {
	l.mu.Lock()
	cancel := l.cancel
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// finish closes Done exactly once.
func (l *Loader) finish() {
	l.doneOnce.Do(func() { close(l.done) })
}

// setErr records the first reason the loader stopped. Later failures are
// logged by their own caller but do not overwrite the first, which is the one
// that explains the shutdown.
func (l *Loader) setErr(err error) {
	if err == nil {
		return
	}
	l.err.CompareAndSwap(nil, &err)
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
