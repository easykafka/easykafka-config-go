package unit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loaderWith builds a loader whose consumers come from the given fakes, keyed by
// topic, so a test can script each binding independently.
func loaderWith(t *testing.T, fakes map[string]*fakeConsumer, extra ...ekconfig.Option) *ekconfig.Loader {
	t.Helper()

	opts := append([]ekconfig.Option{
		ekconfig.WithBrokers("unused:9092"),
		ekconfig.WithConsumerFactory(func(cfg driver.Config) (driver.Consumer, error) {
			fake, ok := fakes[cfg.Topic]
			if !ok {
				return nil, errors.New("no fake registered for topic " + cfg.Topic)
			}

			return fake, nil
		}),
	}, extra...)

	loader, err := ekconfig.NewLoader(opts...)
	require.NoError(t, err)

	return loader
}

// stopLoader registers the shutdown every test needs: t.Context is cancelled
// just before cleanups run, which is what stops the consumers, and
// WaitUntilStopped then waits for them to exit and close.
//
// The reason is logged rather than asserted, because several tests deliberately
// end with a binding dead — there a non-nil return is the expected outcome, not
// a failure.
func stopLoader(t *testing.T, loader *ekconfig.Loader) {
	t.Helper()

	t.Cleanup(func() {
		if err := loader.WaitUntilStopped(); err != nil {
			t.Logf("loader stopped with: %v", err)
		}
	})
}

func playerBinding(name, topic string) ekconfig.Binding[string, playerConfig] {
	return ekconfig.Binding[string, playerConfig]{
		Name:        name,
		Topic:       topic,
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[playerConfig],
	}
}

// The core path: records are applied, end-of-partition completes warm-up, Start
// returns nil, and the store is populated by the time it does.
func TestLoaderWarmsUpAndPopulatesStore(t *testing.T) {
	t.Parallel()

	// The topic name has to agree in two places — the fake is registered under
	// it, and the binding asks for it — so a typo would show up as "no fake
	// registered for topic" rather than anything about the topic.
	const topic = "players"

	// Held in a variable rather than inlined so the event sequence the fake will
	// replay can be inspected in a debugger before Start runs.
	scripted := script(
		events(
			record(0, 0, "p1", `{"playerId":"p1","limit":10}`),
			record(1, 0, "p2", `{"playerId":"p2","limit":20}`),
		),
		eofAll(0, 1),
	)

	fake := newFakeConsumer([]int32{0, 1}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{topic: fake})
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	require.NoError(t, loader.Start(t.Context()))
	stopLoader(t, loader)

	assert.Equal(t, 2, players.Len(), "the store must be complete when Start returns")
	assert.Equal(t, 10, players.GetOrNil("p1").Limit)
	assert.Equal(t, 20, players.GetOrNil("p2").Limit)

	// Start returning nil is the readiness signal, and Err stays nil while the
	// loader is serving.
	assert.NoError(t, loader.Err())
}

// Warm-up only completes when every assigned partition has reported, so a
// partition still catching up cannot let Start return early with a partial
// store.
func TestLoaderWaitsForEveryPartition(t *testing.T) {
	t.Parallel()

	// EOF for partition 0 only: warm-up must not complete.
	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1"}`)),
		eofAll(0),
	)

	// Two partitions are assigned, but only one of them ever reports — despite
	// the helper's name, eofAll(0) emits an EOF for partition 0 alone, not for
	// all of them. So the detector waits on partition 1 forever and the timeout
	// above is what ends Start.
	fake := newFakeConsumer([]int32{0, 1}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake},
		ekconfig.WithWarmupTimeout(300*time.Millisecond))
	loader.Bind(playerBinding("PlayerConfig", "players"))

	err := loader.Start(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ekconfig.ErrWarmupTimeout)

	// A failed warm-up leaves nothing running and nothing open: no serving
	// goroutine was ever started, so waiting returns at once.
	require.NoError(t, loader.WaitUntilStopped(),
		"a loader that never served has nothing to wait for and no fatal error to report")
	assert.True(t, fake.isClosed(), "a failed warm-up must close every consumer")
}

// A tombstone deletes, which is the whole point of reading a compacted topic
// rather than replaying a log.
func TestLoaderAppliesTombstones(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(
			record(0, 0, "keep", `{"playerId":"keep"}`),
			record(0, 1, "gone", `{"playerId":"gone"}`),
			tombstone(0, 2, "gone"),
		),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	players := loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()))
	stopLoader(t, loader)

	assert.Equal(t, 1, players.Len())
	assert.True(t, players.Has("keep"))
	assert.False(t, players.Has("gone"), "the tombstone must have removed the key")
}

// An empty topic is reported rather than accepted, because it is nearly always
// a deployment fault. A binding that says otherwise is honoured.
func TestLoaderEmptyTopic(t *testing.T) {
	t.Parallel()

	t.Run("rejected by default", func(t *testing.T) {
		t.Parallel()

		// Nothing but end-of-partition: the topic holds no records at all.
		scripted := eofAll(0)

		fake := newFakeConsumer([]int32{0}, scripted...)
		loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
		loader.Bind(playerBinding("PlayerConfig", "players"))

		err := loader.Start(t.Context())
		require.Error(t, err)
		require.ErrorIs(t, err, ekconfig.ErrEmptyTopic)
		assert.Contains(t, err.Error(), "PlayerConfig", "the error must name the binding")
		assert.Contains(t, err.Error(), "players", "and the topic")
	})

	t.Run("allowed when the binding permits it", func(t *testing.T) {
		t.Parallel()

		scripted := eofAll(0)

		fake := newFakeConsumer([]int32{0}, scripted...)
		loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})

		binding := playerBinding("PlayerConfig", "players")
		binding.AllowEmpty = true
		players := loader.Bind(binding)

		// Needed here but not in the subtest above: warm-up succeeded, so a
		// serving goroutine is now polling and has to be waited for. A failed
		// warm-up starts none, which is why the other subtest omits this.
		require.NoError(t, loader.Start(t.Context()))
		stopLoader(t, loader)

		assert.Equal(t, 0, players.Len())
	})
}

// Every binding's failure is reported, not just the first, so one run tells you
// about all the misconfigured topics.
func TestLoaderJoinsFailuresAcrossBindings(t *testing.T) {
	t.Parallel()

	// Empty, and not allowed to be.
	firstScript := eofAll(0)

	// Fine.
	thirdScript := script(
		events(record(0, 0, "k", `{"playerId":"k"}`)),
		eofAll(0),
	)

	fakes := map[string]*fakeConsumer{
		"first": newFakeConsumer([]int32{0}, firstScript...),
		// Cannot even assign, so it has no script to run.
		"second": {partitions: []int32{0}, assignErr: errors.New("no such topic"), live: make(chan driver.Event)},
		"third":  newFakeConsumer([]int32{0}, thirdScript...),
	}

	loader := loaderWith(t, fakes)
	loader.Bind(playerBinding("First", "first"))
	loader.Bind(playerBinding("Second", "second"))
	loader.Bind(playerBinding("Third", "third"))

	err := loader.Start(t.Context())
	require.Error(t, err)

	msg := err.Error()
	assert.Contains(t, msg, "First")
	assert.Contains(t, msg, "Second")
	assert.NotContains(t, msg, "Third", "a binding that succeeded must not appear in the failure")
	assert.ErrorIs(t, err, ekconfig.ErrEmptyTopic)
}

// What the error wrapping buys, demonstrated end to end: one returned error is
// matchable per cause AND readable, for several bindings failing differently.
//
// This is the payoff of nameTimeout plus %w at every layer. Without the
// substitution a timeout would arrive as a bare context.DeadlineExceeded, so
// the first assertion would fail; with %v anywhere in the chain the message
// would read identically but every ErrorIs would fail. Nothing at runtime would
// reveal either, which is why it is asserted here.
func TestLoaderFailureIsBothIdentifiableAndDescriptive(t *testing.T) {
	t.Parallel()

	const warmupTimeout = 200 * time.Millisecond

	// Never reports EOF, so warm-up cannot finish: this one hits the deadline.
	slowScript := events(record(0, 0, "p1", `{"playerId":"p1"}`))

	// Reports EOF at once with nothing in it, and is not allowed to be empty: a
	// different cause, in the same run.
	blankScript := eofAll(0)

	fakes := map[string]*fakeConsumer{
		"slow":  newFakeConsumer([]int32{0}, slowScript...),
		"blank": newFakeConsumer([]int32{0}, blankScript...),
	}

	loader := loaderWith(t, fakes, ekconfig.WithWarmupTimeout(warmupTimeout))
	loader.Bind(playerBinding("SlowConfig", "slow"))
	loader.Bind(playerBinding("BlankConfig", "blank"))

	err := loader.Start(t.Context())
	require.Error(t, err)

	// Identity: each cause is matchable on its own, off the same error, so a
	// caller can branch per cause without parsing the message.
	//
	// assert rather than require, against the usual rule for error assertions:
	// these are independent lookups on one error with nothing to dereference
	// afterwards, and the whole point of the test is that several causes are
	// matchable at once — which only shows if all three are allowed to report.
	assert.ErrorIs(t, err, ekconfig.ErrWarmupTimeout, "the timeout must be matchable")        //nolint:testifylint // see above
	assert.ErrorIs(t, err, ekconfig.ErrEmptyTopic, "so must the unrelated failure beside it") //nolint:testifylint // see above
	assert.NotErrorIs(t, err, ekconfig.ErrNoBindings, "and nothing that did not happen")      //nolint:testifylint // see above

	// Context: the message says which binding, which topic, and how long — none
	// of which a bare sentinel could carry.
	msg := err.Error()
	assert.Contains(t, msg, "SlowConfig", "the timeout must name its binding")
	assert.Contains(t, msg, warmupTimeout.String(), "and say how long it waited")
	assert.Contains(t, msg, "BlankConfig", "the other failure must be reported too")
	assert.Contains(t, msg, "blank", "naming its topic")

	t.Logf("one error, two causes, both matchable:\n%v", err)
}

// Once warm-up is done the bindings keep applying changes, which is the reason
// the consumers stay running after Start returns.
func TestLoaderAppliesLiveUpdatesAfterWarmup(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1","limit":1}`)),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake},
		ekconfig.WithSteadyPollTimeout(10*time.Millisecond))
	players := loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()))
	stopLoader(t, loader)

	require.Equal(t, 1, players.GetOrNil("p1").Limit)

	fake.push(record(0, 1, "p1", `{"playerId":"p1","limit":99}`))
	fake.push(record(0, 2, "p2", `{"playerId":"p2","limit":5}`))

	require.Eventually(t, func() bool {
		v := players.GetOrNil("p1")

		return v != nil && v.Limit == 99 && players.Has("p2")
	}, 2*time.Second, 10*time.Millisecond, "live changes must reach the store")
}

// A binding dying after warm-up freezes its store, so the loader stops and says
// why — a process serving configuration that can no longer change should be
// restarted, not left running.
func TestLoaderFatalAfterWarmupStopsTheLoader(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1"}`)),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	var (
		mu      sync.Mutex
		fatals  []error
		handler = func(err error) {
			mu.Lock()
			defer mu.Unlock()
			fatals = append(fatals, err)
		}
	)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake},
		ekconfig.WithSteadyPollTimeout(10*time.Millisecond),
		ekconfig.WithFatalHandler(handler))
	players := loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()))
	require.Equal(t, 1, players.Len())

	boom := errors.New("broker gone for good")
	fake.push(driver.Failure{Err: boom, Fatal: true})

	// The dying binding records the reason, which stops every binding, so
	// waiting returns without the context being cancelled at all.
	stopped := make(chan error, 1)
	go func() { stopped <- loader.WaitUntilStopped() }()

	select {
	case err := <-stopped:
		require.ErrorIs(t, err, boom, "WaitUntilStopped must report why the loader went down")
	case <-time.After(2 * time.Second):
		t.Fatal("WaitUntilStopped must return when a binding dies after warm-up")
	}

	require.ErrorIs(t, loader.Err(), boom)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, fatals, 1, "the fatal handler must be called exactly once")
	require.ErrorIs(t, fatals[0], boom)

	// The store keeps serving what it already held.
	assert.Equal(t, 1, players.Len())
}

// A non-fatal error is routine — the broker is unreachable and librdkafka is
// reconnecting — so the binding must keep going rather than treating it as
// death.
func TestLoaderSurvivesNonFatalErrors(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(
			driver.Failure{Err: errors.New("all brokers down"), Fatal: false},
			record(0, 0, "p1", `{"playerId":"p1"}`),
			driver.Failure{Err: errors.New("still down"), Fatal: false},
		),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	players := loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()), "a recoverable error must not fail warm-up")
	stopLoader(t, loader)

	assert.Equal(t, 1, players.Len())
	assert.NoError(t, loader.Err())
}

// Cancelling the context Start was given is the only way to stop the loader,
// and WaitUntilStopped must then report a clean stop however often it is asked.
func TestLoaderStopsWhenContextCancelled(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1"}`)),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.Bind(playerBinding("PlayerConfig", "players"))

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, loader.Start(ctx))

	cancel()

	require.NoError(t, loader.WaitUntilStopped(), "a clean stop leaves no error")
	require.NoError(t, loader.WaitUntilStopped(), "waiting again must be a no-op")

	// Every consumer is closed by the time the wait returns — that guarantee is
	// the reason the method exists.
	assert.True(t, fake.isClosed(), "the binding's consumer must be closed")
	require.NoError(t, loader.Err(), "a clean stop leaves no error")
}

// Cancelling mid-warm-up must abort Start, which is when a service shutting
// down on a signal is most likely to do it.
func TestLoaderCancelDuringWarmup(t *testing.T) {
	t.Parallel()

	// Never reports EOF, so warm-up cannot finish on its own.
	scripted := events(record(0, 0, "p1", `{"playerId":"p1"}`))

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.Bind(playerBinding("PlayerConfig", "players"))

	ctx, cancel := context.WithCancel(t.Context())

	startErr := make(chan error, 1)
	go func() { startErr <- loader.Start(ctx) }()

	// Give warm-up a moment to get going, then cancel underneath it.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-startErr:
		require.Error(t, err, "Start must report that warm-up did not finish")
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Start must return once its context is cancelled")
	}

	// Warm-up never completed, so nothing was handed to a serving goroutine and
	// Start closed the consumer itself.
	assert.True(t, fake.isClosed())
	require.NoError(t, loader.WaitUntilStopped(), "nothing ever served, so nothing is left to wait for")
}

// A blocked assignment must not hang startup forever.
func TestLoaderWarmupTimeoutOnUnresponsiveAssignment(t *testing.T) {
	t.Parallel()

	fake := &fakeConsumer{partitions: []int32{0}, blockUntilCancelled: true, live: make(chan driver.Event)}

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake},
		ekconfig.WithWarmupTimeout(200*time.Millisecond))
	loader.Bind(playerBinding("PlayerConfig", "players"))

	start := time.Now()
	err := loader.Start(t.Context())
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, ekconfig.ErrWarmupTimeout)
	assert.Less(t, elapsed, 2*time.Second, "the timeout must bound Start, not merely be recorded")
}

// Cancelling the caller's context aborts warm-up.
func TestLoaderStartHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	fake := &fakeConsumer{partitions: []int32{0}, blockUntilCancelled: true, live: make(chan driver.Event)}

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.Bind(playerBinding("PlayerConfig", "players"))

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := loader.Start(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLoaderStartTwice(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1"}`)),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()))
	stopLoader(t, loader)

	err := loader.Start(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, ekconfig.ErrAlreadyStarted)
}

func TestLoaderStartWithoutBindings(t *testing.T) {
	t.Parallel()

	loader := loaderWith(t, map[string]*fakeConsumer{})

	err := loader.Start(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, ekconfig.ErrNoBindings)
}

// Binding after Start, and binding the same name twice, are programmer errors
// that a running service cannot act on — so they panic at wiring time rather
// than returning an error nobody would handle.
func TestLoaderBindPanics(t *testing.T) {
	t.Parallel()

	t.Run("after start", func(t *testing.T) {
		t.Parallel()

		scripted := script(
			events(record(0, 0, "p1", `{"playerId":"p1"}`)),
			eofAll(0),
		)

		fake := newFakeConsumer([]int32{0}, scripted...)

		loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
		loader.Bind(playerBinding("PlayerConfig", "players"))
		require.NoError(t, loader.Start(t.Context()))
		stopLoader(t, loader)

		assert.PanicsWithValue(t,
			`easykafkaconfig: Bind("Late") after Start: bind after start`,
			func() { loader.Bind(playerBinding("Late", "players")) })
	})

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()

		loader := loaderWith(t, map[string]*fakeConsumer{})
		loader.Bind(playerBinding("PlayerConfig", "players"))

		assert.PanicsWithValue(t,
			`easykafkaconfig: duplicate binding name "PlayerConfig"`,
			func() { loader.Bind(playerBinding("PlayerConfig", "other")) })
	})

	t.Run("invalid binding", func(t *testing.T) {
		t.Parallel()

		loader := loaderWith(t, map[string]*fakeConsumer{})

		assert.Panics(t, func() {
			loader.Bind(ekconfig.Binding[string, playerConfig]{Name: "Broken"})
		})
	})

	t.Run("nil store", func(t *testing.T) {
		t.Parallel()

		loader := loaderWith(t, map[string]*fakeConsumer{})

		assert.Panics(t, func() {
			loader.BindTo(playerBinding("PlayerConfig", "players"), nil)
		})
	})
}

// BindTo fills a store the caller already owns, for a store that is a field of
// an existing struct.
func TestLoaderBindTo(t *testing.T) {
	t.Parallel()

	scripted := script(
		events(record(0, 0, "p1", `{"playerId":"p1","limit":7}`)),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	stores := struct {
		Players *ekconfig.Store[string, playerConfig]
	}{Players: ekconfig.NewStore[string, playerConfig]()}

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.BindTo(playerBinding("PlayerConfig", "players"), stores.Players)

	require.NoError(t, loader.Start(t.Context()))
	stopLoader(t, loader)

	assert.Equal(t, 7, stores.Players.GetOrNil("p1").Limit)
}
