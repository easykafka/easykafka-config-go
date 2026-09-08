package integration

// T-6.6 and T-6.8: what happens across a restart, and on the way down.

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A second loader over the same topic rebuilds an identical store.
//
// The driver test beside this one proves no offsets are committed; this proves
// the loader rebuilds from that — which is the property that makes an
// in-memory config store reproducible, and the reason a restarting pod does not
// need to remember anything.
func TestLoaderRestartRebuildsIdenticalStore(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-restart")

	cluster.CreateCompactedTopic(t, topic, 2)
	cluster.Produce(t, topic,
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":1}`)},
		helpers.Record{Key: "p2", Value: []byte(`{"playerId":"p2","limit":2}`)},
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":11}`)},
		helpers.Record{Key: "p3", Value: []byte(`{"playerId":"p3","limit":3}`)},
		helpers.Record{Key: "p3", Value: nil}, // deleted, and must stay deleted
	)

	// Both loaders use the same group id, which for this library is inert:
	// nothing joins a group, so sharing it cannot make the second loader resume
	// where the first left off. Sharing it deliberately, because a service
	// deploys every replica with the same configuration.
	const shared = "loader-restart-shared"

	first := newLoader(t, cluster, ekconfig.WithClientGroupID(shared))
	firstStore := first.Bind(playerBinding("PlayerConfig", topic))
	startedLoader(t, first)

	before := firstStore.Snapshot()
	require.Len(t, before, 2, "p1 and p2 survive, p3 was tombstoned")

	second := newLoader(t, cluster, ekconfig.WithClientGroupID(shared))
	secondStore := second.Bind(playerBinding("PlayerConfig", topic))
	startedLoader(t, second)

	after := secondStore.Snapshot()

	require.Len(t, after, len(before), "the second loader must rebuild the whole topic")
	assert.True(t, maps.EqualFunc(before, after, func(a, b *playerConfig) bool {
		return a.PlayerID == b.PlayerID && a.Limit == b.Limit
	}), "and rebuild it identically: before=%v after=%v", before, after)

	assert.Equal(t, 11, after["p1"].Limit, "including the last value for a superseded key")
	assert.NotContains(t, after, "p3", "and the tombstoned key must stay absent")
}

// Cancelling the context after warm-up stops the loader: WaitUntilStopped
// returns, every real consumer has been closed, and every binding reports
// itself stopped.
//
// The closing is what matters here and what a fake cannot check. A librdkafka
// handle closed from a goroutine other than the one polling it is the bug the
// design is arranged to avoid, so this is the test that says the arrangement
// holds against the real client.
func TestLoaderStopsAndClosesRealConsumers(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)

	first := helpers.UniqueTopic(t, "loader-shutdown-a")
	second := helpers.UniqueTopic(t, "loader-shutdown-b")

	for _, topic := range []string{first, second} {
		cluster.CreateCompactedTopic(t, topic, 2)
		cluster.Produce(t, topic, records("k1", "k2")...)
	}

	loader := newLoader(t, cluster)
	loader.Bind(playerBinding("First", first))
	loader.Bind(playerBinding("Second", second))

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, loader.Start(ctx))

	for _, s := range loader.Stats() {
		require.Equal(t, ekconfig.PhaseSteady, s.Phase, "both bindings serving before the stop")
	}

	cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- loader.WaitUntilStopped() }()

	select {
	case err := <-stopped:
		require.NoError(t, err, "a cancelled context is a clean stop, not a failure")
	case <-time.After(30 * time.Second):
		t.Fatal("WaitUntilStopped must return once the context is cancelled")
	}

	// WaitUntilStopped returning means every goroutine has exited and closed
	// its consumer, so this needs no polling: the phases are already final.
	for _, s := range loader.Stats() {
		assert.Equal(t, ekconfig.PhaseStopped, s.Phase,
			"every binding must be stopped by the time the wait returns")
	}

	assert.NoError(t, loader.Err(), "a clean stop leaves no error")
	assert.NoError(t, loader.WaitUntilStopped(), "and waiting again is a no-op")
}

// Cancelling during warm-up aborts Start, and leaves nothing running.
//
// A real topic large enough that warm-up cannot finish inside the window,
// because the point is to catch the loader mid-warm-up rather than after it.
func TestLoaderCancelDuringRealWarmup(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-cancel-warmup")

	cluster.CreateCompactedTopic(t, topic, 1)

	// Enough records that warm-up takes long enough to be interrupted, without
	// making the test slow.
	const many = 5_000

	keys := make([]string, 0, many)
	for i := range many {
		keys = append(keys, "k"+strconv.Itoa(i))
	}
	cluster.Produce(t, topic, records(keys...)...)

	loader := newLoader(t, cluster)
	loader.Bind(playerBinding("PlayerConfig", topic))

	ctx, cancel := context.WithCancel(t.Context())

	startErr := make(chan error, 1)
	go func() { startErr <- loader.Start(ctx) }()

	// Cancel almost immediately: warm-up has begun but cannot have finished.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-startErr:
		if err == nil {
			t.Skip("warm-up finished before the cancellation landed; nothing to assert")
		}
		require.ErrorIs(t, err, context.Canceled, "Start must report why it gave up")
	case <-time.After(60 * time.Second):
		t.Fatal("Start must return once its context is cancelled")
	}

	// Nothing was handed to a serving goroutine, so there is nothing to wait
	// for and no error to report.
	require.NoError(t, loader.WaitUntilStopped())
}
