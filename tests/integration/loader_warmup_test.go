package integration

// Warm-up, against a real broker.

import (
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The test that proves the library does its job at all: a real compacted topic
// with several partitions is read whole, and every store is complete by the
// time Start returns.
//
// Multi-partition on purpose. Warm-up completes only when every assigned
// partition has reported, and with one partition that condition is
// indistinguishable from "the first partition reported".
func TestLoaderWarmsUpFromRealTopic(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-warmup")

	cluster.CreateCompactedTopic(t, topic, 3)

	// Superseded versions of p1 as well, so the store must hold the last value
	// rather than the first — the whole point of reading a compacted topic.
	cluster.Produce(t, topic,
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":1}`)},
		helpers.Record{Key: "p2", Value: []byte(`{"playerId":"p2","limit":2}`)},
		helpers.Record{Key: "p3", Value: []byte(`{"playerId":"p3","limit":3}`)},
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":99}`)},
	)

	obs := &recordingObserver{}
	loader := newLoader(t, cluster, ekconfig.WithObserver(obs))
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	startedLoader(t, loader)

	// Complete when Start returns — not eventually, immediately. Nothing here
	// waits, which is the guarantee that lets a service treat Start returning
	// as "configuration is loaded".
	require.Equal(t, 3, players.Len(), "the store must be complete when Start returns")
	assert.Equal(t, 99, players.GetOrNil("p1").Limit, "the last value for a key wins")
	assert.Equal(t, 2, players.GetOrNil("p2").Limit)
	assert.Equal(t, 3, players.GetOrNil("p3").Limit)
	require.NoError(t, loader.Err())

	got := obs.snapshot()
	assert.Equal(t, 4, got.warmupUpserts, "every record read during warm-up is reported as warm-up")
	assert.Contains(t, got.phases, "PlayerConfig:steady", "and the phase change is reported")

	stats := loader.Stats()
	require.Len(t, stats, 1)
	assert.Equal(t, 3, stats[0].Size)
	assert.Equal(t, 4, stats[0].WarmupApplied, "four records applied, three keys survive")
	assert.Positive(t, stats[0].WarmupTook)
}

// Several bindings in one loader warm up concurrently and independently: all
// stores are complete when Start returns, and no topic's records reach another
// topic's store.
//
// The cross-contamination half is what a fake cannot check, since it hands each
// binding a consumer that only ever returns that binding's script.
func TestLoaderWarmsUpSeveralBindings(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)

	first := helpers.UniqueTopic(t, "loader-multi-a")
	second := helpers.UniqueTopic(t, "loader-multi-b")
	third := helpers.UniqueTopic(t, "loader-multi-c")

	for _, topic := range []string{first, second, third} {
		cluster.CreateCompactedTopic(t, topic, 2)
	}

	cluster.Produce(t, first, records("a1", "a2")...)
	cluster.Produce(t, second, records("b1", "b2", "b3")...)
	cluster.Produce(t, third, records("c1")...)

	loader := newLoader(t, cluster)
	a := loader.Bind(playerBinding("First", first))
	b := loader.Bind(playerBinding("Second", second))
	c := loader.Bind(playerBinding("Third", third))

	startedLoader(t, loader)

	require.Equal(t, 2, a.Len(), "every binding is complete when Start returns, not just the first")
	require.Equal(t, 3, b.Len())
	require.Equal(t, 1, c.Len())

	// Each store holds only its own topic's keys.
	assert.True(t, a.Has("a1"))
	assert.False(t, a.Has("b1"), "one binding's records must not reach another's store")
	assert.False(t, b.Has("a1"))
	assert.False(t, c.Has("b3"))

	// Stats reports them in bind order, which is what a config-status endpoint
	// relies on.
	stats := loader.Stats()
	require.Len(t, stats, 3)
	assert.Equal(t, []string{"First", "Second", "Third"},
		[]string{stats[0].Name, stats[1].Name, stats[2].Name})
}
