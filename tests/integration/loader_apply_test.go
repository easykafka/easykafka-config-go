package integration

// T-6.3, T-6.4, T-6.5 and T-6.7: what the loader does with the records a real
// broker delivers.

import (
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A tombstone produced to a real topic removes the key from the typed store.
//
// The driver test beside this one proves a blank payload is delivered as such;
// this proves the store reacts to it, which is the layer that turns a Kafka
// record into an absent map entry.
func TestLoaderAppliesTombstonesFromRealTopic(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-tombstone")

	cluster.CreateCompactedTopic(t, topic, 1)
	cluster.Produce(t, topic,
		helpers.Record{Key: "keep", Value: []byte(`{"playerId":"keep","limit":1}`)},
		helpers.Record{Key: "gone", Value: []byte(`{"playerId":"gone","limit":2}`)},
		helpers.Record{Key: "gone", Value: nil}, // the tombstone
	)

	obs := &recordingObserver{}
	loader := newLoader(t, cluster, ekconfig.WithObserver(obs))
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	startedLoader(t, loader)

	require.Equal(t, 1, players.Len())
	assert.True(t, players.Has("keep"))
	assert.False(t, players.Has("gone"), "the tombstone must have removed the key")
	assert.Equal(t, 1, obs.snapshot().deletes, "and the delete must be reported")
}

// An empty topic is a deployment fault unless the binding says otherwise, and
// the distinction has to hold against a real broker, where "empty" means the
// partition reports end-of-partition with nothing before it.
func TestLoaderEmptyRealTopic(t *testing.T) {
	t.Parallel()

	t.Run("rejected by default", func(t *testing.T) {
		t.Parallel()

		cluster := helpers.SharedCluster(t)
		topic := helpers.UniqueTopic(t, "loader-empty-rejected")

		cluster.CreateCompactedTopic(t, topic, 2)

		loader := newLoader(t, cluster)
		loader.Bind(playerBinding("PlayerConfig", topic))

		err := loader.Start(t.Context())
		require.Error(t, err)
		require.ErrorIs(t, err, ekconfig.ErrEmptyTopic)
		assert.Contains(t, err.Error(), "PlayerConfig", "the error must name the binding")
		assert.Contains(t, err.Error(), topic, "and the topic")

		// A failed warm-up leaves nothing running: Start closed every consumer
		// before returning, so there is nothing to wait for.
		require.NoError(t, loader.WaitUntilStopped())
	})

	t.Run("allowed when the binding permits it", func(t *testing.T) {
		t.Parallel()

		cluster := helpers.SharedCluster(t)
		topic := helpers.UniqueTopic(t, "loader-empty-allowed")

		cluster.CreateCompactedTopic(t, topic, 2)

		loader := newLoader(t, cluster)

		binding := playerBinding("PlayerConfig", topic)
		binding.AllowEmpty = true
		players := loader.Bind(binding)

		startedLoader(t, loader)

		assert.Equal(t, 0, players.Len())
	})
}

// After Start returns the consumers keep running, so a record produced later
// reaches the store on its own — and a tombstone produced later removes it.
//
// This is the half of the library's job that Start's return value says nothing
// about, and the reason the consumers outlive it.
func TestLoaderAppliesLiveUpdatesFromRealTopic(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-live")

	cluster.CreateCompactedTopic(t, topic, 1)
	cluster.Produce(t, topic, helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":1}`)})

	loader := newLoader(t, cluster)
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	startedLoader(t, loader)

	require.Equal(t, 1, players.GetOrNil("p1").Limit, "the warm-up value")

	// An update to an existing key, and a brand new one.
	cluster.Produce(t, topic,
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":42}`)},
		helpers.Record{Key: "p2", Value: []byte(`{"playerId":"p2","limit":7}`)},
	)

	eventually(t, "a live update must reach the store", func() bool {
		p1 := players.GetOrNil("p1")

		return p1 != nil && p1.Limit == 42 && players.Has("p2")
	})

	// And a live tombstone removes a key that warm-up had loaded.
	cluster.Produce(t, topic, helpers.Record{Key: "p1", Value: nil})

	eventually(t, "a live tombstone must remove the key", func() bool {
		return !players.Has("p1")
	})

	assert.True(t, players.Has("p2"), "and must remove only that key")
}

// One malformed payload among good ones is skipped, reported, and does not stop
// the topic loading or Start succeeding. Bad data on a config topic is a fault
// to see, not a reason to refuse to boot.
func TestLoaderSkipsUndecodableRecordsFromRealTopic(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-decode-error")

	cluster.CreateCompactedTopic(t, topic, 1)
	cluster.Produce(t, topic,
		helpers.Record{Key: "good", Value: []byte(`{"playerId":"good","limit":1}`)},
		helpers.Record{Key: "bad", Value: []byte(`{"playerId":`)}, // truncated JSON
		helpers.Record{Key: "also-good", Value: []byte(`{"playerId":"also-good","limit":2}`)},
	)

	obs := &recordingObserver{}
	loader := newLoader(t, cluster, ekconfig.WithObserver(obs))
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	// Start must still succeed: a skipped record is not a failed warm-up.
	startedLoader(t, loader)

	assert.Equal(t, 2, players.Len(), "the good records load, the bad one is skipped")
	assert.True(t, players.Has("good"))
	assert.True(t, players.Has("also-good"), "a bad record must not stop the ones after it")
	assert.False(t, players.Has("bad"))

	got := obs.snapshot()
	require.Len(t, got.decodeErrors, 1, "and the failure must be reported")

	assert.Equal(t, uint64(1), loader.Stats()[0].DecodeErrors, "and counted")
}
