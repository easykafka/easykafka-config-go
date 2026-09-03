package integration

import (
	"sync"
	"testing"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pollTimeout = 500 * time.Millisecond
	drainBudget = 30 * time.Second
)

// newConsumer builds a driver consumer against the test cluster.
func newConsumer(t *testing.T, cluster *helpers.Cluster, topic, groupID string) driver.Consumer {
	t.Helper()

	c, err := driver.New(driver.Config{
		Brokers: cluster.Brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	return c
}

// drained is what one consumer saw while reading a topic to its end.
type drained struct {
	keys       []string
	tombstones []string
	eofs       map[int32]bool
}

// drainToEOF assigns every partition and polls until each has reported
// end-of-partition, which is how the layers above will detect that a topic has
// been read whole.
func drainToEOF(t *testing.T, c driver.Consumer, partitions int) drained {
	t.Helper()

	ids, err := c.AssignAll(t.Context())
	require.NoError(t, err)
	require.Len(t, ids, partitions, "every partition must be assigned, not a subset")

	got := drained{eofs: map[int32]bool{}}
	deadline := time.Now().Add(drainBudget)

	for len(got.eofs) < partitions && time.Now().Before(deadline) {
		switch e := c.Poll(pollTimeout).(type) {
		case *driver.Record:
			if len(e.Payload) == 0 {
				got.tombstones = append(got.tombstones, string(e.Key))
			} else {
				got.keys = append(got.keys, string(e.Key))
			}
		case driver.EOF:
			got.eofs[e.Partition] = true
		case driver.Idle:
		case driver.Failure:
			require.False(t, e.Fatal, "unexpected fatal failure: %v", e)
		}
	}

	require.Len(t, got.eofs, partitions, "did not reach end-of-partition on every partition within %s", drainBudget)

	return got
}

// The P2 smoke test: the driver reads a real compacted topic end to end.
//
// It exercises the paths no fake can cover — the librdkafka config map,
// partition discovery, explicit assignment, the poll loop and event
// classification — against a real broker.
func TestDriverReadsCompactedTopicToEnd(t *testing.T) {
	t.Parallel()

	const partitions = 3

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "smoke")
	cluster.CreateCompactedTopic(t, topic, partitions)
	cluster.Produce(t,
		topic,
		helpers.Record{Key: "k1", Value: []byte(`{"limit":1}`)},
		helpers.Record{Key: "k2", Value: []byte(`{"limit":2}`)},
		helpers.Record{Key: "k3", Value: []byte(`{"limit":3}`)},
	)

	got := drainToEOF(t, newConsumer(t, cluster, topic, "smoke-reader"), partitions)

	assert.ElementsMatch(t, []string{"k1", "k2", "k3"}, got.keys)
	assert.Empty(t, got.tombstones)
}

// A tombstone arrives as a record with an empty payload, which is what the
// layers above turn into a delete. The driver must not swallow or special-case
// it.
func TestDriverDeliversTombstones(t *testing.T) {
	t.Parallel()

	const partitions = 1

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "tombstone")
	cluster.CreateCompactedTopic(t, topic, partitions)
	cluster.Produce(t,
		topic,
		helpers.Record{Key: "gone", Value: []byte(`{"limit":1}`)},
		helpers.Record{Key: "gone", Value: nil},
	)

	got := drainToEOF(t, newConsumer(t, cluster, topic, "tombstone-reader"), partitions)

	assert.Equal(t, []string{"gone"}, got.keys)
	assert.Equal(t, []string{"gone"}, got.tombstones, "the null-payload record must be delivered, not dropped")
}

// An empty topic reports end-of-partition immediately, so warm-up completes in
// milliseconds instead of waiting out a timeout. Whether an empty topic is
// acceptable is a decision for the layer above, which needs this to be
// detectable rather than indistinguishable from a slow broker.
func TestDriverReportsEOFOnEmptyTopic(t *testing.T) {
	t.Parallel()

	const partitions = 2

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "empty")
	cluster.CreateCompactedTopic(t, topic, partitions)

	got := drainToEOF(t, newConsumer(t, cluster, topic, "empty-reader"), partitions)

	assert.Empty(t, got.keys)
	assert.Empty(t, got.tombstones)
}

// Every replica must read the whole topic. Two consumers sharing one group id
// both take every partition and see every record, because assignment is
// explicit and no group is joined — with SubscribeTopics the coordinator would
// split the partitions between them and each would see a fraction.
func TestDriverEveryConsumerReadsAllPartitions(t *testing.T) {
	t.Parallel()

	const (
		partitions = 3
		records    = 9
	)

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "assign")
	cluster.CreateCompactedTopic(t, topic, partitions)

	produced := make([]helpers.Record, 0, records)
	want := make([]string, 0, records)
	for i := range records {
		key := string(rune('a' + i))
		produced = append(produced, helpers.Record{Key: key, Value: []byte(`{"n":1}`)})
		want = append(want, key)
	}
	cluster.Produce(t, topic, produced...)

	const sharedGroup = "shared-inert-group"
	first := drainToEOF(t, newConsumer(t, cluster, topic, sharedGroup), partitions)
	second := drainToEOF(t, newConsumer(t, cluster, topic, sharedGroup), partitions)

	assert.ElementsMatch(t, want, first.keys)
	assert.ElementsMatch(t, want, second.keys, "the second consumer must see the same records, not the remainder")

	// The group id is inert: nothing joined a group, so the broker never
	// created one. This is what keeps a shared id safe across replicas.
	assert.NotContains(t, cluster.ListConsumerGroups(t), sharedGroup)
}

// Restarting re-reads everything. No offsets are committed, so a fresh consumer
// over the same topic rebuilds the identical picture — the property that makes
// an in-memory projection reproducible after a restart.
func TestDriverRestartRereadsWholeTopic(t *testing.T) {
	t.Parallel()

	const partitions = 2

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "restart")
	cluster.CreateCompactedTopic(t, topic, partitions)
	cluster.Produce(t,
		topic,
		helpers.Record{Key: "x", Value: []byte(`{"n":1}`)},
		helpers.Record{Key: "y", Value: []byte(`{"n":2}`)},
	)

	const groupID = "restart-reader"
	before := drainToEOF(t, newConsumer(t, cluster, topic, groupID), partitions)
	after := drainToEOF(t, newConsumer(t, cluster, topic, groupID), partitions)

	assert.ElementsMatch(t, []string{"x", "y"}, before.keys)
	assert.ElementsMatch(t, before.keys, after.keys, "a restart must re-read from the beginning")
}

// Watermarks and Positions back the watermark-based warm-up detector. The high
// watermark is the offset the next record will get, so a partition is fully
// read once the position reaches it.
func TestDriverWatermarksAndPositions(t *testing.T) {
	t.Parallel()

	const partitions = 1

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "watermarks")
	cluster.CreateCompactedTopic(t, topic, partitions)
	cluster.Produce(t,
		topic,
		helpers.Record{Key: "a", Value: []byte(`{"n":1}`)},
		helpers.Record{Key: "b", Value: []byte(`{"n":2}`)},
		helpers.Record{Key: "c", Value: []byte(`{"n":3}`)},
	)

	c := newConsumer(t, cluster, topic, "watermark-reader")
	got := drainToEOF(t, c, partitions)
	require.Len(t, got.keys, 3)

	low, high, err := c.Watermarks(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), low)
	assert.Equal(t, int64(3), high, "the high watermark is the offset the next record will receive")

	positions, err := c.Positions()
	require.NoError(t, err)
	require.Contains(t, positions, int32(0))
	assert.Equal(t, high, positions[0], "having read to the end, the position equals the high watermark")
}

// A topic that does not exist is reported as such, rather than looking like an
// empty one: the layer above needs to tell a misconfigured topic name from a
// topic with no records.
func TestDriverAssignAllOnMissingTopic(t *testing.T) {
	t.Parallel()

	cluster := helpers.SharedCluster(t)

	c, err := driver.New(driver.Config{
		Brokers:         cluster.Brokers,
		Topic:           helpers.UniqueTopic(t, "does-not-exist"),
		GroupID:         "missing-topic-reader",
		MetadataTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	_, err = c.AssignAll(t.Context())
	// e.g. topic "does-not-exist-1788437543905163000-TestDriverAssignAllOnMissingTopic" metadata: Broker: Unknown topic or partition
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown topic or partition")
}

// Close must be safe, and must keep its contract, while other calls are in
// flight: it succeeds, later calls report ErrClosed, and a second Close is
// still a no-op.
//
// The consumer is driven from several goroutines at once here, which is not how
// the library uses it. That is the point: the layer above closes on a signal
// without knowing what is in flight, so the driver has to be safe against it.
//
// Scope, so this test is not read as more than it is: it is a regression test
// for the contract, not proof that the locking prevents a crash. The bad
// outcome it guards against is a use-after-free window that is both narrow and
// partly covered by the binding's own isClosed check, so it does not reproduce
// on demand — verified by reverting the locking, after which this test still
// passed repeatedly. What it does catch reliably is the contract breaking:
// Close returning an error under load, or a post-close call returning something
// other than ErrClosed.
func TestDriverCloseRacesOtherCallsSafely(t *testing.T) {
	t.Parallel()

	const (
		partitions = 2
		workers    = 4
	)

	cluster := helpers.SharedCluster(t)
	topic := helpers.UniqueTopic(t, "close-race")
	cluster.CreateCompactedTopic(t, topic, partitions)
	cluster.Produce(t, topic, helpers.Record{Key: "k", Value: []byte(`{"n":1}`)})

	c, err := driver.New(driver.Config{
		Brokers: cluster.Brokers,
		Topic:   topic,
		GroupID: "close-race-reader",
	})
	require.NoError(t, err)

	_, err = c.AssignAll(t.Context())
	require.NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Hammer every handle-touching call while the close happens.
	for i := range workers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}

				switch i % 3 {
				case 0:
					c.Poll(10 * time.Millisecond)
				case 1:
					_, _, _ = c.Watermarks(0)
				case 2:
					_, _ = c.Positions()
				}
			}
		})
	}

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Close(), "close must succeed with calls in flight")

	// Every call must keep reporting ErrClosed rather than crashing.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _, err := c.Watermarks(0)
		require.ErrorIs(t, err, driver.ErrClosed)
	}

	close(stop)
	wg.Wait()

	require.NoError(t, c.Close(), "close stays idempotent after the race")
}
