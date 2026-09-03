// Package driver is the only package in this library that talks to
// confluent-kafka-go. Everything above it works with the Consumer interface and
// the Event types declared here, which keeps the librdkafka surface small
// enough to audit and lets the layers above be tested without a broker.
//
// The consumption pattern is deliberately narrow: one compacted topic, every
// partition assigned explicitly from the beginning, offsets never committed.
// See Consumer.AssignAll for why that is not the usual subscribe-and-rebalance
// arrangement.
package driver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

// Consumer reads one compacted topic. The layers above depend on this interface
// rather than on the implementation, so they can be exercised with a fake.
type Consumer interface {
	// AssignAll discovers the topic's partitions and assigns all of them from
	// the beginning. Returns the partition ids assigned.
	AssignAll(ctx context.Context) ([]int32, error)

	// Poll waits up to timeout for one event and always returns a non-nil
	// Event. Must be called from a single goroutine.
	Poll(timeout time.Duration) Event

	// Watermarks returns the first and next-to-be-written offsets for one
	// assigned partition.
	Watermarks(partition int32) (low, high int64, err error)

	// Positions returns the next offset to be consumed per assigned partition.
	Positions() (map[int32]int64, error)

	// Close releases the consumer. Idempotent, and safe to call from another
	// goroutine while any other call is in flight — it waits for that call to
	// return first.
	Close() error
}

// consumer is the confluent-kafka-go implementation of Consumer.
type consumer struct {
	cfg    Config
	kc     *kafka.Consumer
	logger zerolog.Logger

	// handleMu exists to stop any cgo call this package makes from landing on a
	// handle that Close has already released.
	//
	// Checking the closed flag first is not enough, because a check reports the
	// state at the moment you looked and nothing stops it changing before you
	// act. There are two such check-then-act pairs stacked on each other. Ours:
	//
	//	func (c *consumer) Watermarks(partition int32) (...) {
	//	    if c.closed.Load() { return ErrClosed }   // CHECK (ours)
	//	    ... c.kc.QueryWatermarkOffsets(...)       // ACT
	//	}
	//
	// and the binding's, inside that call:
	//
	//	func (c *Consumer) QueryWatermarkOffsets(...) {
	//	    if err := c.verifyClient(); err != nil { ... }          // CHECK (theirs: isClosed)
	//	    ... C.rd_kafka_query_watermark_offsets(c.handle.rk, ...) // ACT: into cgo
	//	}
	//
	// Unlocked, this interleaving is possible — A being the goroutine running a
	// warm-up detector, B the one shutting down on a signal:
	//
	//	    | A: warm-up detector                       | B: shutdown on a signal
	//	----+-------------------------------------------+--------------------------------------
	//	 t0 | Watermarks: our closed is false        OK |
	//	 t1 | QueryWatermarkOffsets: isClosed is 0   OK |
	//	 t2 | about to make the cgo call                | Close: Swap(true), takes the lock
	//	    |                                           | (free — A never held it)
	//	 t3 |                                           | kc.Close(): sets isClosed, then
	//	    |                                           | rd_kafka_destroy(handle)
	//	 t4 | cgo call runs on the destroyed handle     |
	//
	// A passed both guards at t1, so neither helps it at t4. The binding's guard
	// is real but not sufficient, and the window is only a few instructions
	// wide, which is why the failure is rare rather than absent.
	//
	// Holding this across both the check and the act means Close cannot begin
	// tearing down until the call returns, so every later call gets a
	// deterministic ErrClosed instead of a narrow race.
	//
	// It also keeps Close from polling concurrently with us: Close loops on
	// Poll(100) internally until librdkafka reports the consumer closed, which
	// without this lock would mean two goroutines polling one consumer.
	//
	// The cost is that Close waits for the current call, up to one poll timeout.
	handleMu sync.Mutex
	closed   atomic.Bool

	// connected tracks whether the broker is currently reachable, so that
	// losing and regaining a connection is logged once each rather than on
	// every poll.
	connected atomic.Bool

	// errLog throttles repeated non-fatal error logging.
	errLog *throttle

	// assigned is the partition set from the last successful AssignAll. Guarded
	// by handleMu: it is written and read only by AssignAll and Positions, both
	// of which hold that lock for their whole duration.
	assigned []int32
}

// Compile-time assertion that the implementation satisfies the interface.
var _ Consumer = (*consumer)(nil)

// New builds a consumer for one compacted topic. It does not contact the broker;
// the first network call happens in AssignAll.
//
// The concrete type stays unexported: callers hold the Consumer interface, which
// is what lets the layers above be tested with a fake.
func New(cfg Config) (Consumer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("driver config: %w", err)
	}

	configMap, err := buildConfigMap(cfg)
	if err != nil {
		return nil, err
	}

	kc, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer for topic %q: %w", cfg.Topic, err)
	}

	c := &consumer{
		cfg:    cfg,
		kc:     kc,
		logger: cfg.Logger.With().Str("topic", cfg.Topic).Logger(),
		errLog: newThrottle(cfg.ErrorLogInterval),
	}
	c.connected.Store(true)

	return c, nil
}

// buildConfigMap assembles the librdkafka properties.
//
// The base set mirrors what these services already run in production, with the
// commit-related properties pinned to the only values this library's semantics
// allow.
func buildConfigMap(cfg Config) (*kafka.ConfigMap, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),

		// Required by the driver even though no group is ever joined; see
		// Config.GroupID and Consumer.AssignAll.
		"group.id": cfg.GroupID,

		// Offsets are never committed, and nothing is stored for committing.
		// This is what makes a restart re-read the whole topic, and what makes
		// the same group id safe to share across replicas: with no committed
		// offsets there is no per-(group, topic, partition) state to collide
		// over. Changing either of these breaks both properties.
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,

		// A fallback only. Assignment pins every partition to the beginning
		// explicitly, so this is never consulted in practice; it is set so the
		// intent is unambiguous if assignment ever changes.
		"auto.offset.reset": "earliest",

		// End-of-partition events are how the layers above learn that a
		// partition has been read to its end.
		"enable.partition.eof": true,

		// librdkafka reconnects on its own; these bound how fast it retries.
		"reconnect.backoff.ms":     int(DefaultReconnectBackoff.Milliseconds()),
		"reconnect.backoff.max.ms": int(DefaultReconnectBackoffMax.Milliseconds()),
	}

	if cfg.SecurityProtocol != "" {
		sasl := map[string]any{
			"security.protocol": cfg.SecurityProtocol,
			"sasl.mechanism":    cfg.SASLMechanism,
			"sasl.username":     cfg.SASLUsername,
			"sasl.password":     cfg.SASLPassword,
		}
		for key, value := range sasl {
			if err := configMap.SetKey(key, value); err != nil {
				return nil, fmt.Errorf("setting %s: %w", key, err)
			}
		}
	}

	// Applied last so a caller can tune anything not reserved.
	for key, value := range cfg.Extra {
		if err := configMap.SetKey(key, value); err != nil {
			return nil, fmt.Errorf("setting %s: %w", key, err)
		}
	}

	return configMap, nil
}

// AssignAll discovers every partition of the topic and assigns all of them at
// the earliest offset.
//
// This is an explicit Assign, never SubscribeTopics, and the difference is the
// whole point. Subscribing joins a consumer group, and the group coordinator
// then splits the partitions across the members — so several replicas of the
// same service would each read a fraction of the configuration. Assigning
// directly joins no group: every replica takes every partition, nothing is
// split, no rebalance can move a partition mid-flight, and no consumer group is
// created in the cluster at all.
//
// The trade-off is that partition discovery is ours: the partition set is read
// once, here, so a topic repartitioned later is not picked up until the next
// call. The partition count is logged to make that diagnosable.
func (c *consumer) AssignAll(ctx context.Context) ([]int32, error) {
	c.handleMu.Lock()
	defer c.handleMu.Unlock()

	if c.closed.Load() {
		return nil, ErrClosed
	}

	partitions, err := c.discoverPartitions(ctx)
	if err != nil {
		return nil, err
	}

	topicPartitions := make([]kafka.TopicPartition, 0, len(partitions))
	ids := make([]int32, 0, len(partitions))
	for _, id := range partitions {
		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     &c.cfg.Topic,
			Partition: id,
			Offset:    kafka.OffsetBeginning,
		})
		ids = append(ids, id)
	}

	if err := c.kc.Assign(topicPartitions); err != nil {
		return nil, fmt.Errorf("assigning %d partitions of %q: %w", len(ids), c.cfg.Topic, err)
	}

	c.assigned = ids

	c.logger.Info().
		Ints32("partitions", ids).
		Int("partition_count", len(ids)).
		Msg("assigned all partitions from the beginning")

	return ids, nil
}

// discoverPartitions asks the broker which partitions the topic has.
func (c *consumer) discoverPartitions(ctx context.Context) ([]int32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	topic := c.cfg.Topic
	metadata, err := c.kc.GetMetadata(&topic, false, int(c.cfg.MetadataTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("fetching metadata for topic %q: %w", topic, err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}
	if topicMetadata.Error.Code() != kafka.ErrNoError {
		return nil, fmt.Errorf("topic %q metadata: %w", topic, topicMetadata.Error)
	}
	if len(topicMetadata.Partitions) == 0 {
		return nil, fmt.Errorf("%w: %q reports no partitions", ErrTopicNotFound, topic)
	}

	ids := make([]int32, 0, len(topicMetadata.Partitions))
	for _, p := range topicMetadata.Partitions {
		ids = append(ids, p.ID)
	}

	return ids, nil
}

// Poll waits up to timeout for one event.
//
// The timeout is per call rather than fixed at construction so the caller can
// wait patiently while reading a backlog and then poll tightly once it is
// keeping up.
func (c *consumer) Poll(timeout time.Duration) Event {
	c.handleMu.Lock()
	defer c.handleMu.Unlock()

	if c.closed.Load() {
		return Failure{Err: ErrClosed, Fatal: true}
	}

	switch e := c.kc.Poll(int(timeout.Milliseconds())).(type) {
	case *kafka.Message:
		c.noteConnected()

		return &Record{
			Topic:     derefTopic(e.TopicPartition.Topic, c.cfg.Topic),
			Partition: e.TopicPartition.Partition,
			Offset:    int64(e.TopicPartition.Offset),
			Key:       e.Key,
			Payload:   e.Value,
			Timestamp: e.Timestamp,
		}

	case kafka.PartitionEOF:
		c.noteConnected()

		return EOF{Partition: e.Partition, Offset: int64(e.Offset)}

	case kafka.Error:
		return c.classifyError(e)

	case nil:
		// Timeout: nothing was available.
		return Idle{}

	default:
		// Stats, offsets-committed and other events we do not act on. Reported
		// as idle so the caller's loop treats them as "nothing to apply"; they
		// are not silently dropped, since an idle tick is meaningful to the
		// warm-up detectors.
		c.logger.Debug().Type("event_type", e).Msg("ignoring kafka event")

		return Idle{}
	}
}

// classifyError turns a librdkafka error into a Failure, logging connection
// state changes once rather than on every poll.
func (c *consumer) classifyError(err kafka.Error) Event {
	if err.IsFatal() {
		c.logger.Error().Err(err).Int("code", int(err.Code())).
			Msg("fatal kafka error, consumer is unusable")

		return Failure{Err: err, Fatal: true}
	}

	switch err.Code() {
	case kafka.ErrTransport, kafka.ErrAllBrokersDown:
		// The broker is unreachable. librdkafka is already retrying, so this is
		// not fatal — the stores keep serving what they hold. Log the
		// transition, then throttle: a broker that stays down errors on every
		// single poll.
		if c.connected.CompareAndSwap(true, false) {
			c.logger.Warn().Err(err).Int("code", int(err.Code())).
				Msg("broker connection lost, librdkafka is reconnecting; cached configuration is still served")
		} else if allowed, suppressed := c.errLog.allow(); allowed {
			c.logger.Warn().Err(err).Int("code", int(err.Code())).
				Int("suppressed_since_last_log", suppressed).
				Msg("broker still unreachable, reconnection in progress")
		}

	default:
		if allowed, suppressed := c.errLog.allow(); allowed {
			c.logger.Warn().Err(err).Int("code", int(err.Code())).
				Int("suppressed_since_last_log", suppressed).
				Msg("non-fatal kafka error")
		}
	}

	return Failure{Err: err, Fatal: false}
}

// noteConnected records that the broker answered, logging the recovery if the
// connection had previously been reported lost.
func (c *consumer) noteConnected() {
	// if the current value is false, set it to true and return true;
	// otherwise leave it and return false
	// Broker was believed down → CAS succeeds → log the recovery once.
	// Broker was already up (the normal case, every single poll) → CAS returns false → do nothing.
	if c.connected.CompareAndSwap(false, true) {
		c.errLog.reset()
		c.logger.Info().Msg("broker connection restored, consuming again")
	}
}

// Watermarks returns the low and high watermark offsets for one partition. The
// high watermark is the offset the next record will be given, so a partition is
// fully read once the consumer's position reaches it.
//
// Offsets on a compacted topic are not contiguous — compaction removes
// superseded records — so high minus low is not a record count and must not be
// used as one.
func (c *consumer) Watermarks(partition int32) (low, high int64, err error) {
	c.handleMu.Lock()
	defer c.handleMu.Unlock()

	if c.closed.Load() {
		return 0, 0, ErrClosed
	}

	low, high, err = c.kc.QueryWatermarkOffsets(c.cfg.Topic, partition, int(c.cfg.MetadataTimeout.Milliseconds()))
	if err != nil {
		return 0, 0, fmt.Errorf("querying watermarks for %s[%d]: %w", c.cfg.Topic, partition, err)
	}

	return low, high, nil
}

// Positions returns the next offset to be consumed for each assigned partition.
// A partition that has not yet delivered anything may be absent from the map.
func (c *consumer) Positions() (map[int32]int64, error) {
	c.handleMu.Lock()
	defer c.handleMu.Unlock()

	if c.closed.Load() {
		return nil, ErrClosed
	}

	if len(c.assigned) == 0 {
		return nil, ErrNotAssigned
	}

	query := make([]kafka.TopicPartition, 0, len(c.assigned))
	for _, id := range c.assigned {
		query = append(query, kafka.TopicPartition{Topic: &c.cfg.Topic, Partition: id})
	}

	positions, err := c.kc.Position(query)
	if err != nil {
		return nil, fmt.Errorf("reading consumer position for %q: %w", c.cfg.Topic, err)
	}

	out := make(map[int32]int64, len(positions))
	for _, tp := range positions {
		if tp.Offset < 0 {
			// A logical offset (unset, beginning, end) rather than a real
			// position: nothing has been consumed from this partition yet.
			continue
		}
		out[tp.Partition] = int64(tp.Offset)
	}

	return out, nil
}

// Close releases the consumer, waiting for any in-flight Poll to return first.
//
// Idempotent, and safe from a goroutine other than the one polling: the wait is
// what makes that safe, since librdkafka does not tolerate a consumer closed
// underneath an active poll. The cost is that Close can block for up to one
// poll timeout.
func (c *consumer) Close() error {
	// Set the flag and read what it was: if it was already true, someone else
	// has closed this consumer, so there is nothing to do.
	if c.closed.Swap(true) {
		return nil
	}

	c.handleMu.Lock()
	defer c.handleMu.Unlock()

	if err := c.kc.Close(); err != nil {
		return fmt.Errorf("closing consumer for topic %q: %w", c.cfg.Topic, err)
	}

	c.logger.Debug().Msg("consumer closed")

	return nil
}

// derefTopic guards against a nil topic pointer in a driver event.
func derefTopic(topic *string, fallback string) string {
	if topic == nil {
		return fallback
	}

	return *topic
}
