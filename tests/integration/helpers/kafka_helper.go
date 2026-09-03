// Package helpers starts a real Kafka broker for the integration suite and
// provides the producing and topic-management calls the tests need.
package helpers

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	kfk "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	defaultKafkaImage = "confluentinc/cp-kafka:7.5.0"
	adminTimeout      = 30 * time.Second
	flushTimeoutMs    = 15_000
)

// kafkaImage returns the Kafka Docker image to use. CI sets KAFKA_IMAGE to a
// GHCR mirror to avoid Docker Hub rate limits; locally it falls back to Docker
// Hub.
func kafkaImage() string {
	if img := os.Getenv("KAFKA_IMAGE"); img != "" {
		return img
	}

	return defaultKafkaImage
}

var (
	sharedCluster *Cluster
	sharedOnce    sync.Once
	sharedErr     error
)

// Cluster is a running Kafka broker.
type Cluster struct {
	container *kafka.KafkaContainer
	Brokers   []string
}

// SharedCluster starts one broker for the whole test binary and reuses it.
//
// Starting a container costs seconds, so tests share one rather than paying
// that per test. Each test must therefore use its own topic name — see
// UniqueTopic — since the broker's state is shared.
func SharedCluster(t *testing.T) *Cluster {
	t.Helper()

	sharedOnce.Do(func() {
		ctx := context.Background()

		container, err := kafka.Run(ctx, kafkaImage(), kafka.WithClusterID("ekconfig-test"))
		if err != nil {
			sharedErr = fmt.Errorf("starting kafka container: %w", err)

			return
		}

		brokers, err := container.Brokers(ctx)
		if err != nil {
			sharedErr = fmt.Errorf("resolving broker addresses: %w", err)

			return
		}

		sharedCluster = &Cluster{container: container, Brokers: brokers}
	})

	if sharedErr != nil {
		t.Fatalf("kafka cluster unavailable: %v", sharedErr)
	}

	return sharedCluster
}

// UniqueTopic returns a topic name unique to this test, so tests sharing the
// broker cannot interfere with one another.
func UniqueTopic(t *testing.T, prefix string) string {
	t.Helper()

	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixNano(), sanitise(t.Name()))
}

// sanitise reduces a test name to characters Kafka accepts in a topic name.
func sanitise(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}

	return string(out)
}

// CreateCompactedTopic creates a compacted topic with the given partition
// count and waits for it to be usable.
func (c *Cluster) CreateCompactedTopic(t *testing.T, topic string, partitions int) {
	t.Helper()

	admin, err := kfk.NewAdminClient(&kfk.ConfigMap{"bootstrap.servers": c.brokerList()})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), adminTimeout)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []kfk.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
		Config: map[string]string{
			"cleanup.policy": "compact",
			// Keep compaction from running during a test: these tests assert on
			// what was produced, not on what compaction later removes.
			"min.cleanable.dirty.ratio": "1.0",
		},
	}})
	if err != nil {
		t.Fatalf("creating topic %s: %v", topic, err)
	}
	for _, r := range results {
		if r.Error.Code() != kfk.ErrNoError && r.Error.Code() != kfk.ErrTopicAlreadyExists {
			t.Fatalf("creating topic %s: %v", topic, r.Error)
		}
	}
}

// Record is a key/value pair to produce. A nil Value produces a tombstone.
type Record struct {
	Key   string
	Value []byte
}

// Produce writes records to a topic and fails the test unless every one of them
// is acknowledged by the broker.
//
// Passing a delivery channel to Produce registers it for that one message, so
// each Produce yields exactly one *kfk.Message report on it — success or
// failure alike, with a failure carrying TopicPartition.Error. Nothing else is
// ever sent there.
//
// Two details are load-bearing:
//
// The buffer must hold every report. Flush drives the event loop that delivers
// them, and that loop blocks on the send. Since the reports are drained only
// after Flush returns, an unbuffered channel would block the first send and
// Flush would then sit until its timeout.
//
// Flush returning zero is what makes the drain non-blocking. It reports how
// many messages are still outstanding, so zero means every report has already
// been generated and is waiting in the buffer.
func (c *Cluster) Produce(t *testing.T, topic string, records ...Record) {
	t.Helper()

	producer, err := kfk.NewProducer(&kfk.ConfigMap{"bootstrap.servers": c.brokerList()})
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	deliveries := make(chan kfk.Event, len(records))
	for _, r := range records {
		msg := &kfk.Message{
			TopicPartition: kfk.TopicPartition{Topic: &topic, Partition: kfk.PartitionAny},
			Key:            []byte(r.Key),
			Value:          r.Value,
		}
		if err := producer.Produce(msg, deliveries); err != nil {
			t.Fatalf("producing to %s: %v", topic, err)
		}
	}

	if remaining := producer.Flush(flushTimeoutMs); remaining > 0 {
		t.Fatalf("producing to %s: %d messages undelivered after %s",
			topic, remaining, time.Duration(flushTimeoutMs)*time.Millisecond)
	}

	// Reports arrive in completion order, not produce order, so the nth report
	// is not necessarily for the nth record — the report's own key identifies
	// which record failed.
	for i := range records {
		ev := <-deliveries

		msg, ok := ev.(*kfk.Message)
		if !ok {
			// Unreachable per the driver's contract. Reported rather than
			// skipped, so a change in that contract surfaces here instead of
			// letting a record silently go unverified.
			t.Fatalf("producing to %s: delivery report %d was %T, want *kafka.Message", topic, i, ev)
		}
		if msg.TopicPartition.Error != nil {
			t.Fatalf("producing to %s: record %q: %v", topic, msg.Key, msg.TopicPartition.Error)
		}
	}
}

// ListConsumerGroups returns the consumer groups the broker currently knows
// about. Used to prove that manual assignment creates none.
func (c *Cluster) ListConsumerGroups(t *testing.T) []string {
	t.Helper()

	admin, err := kfk.NewAdminClient(&kfk.ConfigMap{"bootstrap.servers": c.brokerList()})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), adminTimeout)
	defer cancel()

	result, err := admin.ListConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("listing consumer groups: %v", err)
	}

	groups := make([]string, 0, len(result.Valid))
	for _, g := range result.Valid {
		groups = append(groups, g.GroupID)
	}

	return groups
}

// brokerList joins the broker addresses for a ConfigMap.
func (c *Cluster) brokerList() string {
	return strings.Join(c.Brokers, ",")
}
