// Package helpers starts a real Kafka broker for the integration suite and
// provides the producing and topic-management calls the tests need.
package helpers

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	kfk "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
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

// DedicatedCluster starts a broker for one test and terminates it afterwards.
//
// For tests that must disturb the broker itself — stopping it to watch a client
// reconnect — which the shared cluster cannot host, since every other test in
// the binary is using it concurrently. Costs a container start, so it is worth
// it only for that.
func DedicatedCluster(t *testing.T) *Cluster {
	t.Helper()

	ctx := context.Background()

	// The host port is pinned rather than left to Docker, because a stop and
	// start would otherwise hand the broker a different one — verified: 55008
	// became 55009 — and a client that survived the outage would then be
	// reconnecting to nothing. Pinning it is what makes the outage look to the
	// client like the broker it already knows going away and coming back.
	port := freePort(t)

	broker, err := kafka.Run(ctx, kafkaImage(),
		kafka.WithClusterID("ekconfig-dedicated"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				// Both families. The broker advertises "localhost", which
				// resolves to ::1 first, so an IPv4-only binding is refused by
				// every client that gets that far — the admin client falls back
				// to IPv4 and works, while the producer does not, which makes
				// the failure look like a broken broker rather than a binding.
				network.MustParsePort(kafkaBrokerPort): []network.PortBinding{
					{HostIP: netip.IPv4Unspecified(), HostPort: port},
					{HostIP: netip.IPv6Unspecified(), HostPort: port},
				},
			}
		}),
	)
	if err != nil {
		t.Fatalf("starting dedicated kafka container: %v", err)
	}

	t.Cleanup(func() {
		if err := broker.Terminate(context.Background()); err != nil {
			t.Logf("terminating dedicated kafka container: %v", err)
		}
	})

	brokers, err := broker.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolving broker addresses: %v", err)
	}

	return &Cluster{container: broker, Brokers: brokers}
}

// kafkaBrokerPort is the container port the Kafka module publishes.
const kafkaBrokerPort = "9093/tcp"

// freePort reserves a port by binding and releasing it, and returns it for the
// container to claim.
//
// Racy in principle — something else could take it in between — but the window
// is microseconds and the alternative is a hard-coded port that collides with
// whatever is already running.
func freePort(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a host port: %v", err)
	}
	defer func() { _ = l.Close() }()

	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("reading the reserved port: %v", err)
	}

	return port
}

// StopBroker stops the broker container, as an outage would.
//
// The host port mapping survives a stop, so the addresses handed to a client
// before the outage are still the right ones when StartBroker brings it back —
// which is what makes a reconnection observable rather than a permanent
// failure. Only use this on a DedicatedCluster.
func (c *Cluster) StopBroker(t *testing.T) {
	t.Helper()

	timeout := adminTimeout
	if err := c.container.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stopping broker: %v", err)
	}
}

// StartBroker brings a stopped broker back at the same address.
func (c *Cluster) StartBroker(t *testing.T) {
	t.Helper()

	if err := c.container.Start(context.Background()); err != nil {
		t.Fatalf("restarting broker: %v", err)
	}

	// The addresses must not have moved, or a client that survived the outage
	// would be reconnecting to nothing and the test would prove the opposite of
	// what it claims.
	brokers, err := c.container.Brokers(context.Background())
	if err != nil {
		t.Fatalf("resolving broker addresses after restart: %v", err)
	}
	if strings.Join(brokers, ",") != strings.Join(c.Brokers, ",") {
		t.Fatalf("broker moved across the restart: was %v, now %v — this test cannot say anything "+
			"about reconnection", c.Brokers, brokers)
	}

	c.waitReady(t)
}

// waitReady blocks until the broker answers a metadata request.
//
// Starting a container is not the same as the broker inside it being ready, and
// testcontainers does not re-apply its wait strategy to a restart — so without
// this, the first produce after StartBroker races Kafka's startup and fails
// with everything undelivered.
func (c *Cluster) waitReady(t *testing.T) {
	t.Helper()

	admin, err := kfk.NewAdminClient(&kfk.ConfigMap{"bootstrap.servers": c.brokerList()})
	if err != nil {
		t.Fatalf("creating admin client to await readiness: %v", err)
	}
	defer admin.Close()

	deadline := time.Now().Add(adminTimeout)
	for time.Now().Before(deadline) {
		if _, err := admin.GetMetadata(nil, true, 2_000); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("broker did not become ready within %s of being restarted", adminTimeout)
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
