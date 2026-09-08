package integration

// Shared fixtures for the loader integration tests.
//
// These run a real loader over a real broker, which is a different job from the
// driver tests beside them: those check what Kafka does, these check what the
// loader does with it. The unit suite covers the loader's logic against a
// scripted fake, so what is worth testing here is only what a fake cannot
// prove — that the assumptions the fake encodes hold against librdkafka.

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/require"
)

// playerConfig is the value type the loader tests bind. Small on purpose: these
// tests are about the loader, not about decoding.
type playerConfig struct {
	PlayerID string `json:"playerId"`
	Limit    int    `json:"limit"`
}

// playerBinding is the binding every loader test starts from.
func playerBinding(name, topic string) ekconfig.Binding[string, playerConfig] {
	return ekconfig.Binding[string, playerConfig]{
		Name:        name,
		Topic:       topic,
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[playerConfig],
	}
}

// newLoader builds a loader pointed at the cluster, with the short steady poll
// timeout the live-update tests need to finish quickly.
func newLoader(t *testing.T, c *helpers.Cluster, extra ...ekconfig.Option) *ekconfig.Loader {
	t.Helper()

	opts := append([]ekconfig.Option{
		ekconfig.WithBrokers(c.Brokers...),
		ekconfig.WithSteadyPollTimeout(50 * time.Millisecond),
		ekconfig.WithWarmupTimeout(60 * time.Second),
	}, extra...)

	loader, err := ekconfig.NewLoader(opts...)
	require.NoError(t, err)

	return loader
}

// startedLoader starts the loader and registers the shutdown: cancel the
// context, then wait for every real consumer to close.
//
// Returning the cancel matters — a test that wants to observe shutdown itself
// calls it early, and calling it twice is harmless.
func startedLoader(t *testing.T, loader *ekconfig.Loader) context.CancelFunc {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	require.NoError(t, loader.Start(ctx), "warm-up must succeed")

	t.Cleanup(func() {
		cancel()
		if err := loader.WaitUntilStopped(); err != nil {
			t.Logf("loader stopped with: %v", err)
		}
	})

	return cancel
}

// records turns keys into producible records carrying a limit derived from the
// key's position, so a test can assert values rather than just presence.
func records(keys ...string) []helpers.Record {
	out := make([]helpers.Record, 0, len(keys))
	for i, k := range keys {
		out = append(out, helpers.Record{
			Key:   k,
			Value: []byte(`{"playerId":"` + k + `","limit":` + strconv.Itoa(i+1) + `}`),
		})
	}

	return out
}

// eventually polls until the condition holds, for assertions about records that
// arrive after Start has returned.
//
// A generous bound: the poll timeout decides the real latency, and a long
// ceiling means a failure here says "this never happened" rather than "the
// machine was busy".
func eventually(t *testing.T, why string, cond func() bool) {
	t.Helper()

	require.Eventually(t, cond, 30*time.Second, 25*time.Millisecond, why)
}

// recordingObserver captures the callbacks, so a test can assert the loader
// reported what it did as well as stored it.
type recordingObserver struct {
	ekconfig.NopObserver // so the interface can grow without breaking this

	mu            sync.Mutex
	upserts       int
	deletes       int
	filtered      int
	decodeErrors  []string
	kafkaErrors   []string
	loadProgress  int
	phases        []string
	warmupUpserts int
}

func (o *recordingObserver) OnUpsert(_ string, warmup bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.upserts++
	if warmup {
		o.warmupUpserts++
	}
}

func (o *recordingObserver) OnDelete(_ string, _ bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.deletes++
}

func (o *recordingObserver) OnFiltered(_ string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.filtered++
}

func (o *recordingObserver) OnDecodeError(name string, err error, _ []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.decodeErrors = append(o.decodeErrors, name+": "+err.Error())
}

func (o *recordingObserver) OnKafkaError(name string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.kafkaErrors = append(o.kafkaErrors, name+": "+err.Error())
}

func (o *recordingObserver) OnLoadProgress(_ string, applied int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.loadProgress = applied
}

func (o *recordingObserver) OnPhase(name string, phase ekconfig.Phase, _ int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.phases = append(o.phases, name+":"+string(phase))
}

// snapshot copies the counts, so assertions never read them while a binding's
// goroutine is still reporting.
func (o *recordingObserver) snapshot() recordingObserver {
	o.mu.Lock()
	defer o.mu.Unlock()

	return recordingObserver{
		upserts:       o.upserts,
		deletes:       o.deletes,
		filtered:      o.filtered,
		decodeErrors:  append([]string(nil), o.decodeErrors...),
		kafkaErrors:   append([]string(nil), o.kafkaErrors...),
		loadProgress:  o.loadProgress,
		phases:        append([]string(nil), o.phases...),
		warmupUpserts: o.warmupUpserts,
	}
}
