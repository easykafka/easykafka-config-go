package integration

// The broker goes away and comes back.

import (
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A broker outage must not take the loader down: the stores keep answering from
// what they hold, the errors are reported as non-fatal, and consumption resumes
// when the broker returns.
//
// This is the behaviour nothing else covers at any level. The unit suite can
// inject a non-fatal driver.Failure and check the loader survives it, but only
// a real outage says whether librdkafka reports one — as opposed to a fatal
// error, which would stop every binding and freeze the configuration for the
// life of the process. Getting that classification wrong is the failure mode
// with the worst consequences in the whole library, and this is the only test
// that exercises it.
//
// Uses a dedicated broker, because it stops it: the shared cluster is in use by
// every other test in the binary.
func TestLoaderSurvivesBrokerOutage(t *testing.T) {
	t.Parallel()

	cluster := helpers.DedicatedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-reconnect")

	cluster.CreateCompactedTopic(t, topic, 1)
	cluster.Produce(t, topic,
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":1}`)},
	)

	obs := &recordingObserver{}
	loader := newLoader(t, cluster, ekconfig.WithObserver(obs))
	players := loader.Bind(playerBinding("PlayerConfig", topic))

	startedLoader(t, loader)

	require.Equal(t, 1, players.GetOrNil("p1").Limit, "loaded before the outage")

	cluster.StopBroker(t)

	// The store keeps serving throughout. This is the property that matters
	// operationally: a broker outage must not make a service unable to answer,
	// only unable to learn about changes.
	assert.Equal(t, 1, players.GetOrNil("p1").Limit, "the store must keep serving during the outage")
	assert.Equal(t, 1, players.Len())

	// librdkafka retries in the background, so errors accumulate while the
	// broker is gone. Waiting for one is what proves the loader hears about the
	// outage at all rather than sitting silently.
	eventually(t, "the outage must be reported to the observer", func() bool {
		return len(obs.snapshot().kafkaErrors) > 0
	})

	// And crucially: reported without stopping the loader. A fatal
	// classification here would freeze the configuration permanently.
	require.NoError(t, loader.Err(), "a broker outage must not be treated as fatal")
	assert.Equal(t, ekconfig.PhaseSteady, loader.Stats()[0].Phase, "the binding must still be serving")

	cluster.StartBroker(t)

	// Consumption resumes on its own: a record produced after the outage
	// reaches the store with no intervention from the caller.
	cluster.Produce(t, topic,
		helpers.Record{Key: "p2", Value: []byte(`{"playerId":"p2","limit":2}`)},
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":42}`)},
	)

	eventually(t, "consumption must resume once the broker is back", func() bool {
		p1 := players.GetOrNil("p1")

		return players.Has("p2") && p1 != nil && p1.Limit == 42
	})

	assert.NoError(t, loader.Err(), "and the loader must still be healthy afterwards")
}

// The same outage while warm-up is still in progress: Start must not return
// success on a partially read topic, and must not hang forever either — the
// warm-up timeout is what bounds it.
func TestLoaderWarmupFailsWhenBrokerNeverAnswers(t *testing.T) {
	t.Parallel()

	cluster := helpers.DedicatedCluster(t)
	topic := helpers.UniqueTopic(t, "loader-warmup-outage")

	cluster.CreateCompactedTopic(t, topic, 1)
	cluster.Produce(t, topic,
		helpers.Record{Key: "p1", Value: []byte(`{"playerId":"p1","limit":1}`)},
	)

	// Down before the loader ever starts, so warm-up cannot complete.
	cluster.StopBroker(t)

	loader := newLoader(t, cluster, ekconfig.WithWarmupTimeout(5*time.Second))
	loader.Bind(playerBinding("PlayerConfig", topic))

	started := time.Now()
	err := loader.Start(t.Context())
	elapsed := time.Since(started)

	require.Error(t, err, "Start must not report success on a topic it could not read")
	assert.Less(t, elapsed, 60*time.Second, "and the warm-up timeout must bound it")

	// Nothing is left running, whichever way it failed — a metadata error or
	// the deadline.
	require.NoError(t, loader.WaitUntilStopped())
	t.Logf("Start failed after %s with: %v", elapsed.Round(time.Millisecond), err)
}
