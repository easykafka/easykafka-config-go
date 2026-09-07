package unit

import (
	"errors"
	"io"
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Brokers are the one thing with no sensible default, so a loader cannot be
// built without them.
func TestNewLoaderRequiresBrokers(t *testing.T) {
	t.Parallel()

	loader, err := ekconfig.NewLoader()
	require.Error(t, err)
	assert.Nil(t, loader)
	assert.Contains(t, err.Error(), "at least one broker")
}

// Everything else has a default, so this is the minimum viable configuration.
func TestNewLoaderMinimal(t *testing.T) {
	t.Parallel()

	loader, err := ekconfig.NewLoader(ekconfig.WithBrokers("localhost:9092"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	// Nothing has run, so there is no error and nothing bound. Waiting on a
	// loader that was never started returns immediately rather than hanging.
	require.NoError(t, loader.Err())
	require.NoError(t, loader.WaitUntilStopped())
	assert.Empty(t, loader.Stats())
}

// Every option is exercised together, which also proves they compose rather
// than overwriting one another.
func TestNewLoaderAcceptsEveryOption(t *testing.T) {
	t.Parallel()

	loader, err := ekconfig.NewLoader(
		ekconfig.WithBrokers("a:9092", "b:9092"),
		ekconfig.WithClientGroupID("my-reader"),
		ekconfig.WithSASL("SASL_SSL", "SCRAM-SHA-512", "user", "secret"),
		ekconfig.WithKafkaConfig(map[string]any{"fetch.min.bytes": 1024}),
		ekconfig.WithInitialLoadDetector(ekconfig.PartitionEOF()),
		ekconfig.WithSteadyPollTimeout(50*time.Millisecond),
		ekconfig.WithWarmupTimeout(time.Minute),
		ekconfig.WithMetadataTimeout(2*time.Second),
		ekconfig.WithLogger(zerolog.New(io.Discard)),
		ekconfig.WithObserver(ekconfig.NopObserver{}),
		ekconfig.WithFatalHandler(func(error) {}),
		ekconfig.WithConsumerFactory(func(driver.Config) (driver.Consumer, error) {
			return nil, errors.New("unused")
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, loader)
}

// Each option rejects its own bad input at construction, so a misconfiguration
// cannot survive until Start.
func TestOptionValidation(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		opt     ekconfig.Option
		wantMsg string
	}{
		"no brokers": {
			opt:     ekconfig.WithBrokers(),
			wantMsg: "at least one address",
		},
		"blank broker": {
			opt:     ekconfig.WithBrokers(" "),
			wantMsg: "broker address cannot be empty",
		},
		"blank group id": {
			opt:     ekconfig.WithClientGroupID("  "),
			wantMsg: "non-empty id",
		},
		"no security protocol": {
			opt:     ekconfig.WithSASL("", "PLAIN", "u", "p"),
			wantMsg: "security protocol",
		},
		"nil kafka config": {
			opt:     ekconfig.WithKafkaConfig(nil),
			wantMsg: "non-nil map",
		},
		"nil detector": {
			opt:     ekconfig.WithInitialLoadDetector(nil),
			wantMsg: "needs a detector",
		},
		"zero steady poll timeout": {
			opt:     ekconfig.WithSteadyPollTimeout(0),
			wantMsg: "positive duration",
		},
		"negative steady poll timeout": {
			opt:     ekconfig.WithSteadyPollTimeout(-time.Second),
			wantMsg: "positive duration",
		},
		"negative warm-up timeout": {
			opt:     ekconfig.WithWarmupTimeout(-time.Second),
			wantMsg: "cannot be negative",
		},
		"zero metadata timeout": {
			opt:     ekconfig.WithMetadataTimeout(0),
			wantMsg: "positive duration",
		},
		"nil observer": {
			opt:     ekconfig.WithObserver(nil),
			wantMsg: "needs an observer",
		},
		"nil fatal handler": {
			opt:     ekconfig.WithFatalHandler(nil),
			wantMsg: "needs a function",
		},
		"nil consumer factory": {
			opt:     ekconfig.WithConsumerFactory(nil),
			wantMsg: "needs a function",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ekconfig.NewLoader(ekconfig.WithBrokers("localhost:9092"), tc.opt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// A nil option is a programmer slip — usually a conditional that returned
// nothing — and is reported with its position rather than panicking somewhere
// later.
func TestNewLoaderRejectsNilOption(t *testing.T) {
	t.Parallel()

	_, err := ekconfig.NewLoader(ekconfig.WithBrokers("localhost:9092"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "option 1 is nil")
}

// WithKafkaConfig merges rather than replaces, so two calls do not silently
// discard the first.
func TestWithKafkaConfigMerges(t *testing.T) {
	t.Parallel()

	loader, err := ekconfig.NewLoader(
		ekconfig.WithBrokers("localhost:9092"),
		ekconfig.WithKafkaConfig(map[string]any{"fetch.min.bytes": 1}),
		ekconfig.WithKafkaConfig(map[string]any{"fetch.wait.max.ms": 2}),
	)
	require.NoError(t, err)
	require.NotNil(t, loader)
}

// WithBrokers copies its input, so a caller mutating the slice afterwards
// cannot change where the loader connects.
func TestWithBrokersCopiesInput(t *testing.T) {
	t.Parallel()

	brokers := []string{"first:9092"}

	loader, err := ekconfig.NewLoader(ekconfig.WithBrokers(brokers...))
	require.NoError(t, err)

	brokers[0] = " " // would have failed validation had it been read now

	require.NotNil(t, loader)
}

// NopObserver must satisfy the whole interface, which is what makes embedding it
// a safe hedge against the interface growing.
func TestNopObserverImplementsEverything(t *testing.T) {
	t.Parallel()

	var obs ekconfig.Observer = ekconfig.NopObserver{}

	assert.NotPanics(t, func() {
		obs.OnUpsert("n", true)
		obs.OnDelete("n", false)
		obs.OnFiltered("n")
		obs.OnDecodeError("n", errors.New("x"), []byte("raw"))
		obs.OnKeyMismatch("n", "a", "b")
		obs.OnLoadProgress("n", 1)
		obs.OnPhase("n", ekconfig.PhaseSteady, 1, time.Second)
		obs.OnKafkaError("n", errors.New("x"))
	})
}

// Embedding NopObserver and overriding one method must satisfy the interface —
// the pattern the docs recommend.
func TestObserverEmbeddingPattern(t *testing.T) {
	t.Parallel()

	var obs ekconfig.Observer = &partialObserver{}

	assert.NotPanics(t, func() {
		obs.OnDelete("n", false)
		obs.OnPhase("n", ekconfig.PhaseWarmup, 0, 0)
	})

	po, ok := obs.(*partialObserver)
	require.True(t, ok)
	obs.OnUpsert("n", true)
	assert.Equal(t, 1, po.upserts)
}

type partialObserver struct {
	ekconfig.NopObserver

	upserts int
}

func (p *partialObserver) OnUpsert(_ string, _ bool) { p.upserts++ }
