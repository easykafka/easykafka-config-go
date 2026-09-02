package unit

import (
	"fmt"
	"strings"
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validBinding() ekconfig.Binding[string, playerConfig] {
	return ekconfig.Binding[string, playerConfig]{
		Name:        "PlayerConfig",
		Topic:       "player-config.compact",
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[playerConfig],
	}
}

func TestBindingValidateMinimal(t *testing.T) {
	t.Parallel()

	require.NoError(t, validBinding().Validate())
}

func TestBindingValidateFullyPopulated(t *testing.T) {
	t.Parallel()

	binding := validBinding()
	binding.KeyFromValue = func(v *playerConfig) string { return v.PlayerID }
	binding.VerifyKeyAgreement = true
	binding.Filter = func(_ string, v *playerConfig) bool { return v.Limit > 0 }
	binding.Tombstone = ekconfig.TombstoneOnNilPayload
	binding.AllowEmpty = true

	require.NoError(t, binding.Validate())
}

func TestBindingValidateRequiredFields(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate  func(*ekconfig.Binding[string, playerConfig])
		wantMsg string
	}{
		"missing name": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.Name = "" },
			wantMsg: "Name is required",
		},
		"blank name": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.Name = "   " },
			wantMsg: "Name is required",
		},
		"missing topic": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.Topic = "" },
			wantMsg: "Topic is required",
		},
		"blank topic": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.Topic = "\t" },
			wantMsg: "Topic is required",
		},
		"missing key decoder": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.DecodeKey = nil },
			wantMsg: "DecodeKey is required",
		},
		"missing value decoder": {
			mutate:  func(b *ekconfig.Binding[string, playerConfig]) { b.DecodeValue = nil },
			wantMsg: "DecodeValue is required",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			binding := validBinding()
			tc.mutate(&binding)

			err := binding.Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, ekconfig.ErrInvalidBinding)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// VerifyKeyAgreement compares the record key against the key derived from the
// payload, so it is meaningless — and almost certainly a mistake — without
// KeyFromValue.
func TestBindingValidateVerifyKeyAgreementNeedsKeyFromValue(t *testing.T) {
	t.Parallel()

	binding := validBinding()
	binding.VerifyKeyAgreement = true

	err := binding.Validate()
	require.Error(t, err)
	require.ErrorIs(t, err, ekconfig.ErrInvalidBinding)
	assert.Contains(t, err.Error(), "VerifyKeyAgreement requires KeyFromValue")
}

// Validate reports every problem at once rather than only the first, so a
// misconfigured binding takes one round trip to fix instead of four.
func TestBindingValidateReportsAllProblems(t *testing.T) {
	t.Parallel()

	var empty ekconfig.Binding[string, playerConfig]

	err := empty.Validate()
	require.Error(t, err)
	require.ErrorIs(t, err, ekconfig.ErrInvalidBinding)

	msg := err.Error()
	for _, want := range []string{
		"Name is required",
		"Topic is required",
		"DecodeKey is required",
		"DecodeValue is required",
	} {
		assert.Contains(t, msg, want)
	}
}

// The binding names the failing configuration, so an error from a loader with
// ten bindings says which one is wrong.
func TestBindingValidateErrorNamesTheBinding(t *testing.T) {
	t.Parallel()

	binding := validBinding()
	binding.Topic = ""

	err := binding.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PlayerConfig")
}

// Int-keyed bindings are the other common shape: a topic keyed by a numeric id
// rather than a string.
func TestBindingValidateIntKeyed(t *testing.T) {
	t.Parallel()

	type baseLimitTemplate struct {
		ID    int `json:"id"`
		Limit int `json:"limit"`
	}

	binding := ekconfig.Binding[int, baseLimitTemplate]{
		Name:         "BaseLimitTemplate",
		Topic:        "base-limit-template.compact",
		DecodeKey:    ekconfig.IntKey,
		DecodeValue:  ekconfig.JSONValue[baseLimitTemplate],
		KeyFromValue: func(v *baseLimitTemplate) int { return v.ID },
	}

	require.NoError(t, binding.Validate())
}

// A composite key needs no registration anywhere — DecodeKey is just a function,
// and K becomes whatever it returns. Here a record key of "EUR:USD" decodes into
// a two-field struct, so the store is keyed by the pair rather than by a string
// every caller would have to format identically.
func TestBindingValidateCustomStructKey(t *testing.T) {
	t.Parallel()

	type currencyPair struct {
		From string
		To   string
	}

	type exchangeRate struct {
		Rate float64 `json:"rate"`
	}

	binding := ekconfig.Binding[currencyPair, exchangeRate]{
		Name:  "ExchangeRate",
		Topic: "exchange-rate.compact",
		DecodeKey: func(raw []byte) (currencyPair, error) {
			from, to, ok := strings.Cut(string(raw), ":")
			if !ok {
				return currencyPair{}, fmt.Errorf("malformed currency pair %q", raw)
			}

			return currencyPair{From: from, To: to}, nil
		},
		DecodeValue: ekconfig.JSONValue[exchangeRate],
	}

	require.NoError(t, binding.Validate())

	key, err := binding.DecodeKey([]byte("EUR:USD"))
	require.NoError(t, err)
	assert.Equal(t, currencyPair{From: "EUR", To: "USD"}, key)

	_, err = binding.DecodeKey([]byte("EURUSD"))
	require.Error(t, err, "a key the decoder cannot split must be an error, not a half-filled struct")

	store := ekconfig.NewStore[currencyPair, exchangeRate]()
	store.Put(key, &exchangeRate{Rate: 1.09})

	assert.InDelta(t, 1.09, store.GetOrNil(key).Rate, 0.0001)
	assert.Nil(t, store.GetOrNil(currencyPair{From: "USD", To: "EUR"}), "the reversed pair is a different key")
}
