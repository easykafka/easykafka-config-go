package unit

import (
	"math"
	"strconv"
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringKey(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw  []byte
		want string
	}{
		"plain":            {raw: []byte("player-42"), want: "player-42"},
		"empty":            {raw: []byte(""), want: ""},
		"nil":              {raw: nil, want: ""},
		"composite":        {raw: []byte("hr:123"), want: "hr:123"},
		"non-utf8 is kept": {raw: []byte{0xff, 0xfe}, want: string([]byte{0xff, 0xfe})},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := ekconfig.StringKey(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIntKey(t *testing.T) {
	t.Parallel()

	got, err := ekconfig.IntKey([]byte("42"))
	require.NoError(t, err)
	assert.Equal(t, 42, got)

	got, err = ekconfig.IntKey([]byte("-7"))
	require.NoError(t, err)
	assert.Equal(t, -7, got)
}

// A malformed key must be an error, never a sentinel: substituting something
// like -1 would store or delete an entry under a key no producer ever wrote.
func TestIntKeyRejectsMalformed(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string][]byte{
		"empty":            []byte(""),
		"nil":              nil,
		"whitespace":       []byte(" 42 "),
		"not a number":     []byte("abc"),
		"float":            []byte("4.2"),
		"trailing garbage": []byte("42x"),
		"overflow":         []byte("999999999999999999999999"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := ekconfig.IntKey(raw)
			require.Error(t, err)
			assert.Zero(t, got, "no sentinel value on failure")
			assert.Contains(t, err.Error(), "record key", "the error must name what failed")
		})
	}
}

func TestInt64Key(t *testing.T) {
	t.Parallel()

	got, err := ekconfig.Int64Key([]byte(strconv.FormatInt(math.MaxInt64, 10)))
	require.NoError(t, err)
	assert.Equal(t, int64(math.MaxInt64), got)

	_, err = ekconfig.Int64Key([]byte("9223372036854775808")) // MaxInt64 + 1
	require.Error(t, err)
	assert.Contains(t, err.Error(), "int64")
}

func TestJSONValue(t *testing.T) {
	t.Parallel()

	got, err := ekconfig.JSONValue[playerConfig]([]byte(`{"playerId":"p1","limit":250}`))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "p1", got.PlayerID)
	assert.Equal(t, 250, got.Limit)
}

func TestJSONValueRejectsMalformed(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string][]byte{
		"empty":            []byte(""),
		"nil":              nil,
		"truncated":        []byte(`{"playerId":`),
		"not an object":    []byte(`["p1"]`),
		"wrong field type": []byte(`{"limit":"not a number"}`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := ekconfig.JSONValue[playerConfig](raw)
			require.Error(t, err)
			assert.Nil(t, got, "a failed decode must not return a half-built value")
		})
	}
}

// Unknown fields are ignored rather than rejected: producers add fields to
// configuration payloads without coordinating with every consumer.
func TestJSONValueIgnoresUnknownFields(t *testing.T) {
	t.Parallel()

	got, err := ekconfig.JSONValue[playerConfig]([]byte(`{"playerId":"p1","addedLater":true}`))
	require.NoError(t, err)
	assert.Equal(t, "p1", got.PlayerID)
}

// encoding/json (v1) matches field names case-insensitively as a fallback. This
// is the documented reason JSONValue does not use encoding/json/v2, which
// matches case-sensitively and would silently stop populating such fields.
func TestJSONValueMatchesFieldNamesCaseInsensitively(t *testing.T) {
	t.Parallel()

	got, err := ekconfig.JSONValue[playerConfig]([]byte(`{"PLAYERID":"p1","Limit":5}`))
	require.NoError(t, err)
	assert.Equal(t, "p1", got.PlayerID)
	assert.Equal(t, 5, got.Limit)
}

// JSONValue is assignable to Binding.DecodeValue. Both forms below are
// compile-time assertions as much as tests.
//
// Three spellings, only two of which compile on go1.27.0:
//
//	var f func([]byte) (*playerConfig, error) = ekconfig.JSONValue  // ok: inferred
//	DecodeValue: ekconfig.JSONValue[playerConfig]                   // ok: explicit
//	DecodeValue: ekconfig.JSONValue                                 // ICE, see below
//
// The asymmetry is a compiler bug, not a language rule — the language permits
// the third form, and the first shows the same inference working. The third
// crashes the compiler with an ICE (internal compiler error — the compiler
// failing on its own bug rather than rejecting the code):
//
//	internal compiler error: func[V any](raw []byte) (*V, error) is not
//	assignable to func(raw []byte) (*unit.playerConfig, error)
//
// It reproduces in ~14 lines whenever the generic function comes from another
// package and is used as a composite-literal field value — the struct need not
// even be generic.
//
// The broken form cannot appear in this test: it fails at compile time, so it
// would break the build of the whole package. Hence this comment.
//
// Nothing is pending in the library. Every binding spells out the type argument,
// which is preferable anyway — JSONValue[playerConfig] says what it decodes at
// the call site instead of leaving a reader to infer it from the enclosing
// literal. The only outstanding action is external: report the ICE to golang/go,
// which the error message itself asks for.
func TestJSONValueIsAssignableToDecodeValue(t *testing.T) {
	t.Parallel()

	// Works: assignment to a func-typed variable infers V. The explicit type is
	// what drives that inference, so it cannot be omitted here.
	var decode func([]byte) (*playerConfig, error) = ekconfig.JSONValue //nolint:staticcheck // ST1023: type drives inference

	// Works: explicit type argument inside a composite literal.
	binding := ekconfig.Binding[string, playerConfig]{
		Name:        "PlayerConfig",
		Topic:       "player-config.compact",
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[playerConfig],
	}

	require.NoError(t, binding.Validate())

	got, err := decode([]byte(`{"playerId":"p1"}`))
	require.NoError(t, err)
	assert.Equal(t, "p1", got.PlayerID)

	got, err = binding.DecodeValue([]byte(`{"playerId":"p2"}`))
	require.NoError(t, err)
	assert.Equal(t, "p2", got.PlayerID)
}

func TestTombstoneOnBlankPayload(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw  []byte
		want bool
	}{
		"nil":            {raw: nil, want: true},
		"empty":          {raw: []byte(""), want: true},
		"space":          {raw: []byte(" "), want: true},
		"tabs and lines": {raw: []byte("\t\n\r "), want: true},
		"json object":    {raw: []byte(`{"limit":1}`), want: false},
		"json null":      {raw: []byte("null"), want: false},
		"empty json":     {raw: []byte("{}"), want: false},
		"zero byte":      {raw: []byte{0x00}, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ekconfig.TombstoneOnBlankPayload(tc.raw))
		})
	}
}

func TestTombstoneOnNilPayload(t *testing.T) {
	t.Parallel()

	assert.True(t, ekconfig.TombstoneOnNilPayload(nil))
	assert.False(t, ekconfig.TombstoneOnNilPayload([]byte("")), "empty but present is not a tombstone")
	assert.False(t, ekconfig.TombstoneOnNilPayload([]byte(" ")))
	assert.False(t, ekconfig.TombstoneOnNilPayload([]byte(`{"limit":1}`)))
}

func TestTombstoneNever(t *testing.T) {
	t.Parallel()

	assert.False(t, ekconfig.TombstoneNever(nil))
	assert.False(t, ekconfig.TombstoneNever([]byte("")))
	assert.False(t, ekconfig.TombstoneNever([]byte(`{"limit":1}`)))
}

// The policies are plain functions, so they are assignable to the Binding field
// without a wrapper — and cannot be reassigned by another package the way
// exported function variables could.
func TestTombstonePoliciesAreAssignable(t *testing.T) {
	t.Parallel()

	for name, policy := range map[string]ekconfig.TombstonePolicy{
		"blank": ekconfig.TombstoneOnBlankPayload,
		"nil":   ekconfig.TombstoneOnNilPayload,
		"never": ekconfig.TombstoneNever,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			binding := ekconfig.Binding[string, playerConfig]{
				Name:        "PlayerConfig",
				Topic:       "player-config.compact",
				DecodeKey:   ekconfig.StringKey,
				DecodeValue: ekconfig.JSONValue[playerConfig],
				Tombstone:   policy,
			}
			require.NoError(t, binding.Validate())
			assert.NotNil(t, binding.Tombstone)
		})
	}
}
