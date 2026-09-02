package easykafkaconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Binding.DecodeKey, Binding.DecodeValue and Binding.Tombstone are plain
// function fields, so a binding can always supply its own implementation — a
// composite key, a generated decoder, a stricter tombstone rule. The functions
// below cover the common cases.

// StringKey decodes a record key as a string. It never fails.
func StringKey(raw []byte) (string, error) {
	return string(raw), nil
}

// IntKey decodes a record key as a base-10 int.
//
// A malformed key is an error, never a sentinel value: silently substituting
// something like -1 would store or delete an entry under a key no producer ever
// wrote. The record is skipped and reported to the Observer instead.
func IntKey(raw []byte) (int, error) {
	n, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, fmt.Errorf("record key %q is not an int: %w", raw, err)
	}

	return n, nil
}

// Int64Key decodes a record key as a base-10 int64. Like IntKey, a malformed
// key is an error.
func Int64Key(raw []byte) (int64, error) {
	n, err := strconv.ParseInt(string(raw), 10, 64) //nolint:mnd // base 10, 64-bit
	if err != nil {
		return 0, fmt.Errorf("record key %q is not an int64: %w", raw, err)
	}

	return n, nil
}

// JSONValue decodes a record payload as JSON into a freshly allocated V.
//
// It is generic because V differs per binding, but it is still an ordinary
// function assignable to Binding.DecodeValue:
//
//	DecodeValue: ekconfig.JSONValue[MyConfig]
//
// Spell out the type argument. In principle the binding's type parameters pin V
// and the bare form would infer it, but on go1.27.0 an uninstantiated generic
// function from another package used as a composite-literal field value crashes
// the compiler ("internal compiler error: ... is not assignable to ..."). The
// bare form does work when assigning to a func-typed variable.
//
// This uses encoding/json (v1) deliberately. encoding/json/v2 is available in
// the Go standard library, but it matches field names case-sensitively where v1
// falls back to case-insensitive matching — switching would silently stop
// populating fields whose producer capitalisation differs from the struct tag.
// A service that wants v2, or a generated decoder, supplies its own function.
func JSONValue[V any](raw []byte) (*V, error) {
	v := new(V)
	if err := json.Unmarshal(raw, v); err != nil {
		return nil, fmt.Errorf("decoding %T: %w", v, err)
	}

	return v, nil
}

// TombstonePolicy reports whether a record payload means "delete this key".
type TombstonePolicy func(raw []byte) bool

// TombstoneOnBlankPayload treats a nil, empty or whitespace-only payload as a
// delete. It is the default, and matches how SRM producers signal removal
// today: a record whose value trims to the empty string.
func TombstoneOnBlankPayload(raw []byte) bool {
	return len(bytes.TrimSpace(raw)) == 0
}

// TombstoneOnNilPayload treats only a genuinely null payload as a delete, which
// is strict Kafka compaction semantics. An empty-but-present payload is then
// passed to DecodeValue like any other record.
func TombstoneOnNilPayload(raw []byte) bool {
	return raw == nil
}

// TombstoneNever disables deletes: every record is an upsert. Use it for topics
// whose producers never tombstone, so a blank payload surfaces as a decode
// error rather than silently removing configuration.
func TombstoneNever([]byte) bool {
	return false
}
