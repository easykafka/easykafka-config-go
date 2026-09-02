package easykafkaconfig

import (
	"errors"
	"fmt"
	"strings"
)

// Binding declares how one compacted topic is projected into one Store.
//
// Everything type-parameterised lives here, so a service states K and V once in
// the literal and both are inferred at the call site:
//
//	players := loader.Bind(ekconfig.Binding[string, PlayerConfig]{
//	    Name:        "PlayerConfig",
//	    Topic:       "player-config.compact",
//	    DecodeKey:   ekconfig.StringKey,
//	    DecodeValue: ekconfig.JSONValue,
//	})
type Binding[K comparable, V any] struct {
	// Name is the logical configuration name, e.g. "PlayerConfig". It appears in
	// logs, metrics, Loader.Stats and Loader.LookupRaw. Required, and unique per
	// Loader.
	Name string

	// Topic is the compacted Kafka topic to read. Required.
	Topic string

	// DecodeKey turns a Kafka record key into the map key. Required: a tombstone
	// carries no payload, so the record key is the only key available for a
	// delete.
	DecodeKey func(raw []byte) (K, error)

	// DecodeValue turns a record payload into a value. Required. Use JSONValue
	// for the common case.
	DecodeValue func(raw []byte) (*V, error)

	// KeyFromValue, when set, overrides DecodeKey for upserts: the map key is
	// derived from the decoded payload instead of the record key. Deletes always
	// use DecodeKey, since a tombstone has no payload to derive a key from.
	//
	// Leave it nil unless the payload is genuinely authoritative — with it set,
	// a producer that disagrees with itself (record key 42, payload id 43)
	// stores an entry that can never be deleted. VerifyKeyAgreement detects
	// exactly that.
	KeyFromValue func(v *V) K

	// VerifyKeyAgreement compares the key decoded from the record with the one
	// derived by KeyFromValue on every upsert and reports any mismatch to the
	// Observer. Requires KeyFromValue.
	VerifyKeyAgreement bool

	// Filter is consulted before a value is stored. Returning false drops the
	// record; it is still counted as read and reported as filtered. Typical use
	// is ignoring synthetic-monitor traffic.
	Filter func(key K, v *V) bool

	// Tombstone decides which records mean "delete this key".
	// Defaults to TombstoneOnBlankPayload when nil.
	Tombstone TombstonePolicy

	// AllowEmpty permits the topic to be empty when warm-up completes. Default
	// false: an empty topic makes Loader.Start report ErrEmptyTopic, on the
	// assumption that missing configuration is a deployment fault rather than a
	// valid state.
	AllowEmpty bool
}

// Validate reports whether the binding is usable, describing every problem it
// finds rather than only the first.
func (b Binding[K, V]) Validate() error {
	var errs []error

	if strings.TrimSpace(b.Name) == "" {
		errs = append(errs, fmt.Errorf("%w: Name is required", ErrInvalidBinding))
	}
	if strings.TrimSpace(b.Topic) == "" {
		errs = append(errs, fmt.Errorf("%w %q: Topic is required", ErrInvalidBinding, b.Name))
	}
	if b.DecodeKey == nil {
		errs = append(errs, fmt.Errorf("%w %q: DecodeKey is required", ErrInvalidBinding, b.Name))
	}
	if b.DecodeValue == nil {
		errs = append(errs, fmt.Errorf("%w %q: DecodeValue is required", ErrInvalidBinding, b.Name))
	}
	if b.VerifyKeyAgreement && b.KeyFromValue == nil {
		errs = append(errs,
			fmt.Errorf("%w %q: VerifyKeyAgreement requires KeyFromValue", ErrInvalidBinding, b.Name))
	}

	return errors.Join(errs...)
}
