package easykafkaconfig

import "errors"

// Sentinel errors returned by the library. Match them with errors.Is.
//
// The library never terminates the process: an empty required topic, a
// malformed record or a lost broker surfaces as an error, as loader lifecycle
// state, or through the Observer. Only the service decides whether a pod should
// die.
var (
	// ErrInvalidBinding indicates a Binding is not usable — a missing Name,
	// Topic, DecodeKey or DecodeValue, or VerifyKeyAgreement without
	// KeyFromValue. Returned by Binding.Validate; a programmer error, so Bind
	// panics on it rather than returning it.
	ErrInvalidBinding = errors.New("invalid binding")
)
