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

	// ErrEmptyTopic means a topic held no records when it was read to its end,
	// and the binding did not set AllowEmpty. Start reports it; whether an
	// empty topic should stop the service is the service's decision.
	ErrEmptyTopic = errors.New("topic is empty")

	// ErrWarmupTimeout means WithWarmupTimeout elapsed before every topic had
	// been read. The usual cause is a partition whose leader never answers, so
	// no end-of-partition ever arrives.
	ErrWarmupTimeout = errors.New("warm-up timed out")

	// ErrBindAfterStart means Bind was called on a loader that is already
	// running. The topic set is fixed before Start; a programmer error, so Bind
	// panics.
	ErrBindAfterStart = errors.New("bind after start")

	// ErrAlreadyStarted means Start was called twice on one loader.
	ErrAlreadyStarted = errors.New("loader already started")

	// ErrNoBindings means Start was called before anything was bound, which
	// would otherwise succeed while loading nothing.
	ErrNoBindings = errors.New("no bindings registered")

	// ErrUnknownBinding means LookupRaw was given a name no binding uses.
	ErrUnknownBinding = errors.New("unknown binding")

	// ErrClosed means the loader was closed while it was still warming up.
	ErrClosed = errors.New("loader closed")
)
