package easykafkaconfig

import "time"

// Phase is where a binding is in its lifecycle.
type Phase string

const (
	// PhaseWarmup means the topic is still being read to its end. Lookups
	// against the store are already possible but may be incomplete.
	PhaseWarmup Phase = "warmup"

	// PhaseSteady means the topic has been read whole and the binding is now
	// applying live changes.
	PhaseSteady Phase = "steady"

	// PhaseStopped means the binding's consumer has stopped and the store is
	// frozen at whatever it last held.
	PhaseStopped Phase = "stopped"
)

// Observer receives everything the loader does per record, keyed by the
// binding's Name. It is how a service attaches metrics or logging without the
// library depending on either.
//
// Calls happen on the binding's own consumer goroutine, so an implementation
// must not block: anything slow belongs behind a counter or a channel. Counting
// is the expected use.
//
// The interface will grow. Embed NopObserver so that adding a method does not
// break an existing implementation.
type Observer interface {
	// OnUpsert reports a record stored. warmup distinguishes the initial read
	// of the topic from a later live change.
	OnUpsert(name string, warmup bool)

	// OnDelete reports a tombstone applied, removing a key.
	OnDelete(name string, warmup bool)

	// OnFiltered reports a record the binding's Filter rejected. It was read
	// and decoded, then deliberately not stored.
	OnFiltered(name string)

	// OnDecodeError reports a record whose key or payload could not be
	// decoded. The record is skipped: bad data never stops a config topic, and
	// there is nothing to retry, since redelivering it would fail identically.
	OnDecodeError(name string, err error, raw []byte)

	// OnKeyMismatch reports that a record's own key and the key derived from
	// its payload disagree, which only VerifyKeyAgreement can detect. It means
	// the entry is stored under one key and could only ever be deleted by a
	// tombstone carrying the other — a permanent leak in the store.
	OnKeyMismatch(name string, fromKey, fromValue string)

	// OnLoadProgress reports how many records have been applied so far during
	// warm-up, for topics large enough that the initial read takes a while.
	OnLoadProgress(name string, applied int)

	// OnPhase reports a binding changing phase, with how many records were
	// applied in the phase just ended and how long it lasted.
	OnPhase(name string, phase Phase, applied int, took time.Duration)

	// OnKafkaError reports a Kafka-level error. Non-fatal ones are routine —
	// an unreachable broker produces them continuously while the stores keep
	// serving what they hold — so treat them as a signal to alert on, not as a
	// reason to act.
	OnKafkaError(name string, err error)
}

// NopObserver implements Observer by doing nothing.
//
// Embed it rather than implementing Observer directly, and override only the
// callbacks of interest; a method added to the interface later then costs
// nothing:
//
//	type myObserver struct{ ekconfig.NopObserver }
//
//	func (myObserver) OnUpsert(name string, _ bool) { upserts.WithLabelValues(name).Inc() }
type NopObserver struct{}

// Compile-time assertion that embedding NopObserver is enough to satisfy Observer.
var _ Observer = NopObserver{}

// OnUpsert does nothing.
func (NopObserver) OnUpsert(_ string, _ bool) {}

// OnDelete does nothing.
func (NopObserver) OnDelete(_ string, _ bool) {}

// OnFiltered does nothing.
func (NopObserver) OnFiltered(_ string) {}

// OnDecodeError does nothing.
func (NopObserver) OnDecodeError(_ string, _ error, _ []byte) {}

// OnKeyMismatch does nothing.
func (NopObserver) OnKeyMismatch(_ string, _, _ string) {}

// OnLoadProgress does nothing.
func (NopObserver) OnLoadProgress(_ string, _ int) {}

// OnPhase does nothing.
func (NopObserver) OnPhase(_ string, _ Phase, _ int, _ time.Duration) {}

// OnKafkaError does nothing.
func (NopObserver) OnKafkaError(_ string, _ error) {}

// BindingStats is a point-in-time view of one binding, for a metrics endpoint or
// a status page. Counters are cumulative since the loader started.
type BindingStats struct {
	// Name and Topic identify the binding.
	Name  string
	Topic string

	// Phase is where the binding currently is.
	Phase Phase

	// Size is the number of entries in the store.
	Size int

	// Cumulative counts of what the binding has done.
	Upserts      uint64
	Deletes      uint64
	Filtered     uint64
	DecodeErrors uint64

	// WarmupApplied and WarmupTook describe the initial read: how many records
	// it applied and how long it took. Both are zero until warm-up completes.
	WarmupApplied int
	WarmupTook    time.Duration

	// LastRecordAt is when a record was last applied, zero if none has been.
	// A steady binding whose topic is quiet legitimately has an old value here.
	LastRecordAt time.Time
}
