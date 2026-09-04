package easykafkaconfig

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
)

// registration is one bound topic as the loader sees it.
//
// It is deliberately not generic. A loader holds bindings with different key
// and value types — (int, Template), (string, PlayerConfig), and so on — and Go
// offers no way to keep those in one slice: there is no wildcard, no
// covariance, and making the loader itself generic would mean one loader per
// type pair, each with its own warm-up and shutdown.
//
// Bind resolves that by capturing the type parameters in closures whose own
// signatures mention neither. K and V stay fully static inside the closure
// bodies; they move from the type into the captured environment, which is what
// lets one non-generic loader drive every binding.
type registration struct {
	name       string
	topic      string
	allowEmpty bool

	// apply folds one record into the store, reporting to the observer. The
	// only closure that knows K and V.
	apply func(rec *driver.Record)

	// lookup decodes a raw key with this binding's decoder and returns the
	// stored value as any. The one place a value loses its static type, and it
	// exists for debug tooling only.
	lookup func(rawKey string) (any, bool, error)

	// size reports the store's entry count.
	size func() int

	phase atomic.Int32

	upserts      atomic.Uint64
	deletes      atomic.Uint64
	filtered     atomic.Uint64
	decodeErrors atomic.Uint64

	lastRecordAtUnixNano atomic.Int64

	// warmupApplied counts records applied during warm-up. Written only by the
	// binding's own goroutine before warm-up completes, read afterwards, so the
	// atomic is for the reader's benefit.
	warmupApplied atomic.Int64
	warmupTookNs  atomic.Int64
}

// phase values, stored as an int32 so they can be read atomically from Stats
// while the binding's goroutine writes them.
const (
	phaseWarmup int32 = iota
	phaseSteady
	phaseStopped
)

func (r *registration) currentPhase() Phase {
	switch r.phase.Load() {
	case phaseSteady:
		return PhaseSteady
	case phaseStopped:
		return PhaseStopped
	default:
		return PhaseWarmup
	}
}

func (r *registration) inWarmup() bool {
	return r.phase.Load() == phaseWarmup
}

func (r *registration) stats() BindingStats {
	var lastRecord time.Time
	if ns := r.lastRecordAtUnixNano.Load(); ns != 0 {
		lastRecord = time.Unix(0, ns)
	}

	return BindingStats{
		Name:          r.name,
		Topic:         r.topic,
		Phase:         r.currentPhase(),
		Size:          r.size(),
		Upserts:       r.upserts.Load(),
		Deletes:       r.deletes.Load(),
		Filtered:      r.filtered.Load(),
		DecodeErrors:  r.decodeErrors.Load(),
		WarmupApplied: int(r.warmupApplied.Load()),
		WarmupTook:    time.Duration(r.warmupTookNs.Load()),
		LastRecordAt:  lastRecord,
	}
}

// applyFunc builds the closure that folds one record into the store.
//
// Generic so that K and V are static inside the body, while the closure it
// returns has a type mentioning neither — that erasure is what lets the
// non-generic registration hold it.
//
// It captures reg rather than any value read from it, so reg.inWarmup() is
// evaluated per record. That matters: had the phase been copied here, every
// record would be reported as warm-up for the life of the process.
func applyFunc[K comparable, V any](
	obs Observer,
	b Binding[K, V],
	store *Store[K, V],
	reg *registration,
) func(*driver.Record) {

	tombstone := b.Tombstone
	if tombstone == nil {
		tombstone = TombstoneOnBlankPayload
	}

	// The order below is part of the contract, not an implementation detail: a
	// tombstone resolves against the record's own key, and Filter sees the
	// final key rather than the one the record carried.
	return func(rec *driver.Record) {
		key, err := b.DecodeKey(rec.Key)
		if err != nil {
			reg.decodeErrors.Add(1)
			obs.OnDecodeError(b.Name, err, rec.Key)

			return
		}

		// A tombstone carries no payload, so the record's own key is the only
		// key available — KeyFromValue cannot help here even when it is set.
		if tombstone(rec.Payload) {
			store.Delete(key)
			reg.deletes.Add(1)
			reg.noteRecord()
			obs.OnDelete(b.Name, reg.inWarmup())

			return
		}

		value, err := b.DecodeValue(rec.Payload)
		if err != nil {
			reg.decodeErrors.Add(1)
			obs.OnDecodeError(b.Name, err, rec.Payload)

			return
		}

		if b.KeyFromValue != nil {
			derived := b.KeyFromValue(value)
			if b.VerifyKeyAgreement && derived != key {
				// Worth reporting rather than resolving: the entry goes under
				// the payload's key, while a future tombstone would carry the
				// record's, so it could never be deleted.
				obs.OnKeyMismatch(b.Name, fmt.Sprint(key), fmt.Sprint(derived))
			}
			key = derived
		}

		if b.Filter != nil && !b.Filter(key, value) {
			reg.filtered.Add(1)
			obs.OnFiltered(b.Name)

			return
		}

		store.Put(key, value)
		reg.upserts.Add(1)
		reg.noteRecord()
		obs.OnUpsert(b.Name, reg.inWarmup())
	}
}

// lookupFunc builds the closure that answers an untyped lookup, decoding the
// raw key with this binding's own decoder. The only place a stored value loses
// its static type, and it exists for debug tooling alone.
func lookupFunc[K comparable, V any](b Binding[K, V], store *Store[K, V]) func(string) (any, bool, error) {
	return func(rawKey string) (any, bool, error) {
		key, err := b.DecodeKey([]byte(rawKey))
		if err != nil {
			return nil, false, fmt.Errorf("decoding key %q for %s: %w", rawKey, b.Name, err)
		}
		value, ok := store.Get(key)

		return value, ok, nil
	}
}

// noteRecord records that a record was applied, for Stats and for warm-up
// progress.
func (r *registration) noteRecord() {
	r.lastRecordAtUnixNano.Store(time.Now().UnixNano())
	if r.inWarmup() {
		r.warmupApplied.Add(1)
	}
}
