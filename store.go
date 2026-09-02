package easykafkaconfig

import (
	"iter"
	"maps"
	"sync"
	"sync/atomic"
)

// Store is a concurrent map from configuration key to configuration value.
//
// It is safe for unlimited concurrent readers while a loader goroutine writes.
// The read path takes no lock the caller can see and performs no type
// assertions: K and V are fixed when the Store is created.
//
// Values are held as *V. The library never mutates a stored value — an update
// replaces the pointer with a freshly decoded one — so a reader holding a *V
// sees a stable snapshot of that key. Callers must treat the returned pointer as
// read-only; mutating it would be visible to every other reader.
//
// The zero Store is ready to use, but prefer NewStore for clarity.
type Store[K comparable, V any] struct {
	m sync.Map

	// size tracks the number of live entries so Len is O(1) rather than a full
	// Range walk. It stays exact even under concurrent writers because Swap and
	// LoadAndDelete report authoritatively whether an entry already existed.
	size atomic.Int64
}

// NewStore returns an empty Store.
func NewStore[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{}
}

// Get returns the value stored under key and whether it was present.
func (s *Store[K, V]) Get(key K) (*V, bool) {
	v, ok := s.m.Load(key)
	if !ok {
		return nil, false
	}

	return v.(*V), true
}

// GetOrNil returns the value stored under key, or nil if the key is absent.
//
// It is the convenient form for lookups that treat "no configuration" and "nil
// configuration" alike. Use Get when the difference matters.
func (s *Store[K, V]) GetOrNil(key K) *V {
	value, _ := s.Get(key)

	return value
}

// Has reports whether key is present, without retrieving the value.
func (s *Store[K, V]) Has(key K) bool {
	_, ok := s.m.Load(key)

	return ok
}

// Len returns the number of entries. It is O(1).
func (s *Store[K, V]) Len() int {
	return int(s.size.Load())
}

// All returns an iterator over every key/value pair, in unspecified order.
//
// Nothing is copied: pairs are yielded straight from the underlying map, and
// stopping the loop early (break, return) stops the walk immediately. Prefer it
// over Snapshot for large stores.
//
// Like any walk of a concurrent map, All is not a point-in-time view: an entry
// written or deleted while the iteration is in progress may or may not be
// observed.
func (s *Store[K, V]) All() iter.Seq2[K, *V] {
	return func(yield func(K, *V) bool) {
		// Returning yield's result is the stop check, not a shortcut: yield
		// reports false once the consumer is done (break, return), and calling
		// it again then panics with "range function continued iteration after
		// function for loop body returned false".
		//
		// It can be propagated directly only because Range and yield share the
		// convention that true means "keep going". A callback with inverted or
		// error-based semantics would need an explicit if !yield(...) { return }.
		s.m.Range(func(k, v any) bool {
			return yield(k.(K), v.(*V))
		})
	}
}

// Keys returns an iterator over every key, in unspecified order. The caveats on
// All apply here too.
func (s *Store[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		s.m.Range(func(k, _ any) bool {
			return yield(k.(K))
		})
	}
}

// Snapshot copies the store into a plain map.
//
// The map is the caller's own — adding or removing entries does not affect the
// store — but the copy is shallow: the *V pointers are shared with the store, so
// the read-only rule on values still applies.
//
// It is not an atomic view. Like All, it observes an arbitrary subset of the
// writes that happen during the copy, so it means "every key that existed
// throughout, plus some of those that changed". It also allocates the whole map,
// which on a large store doubles the memory held; intended for tests and debug
// dumps rather than the hot path.
func (s *Store[K, V]) Snapshot() map[K]*V {
	return maps.Collect(s.All())
}

// Put stores value under key, replacing any previous value.
//
// The pointer is stored as given: Put(key, nil) records a present, nil value —
// it is not a delete. Use Delete to remove an entry.
//
// Exported so tests can seed a store directly; in production the loader owns the
// write path.
func (s *Store[K, V]) Put(key K, value *V) {
	if _, loaded := s.m.Swap(key, value); !loaded {
		s.size.Add(1)
	}
}

// Delete removes key. Deleting an absent key is a no-op.
//
// Exported so tests can manipulate a store directly; in production the loader
// owns the write path and calls this for tombstone records.
func (s *Store[K, V]) Delete(key K) {
	if _, loaded := s.m.LoadAndDelete(key); loaded {
		s.size.Add(-1)
	}
}
