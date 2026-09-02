package unit

import (
	"strconv"
	"sync"
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type playerConfig struct {
	PlayerID string `json:"playerId"`
	Limit    int    `json:"limit"`
}

func TestStoreEmpty(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()

	assert.Equal(t, 0, s.Len())
	assert.False(t, s.Has("absent"))
	assert.Nil(t, s.GetOrNil("absent"))

	value, ok := s.Get("absent")
	assert.Nil(t, value)
	assert.False(t, ok)
}

func TestStorePutAndGet(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("p1", &playerConfig{PlayerID: "p1", Limit: 100})

	value, ok := s.Get("p1")
	require.True(t, ok)
	require.NotNil(t, value)
	assert.Equal(t, "p1", value.PlayerID)
	assert.Equal(t, 100, value.Limit)
	assert.True(t, s.Has("p1"))
	assert.Equal(t, 1, s.Len())
	assert.Equal(t, value, s.GetOrNil("p1"))
}

func TestStorePutReplacesWithoutGrowing(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("p1", &playerConfig{Limit: 1})
	s.Put("p1", &playerConfig{Limit: 2})
	s.Put("p1", &playerConfig{Limit: 3})

	assert.Equal(t, 1, s.Len(), "replacing a key must not change the size")
	assert.Equal(t, 3, s.GetOrNil("p1").Limit, "the newest value must win")
}

// A stored nil pointer is a present entry, not a delete — Delete is the only
// way to remove a key.
func TestStorePutNilIsPresent(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("p1", nil)

	value, ok := s.Get("p1")
	assert.True(t, ok, "a nil value is still a present entry")
	assert.Nil(t, value)
	assert.True(t, s.Has("p1"))
	assert.Equal(t, 1, s.Len())
}

func TestStoreDelete(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("p1", &playerConfig{})
	s.Put("p2", &playerConfig{})
	require.Equal(t, 2, s.Len())

	s.Delete("p1")

	assert.Equal(t, 1, s.Len())
	assert.False(t, s.Has("p1"))
	assert.True(t, s.Has("p2"))
}

func TestStoreDeleteAbsentKeyIsNoop(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("p1", &playerConfig{})

	s.Delete("absent")
	s.Delete("absent")
	s.Delete("p1")
	s.Delete("p1")

	assert.Equal(t, 0, s.Len(), "size must not drift below zero on repeated deletes")
}

func TestStoreIntKeys(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[int, playerConfig]()
	s.Put(42, &playerConfig{Limit: 7})

	assert.Equal(t, 7, s.GetOrNil(42).Limit)
	assert.Nil(t, s.GetOrNil(43))
}

func TestStoreAll(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	for i := range 5 {
		key := "p" + strconv.Itoa(i)
		s.Put(key, &playerConfig{PlayerID: key, Limit: i})
	}

	seen := map[string]int{}
	for k, v := range s.All() {
		require.NotNil(t, v)
		seen[k] = v.Limit
	}

	assert.Len(t, seen, 5)
	assert.Equal(t, map[string]int{"p0": 0, "p1": 1, "p2": 2, "p3": 3, "p4": 4}, seen)
}

// Breaking out of the loop must stop the underlying walk, which is the reason
// All exists alongside Snapshot.
func TestStoreAllStopsOnBreak(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	for i := range 100 {
		s.Put("p"+strconv.Itoa(i), &playerConfig{Limit: i})
	}

	visited := 0
	for range s.All() {
		visited++
		if visited == 3 {
			break
		}
	}

	assert.Equal(t, 3, visited, "iteration must stop at the break, not walk all 100 entries")
}

func TestStoreKeys(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	s.Put("a", &playerConfig{})
	s.Put("b", &playerConfig{})

	seen := map[string]bool{}
	for k := range s.Keys() {
		seen[k] = true
	}

	assert.Equal(t, map[string]bool{"a": true, "b": true}, seen)
}

func TestStoreKeysStopsOnBreak(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[int, playerConfig]()
	for i := range 50 {
		s.Put(i, &playerConfig{})
	}

	visited := 0
	for range s.Keys() {
		visited++
		break
	}

	assert.Equal(t, 1, visited)
}

func TestStoreSnapshotIsIndependentButShallow(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()
	original := &playerConfig{PlayerID: "p1", Limit: 1}
	s.Put("p1", original)

	snap := s.Snapshot()
	require.Len(t, snap, 1)
	assert.Same(t, original, snap["p1"], "the copy is shallow: pointers are shared with the store")

	// Restructuring the returned map must not touch the store. Note this is the
	// builtin delete acting on the caller's own map, nothing to do with
	// Store.Delete. Mutating a *value* through a shared pointer
	// (snap["p1"].Limit = 999) would be a different matter — that is visible in
	// the store, which is what "shallow" means above.
	delete(snap, "p1")
	snap["injected"] = &playerConfig{}

	assert.Equal(t, 1, s.Len())
	assert.True(t, s.Has("p1"))
	assert.False(t, s.Has("injected"))
}

func TestStoreSnapshotEmpty(t *testing.T) {
	t.Parallel()

	s := ekconfig.NewStore[string, playerConfig]()

	assert.Empty(t, s.Snapshot())
}

// The production shape: one writer goroutine, many concurrent readers. Run
// under -race, this is what guards the Len counter and the read path.
//
// The readers all hammer one key, records/2, which is deliberate on three
// counts and not obvious:
//
//  1. It starts absent and becomes present part-way through the run. The writer
//     fills keys 0..records-1 in order, so a key at the midpoint is guaranteed
//     to be missing early and present later whatever the scheduler does — both
//     the miss and the hit path get exercised without the reader having to
//     coordinate with the writer. Key 0 would be present almost immediately and
//     records-1 only at the very end, leaving one path barely covered.
//  2. The value is self-verifying. The writer stores Limit == i under key i, so
//     asserting Limit == records/2 is what would catch a torn read or a value
//     belonging to a different key. The nil guard is what makes the assertion
//     valid: it only fires once the key exists.
//  3. A single hot key helps the race detector, which only reports when two
//     goroutines touch the same memory. Concentrating the readers maximises the
//     chance one of them is reading the very entry the writer is publishing.
//
// The trade-off is breadth: this says little about the rest of sync.Map's
// machinery. Len and Has are called only to add concurrent traffic; their
// results are intentionally discarded.
func TestStoreConcurrentReadersSingleWriter(t *testing.T) {
	t.Parallel()

	const (
		records = 500
		readers = 8
	)

	s := ekconfig.NewStore[int, playerConfig]()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					// Reads must never panic, tear, or observe a partial value.
					//   torn:    half of one write mixed with half of another —
					//            the any in sync.Map is two words (type + data),
					//            so a mismatched pair can crash the assertion.
					//   partial: the pointer is visible but the field writes
					//            behind it are not, e.g. Limit == 0 for a value
					//            written as 250.
					// sync.Map rules both out (atomic publish, happens-before on
					// Load); the assertion pins that guarantee against a future
					// change to a plain map, a missing lock, or publishing a
					// value before it is fully initialised.
					if v := s.GetOrNil(records / 2); v != nil {
						assert.Equal(t, records/2, v.Limit)
					}
					_ = s.Len()
					_ = s.Has(1)
				}
			}
		})
	}

	// Single writer, as the loader guarantees per binding.
	for i := range records {
		s.Put(i, &playerConfig{PlayerID: strconv.Itoa(i), Limit: i})
	}
	close(stop)
	wg.Wait()

	assert.Equal(t, records, s.Len())
	assert.Equal(t, records-1, s.GetOrNil(records-1).Limit)
}

// Concurrent writers are not the production shape, but the size counter must
// still be exact: Swap and LoadAndDelete report authoritatively whether an
// entry existed.
func TestStoreConcurrentWritersKeepSizeExact(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		keys    = 100
	)

	s := ekconfig.NewStore[int, playerConfig]()

	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for k := range keys {
				s.Put(k, &playerConfig{Limit: k})
			}
		})
	}
	wg.Wait()

	assert.Equal(t, keys, s.Len(), "every writer stored the same keys, so size must be the key count")

	for range writers {
		wg.Go(func() {
			for k := range keys {
				s.Delete(k)
			}
		})
	}
	wg.Wait()

	assert.Equal(t, 0, s.Len())
}
