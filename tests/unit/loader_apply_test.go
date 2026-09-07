package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingObserver captures every callback so a test can assert on what the
// loader reported, not just on what ended up in the store.
type recordingObserver struct {
	ekconfig.NopObserver

	mu            sync.Mutex
	upserts       []string
	warmupUpserts int
	deletes       []string
	filtered      []string
	decodeErrors  []string
	keyMismatches [][2]string
	phases        []ekconfig.Phase
	kafkaErrors   []error
}

func (o *recordingObserver) OnUpsert(name string, warmup bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.upserts = append(o.upserts, name)
	if warmup {
		o.warmupUpserts++
	}
}

func (o *recordingObserver) OnDelete(name string, _ bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deletes = append(o.deletes, name)
}

func (o *recordingObserver) OnFiltered(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.filtered = append(o.filtered, name)
}

func (o *recordingObserver) OnDecodeError(name string, err error, _ []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.decodeErrors = append(o.decodeErrors, name+": "+err.Error())
}

func (o *recordingObserver) OnKeyMismatch(_ string, fromKey, fromValue string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.keyMismatches = append(o.keyMismatches, [2]string{fromKey, fromValue})
}

func (o *recordingObserver) OnPhase(_ string, phase ekconfig.Phase, _ int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.phases = append(o.phases, phase)
}

func (o *recordingObserver) OnKafkaError(_ string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.kafkaErrors = append(o.kafkaErrors, err)
}

func (o *recordingObserver) snapshot() recordingObserver {
	o.mu.Lock()
	defer o.mu.Unlock()

	return recordingObserver{
		upserts:       append([]string(nil), o.upserts...),
		warmupUpserts: o.warmupUpserts,
		deletes:       append([]string(nil), o.deletes...),
		filtered:      append([]string(nil), o.filtered...),
		decodeErrors:  append([]string(nil), o.decodeErrors...),
		keyMismatches: append([][2]string(nil), o.keyMismatches...),
		phases:        append([]ekconfig.Phase(nil), o.phases...),
		kafkaErrors:   append([]error(nil), o.kafkaErrors...),
	}
}

// A record the Filter rejects is read and decoded, then deliberately not
// stored — and reported as filtered rather than as an error.
func TestLoaderFilterRejectsRecords(t *testing.T) {
	t.Parallel()

	fake := newFakeConsumer([]int32{0}, script(
		events(
			record(0, 0, "keep", `{"playerId":"keep","limit":5}`),
			record(0, 1, "drop", `{"playerId":"drop","limit":0}`),
		),
		eofAll(0),
	)...)

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake}, ekconfig.WithObserver(obs))

	binding := playerBinding("PlayerConfig", "players")
	binding.Filter = func(_ string, v *playerConfig) bool { return v.Limit > 0 }
	players := loader.Bind(binding)

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	assert.Equal(t, 1, players.Len())
	assert.True(t, players.Has("keep"))
	assert.False(t, players.Has("drop"))

	got := obs.snapshot()
	assert.Equal(t, []string{"PlayerConfig"}, got.filtered)
	assert.Len(t, got.upserts, 1)
}

// Bad data is skipped and reported. It is never retried — redelivering a record
// that cannot be decoded would fail identically — and never fatal, because one
// poison record must not stop a configuration topic.
func TestLoaderSkipsUndecodableRecords(t *testing.T) {
	t.Parallel()

	fake := newFakeConsumer([]int32{0}, script(
		events(
			record(0, 0, "good", `{"playerId":"good","limit":1}`),
			record(0, 1, "bad", `{"playerId":`),               // truncated JSON
			record(0, 2, "worse", `{"limit":"not a number"}`), // wrong field type
			record(0, 3, "also-good", `{"playerId":"also-good","limit":2}`),
		),
		eofAll(0),
	)...)

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake}, ekconfig.WithObserver(obs))
	players := loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()), "bad data must not fail warm-up")
	waitForLoaderOnTestCleanup(t, loader)

	assert.Equal(t, 2, players.Len(), "the two decodable records must be stored")
	assert.True(t, players.Has("good"))
	assert.True(t, players.Has("also-good"))
	assert.False(t, players.Has("bad"))

	got := obs.snapshot()
	assert.Len(t, got.decodeErrors, 2)

	stats := loader.Stats()
	require.Len(t, stats, 1)
	assert.Equal(t, uint64(2), stats[0].DecodeErrors)
	assert.Equal(t, uint64(2), stats[0].Upserts)
}

// An undecodable *key* is skipped too, and must not be substituted with a
// sentinel: storing or deleting under a key no producer wrote would corrupt the
// store silently.
func TestLoaderSkipsUndecodableKeys(t *testing.T) {
	t.Parallel()

	// The second record's key cannot be decoded. Its payload is deliberately
	// well-formed and unremarkable: DecodeKey runs first and the record is
	// dropped there, so the payload is never decoded at all.
	scripted := script(
		events(
			record(0, 0, "42", `{"id":42}`),
			record(0, 1, "not-a-number", `{"id":7}`),
		),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	type template struct {
		ID int `json:"id"`
	}

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"templates": fake}, ekconfig.WithObserver(obs))
	templates := loader.Bind(ekconfig.Binding[int, template]{
		Name:        "Template",
		Topic:       "templates",
		DecodeKey:   ekconfig.IntKey,
		DecodeValue: ekconfig.JSONValue[template],
	})

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	assert.Equal(t, 1, templates.Len())
	assert.True(t, templates.Has(42))

	// Zero is the zero value of the key type, which is what IntKey returns
	// alongside its error. Swallowing that error would file the record under 0
	// — a key no producer ever wrote — so the store must not hold one.
	assert.False(t, templates.Has(0), "a key that failed to decode must not land under the zero value")
	assert.Len(t, obs.snapshot().decodeErrors, 1)
}

// KeyFromValue makes the payload authoritative for upserts. VerifyKeyAgreement
// then reports a producer that disagrees with itself — the entry goes under the
// payload's key while a tombstone would carry the record's, so it could never
// be deleted.
func TestLoaderKeyFromValueAndMismatchReporting(t *testing.T) {
	t.Parallel()

	fake := newFakeConsumer([]int32{0}, script(
		events(
			record(0, 0, "agrees", `{"playerId":"agrees"}`),
			record(0, 1, "record-key", `{"playerId":"payload-key"}`),
		),
		eofAll(0),
	)...)

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake}, ekconfig.WithObserver(obs))

	binding := playerBinding("PlayerConfig", "players")
	binding.KeyFromValue = func(v *playerConfig) string { return v.PlayerID }
	binding.VerifyKeyAgreement = true
	players := loader.Bind(binding)

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	// Stored under the payload's key, not the record's.
	assert.True(t, players.Has("payload-key"))
	assert.False(t, players.Has("record-key"))

	got := obs.snapshot()
	require.Len(t, got.keyMismatches, 1, "only the disagreeing record must be reported")
	assert.Equal(t, [2]string{"record-key", "payload-key"}, got.keyMismatches[0])
}

// A tombstone always uses the record's own key, even when KeyFromValue is set:
// there is no payload to derive a key from. VerifyKeyAgreement is on to pin the
// consequence — the agreement check must stay out of the tombstone path.
func TestLoaderTombstoneUsesRecordKeyDespiteKeyFromValue(t *testing.T) {
	t.Parallel()

	// The upsert agrees with its own payload, so the only record that could
	// produce a mismatch report is the tombstone.
	scripted := script(
		events(
			record(0, 0, "p1", `{"playerId":"p1"}`),
			tombstone(0, 1, "p1"),
		),
		eofAll(0),
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake}, ekconfig.WithObserver(obs))

	binding := playerBinding("PlayerConfig", "players")
	binding.KeyFromValue = func(v *playerConfig) string { return v.PlayerID }
	binding.VerifyKeyAgreement = true
	players := loader.Bind(binding)

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	assert.Equal(t, 0, players.Len(), "the tombstone must delete the key the record carried")

	// Deriving a key from a tombstone would mean deriving it from no payload,
	// giving the zero value and so a mismatch against every delete. A service
	// wiring OnKeyMismatch to an alert would then be paged for each one.
	assert.Empty(t, obs.snapshot().keyMismatches,
		"a tombstone has no payload to derive a key from, so it must not be checked for agreement")
}

// The observer distinguishes the initial read from later changes, which is what
// lets a metric separate startup load from ongoing churn.
func TestLoaderObserverDistinguishesWarmupFromSteady(t *testing.T) {
	t.Parallel()

	fake := newFakeConsumer([]int32{0}, script(
		events(record(0, 0, "p1", `{"playerId":"p1"}`)),
		eofAll(0),
	)...)

	obs := &recordingObserver{}
	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake},
		ekconfig.WithObserver(obs), ekconfig.WithSteadyPollTimeout(10*time.Millisecond))
	loader.Bind(playerBinding("PlayerConfig", "players"))

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	require.Equal(t, 1, obs.snapshot().warmupUpserts)

	fake.push(record(0, 1, "p2", `{"playerId":"p2"}`))
	require.Eventually(t, func() bool {
		return len(obs.snapshot().upserts) == 2
	}, 2*time.Second, 10*time.Millisecond)

	got := obs.snapshot()
	assert.Equal(t, 1, got.warmupUpserts, "the live change must not count as warm-up")
	assert.Equal(t, []ekconfig.Phase{ekconfig.PhaseSteady}, got.phases)
}

func TestLoaderStats(t *testing.T) {
	t.Parallel()

	fake := newFakeConsumer([]int32{0}, script(
		events(
			record(0, 0, "p1", `{"playerId":"p1"}`),
			record(0, 1, "p2", `{"playerId":"p2"}`),
			tombstone(0, 2, "p2"),
		),
		eofAll(0),
	)...)

	loader := loaderWith(t, map[string]*fakeConsumer{"players": fake})
	loader.Bind(playerBinding("PlayerConfig", "players"))

	// Before Start, a binding reports itself as warming up and empty.
	before := loader.Stats()
	require.Len(t, before, 1)
	assert.Equal(t, ekconfig.PhaseWarmup, before[0].Phase)
	assert.Equal(t, 0, before[0].Size)

	// A context this test can cancel: the loader has no stop of its own, so
	// cancelling what Start was given is the only way to stop it.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, loader.Start(ctx))

	got := loader.Stats()
	require.Len(t, got, 1)
	assert.Equal(t, "PlayerConfig", got[0].Name)
	assert.Equal(t, "players", got[0].Topic)
	assert.Equal(t, ekconfig.PhaseSteady, got[0].Phase)
	assert.Equal(t, 1, got[0].Size, "two upserts and one delete leave one entry")
	assert.Equal(t, uint64(2), got[0].Upserts)
	assert.Equal(t, uint64(1), got[0].Deletes)
	assert.Equal(t, 3, got[0].WarmupApplied)
	assert.Positive(t, got[0].WarmupTook)
	assert.False(t, got[0].LastRecordAt.IsZero())

	// Stopping is the caller's: cancel the context Start was given, then wait.
	cancel()
	require.NoError(t, loader.WaitUntilStopped(), "a clean stop leaves no error")

	assert.Equal(t, ekconfig.PhaseStopped, loader.Stats()[0].Phase,
		"WaitUntilStopped returning means every binding has already stopped")
}

// LookupRaw serves any binding through one call, so a debug endpoint needs no
// switch over configuration types. It is the only place a value loses its
// static type.
func TestLoaderLookupRaw(t *testing.T) {
	t.Parallel()

	players := newFakeConsumer([]int32{0}, script(
		events(record(0, 0, "p1", `{"playerId":"p1","limit":3}`)),
		eofAll(0),
	)...)
	templates := newFakeConsumer([]int32{0}, script(
		events(record(0, 0, "42", `{"id":42}`)),
		eofAll(0),
	)...)

	type template struct {
		ID int `json:"id"`
	}

	loader := loaderWith(t, map[string]*fakeConsumer{"players": players, "templates": templates})
	loader.Bind(playerBinding("PlayerConfig", "players"))
	loader.Bind(ekconfig.Binding[int, template]{
		Name:        "Template",
		Topic:       "templates",
		DecodeKey:   ekconfig.IntKey,
		DecodeValue: ekconfig.JSONValue[template],
	})

	require.NoError(t, loader.Start(t.Context()))
	waitForLoaderOnTestCleanup(t, loader)

	// A string-keyed binding.
	value, ok, err := loader.LookupRaw("PlayerConfig", "p1")
	require.NoError(t, err)
	require.True(t, ok)
	cfg, isPlayer := value.(*playerConfig)
	require.True(t, isPlayer, "got %T", value)
	assert.Equal(t, 3, cfg.Limit)

	// An int-keyed binding, through the same call: each binding decodes the raw
	// key with its own decoder.
	value, ok, err = loader.LookupRaw("Template", "42")
	require.NoError(t, err)
	require.True(t, ok)
	tmpl, isTemplate := value.(*template)
	require.True(t, isTemplate, "got %T", value)
	assert.Equal(t, 42, tmpl.ID)

	// Absent key.
	_, ok, err = loader.LookupRaw("PlayerConfig", "nobody")
	require.NoError(t, err)
	assert.False(t, ok)

	// A key the binding's decoder rejects is an error, not a miss.
	_, _, err = loader.LookupRaw("Template", "not-a-number")
	require.Error(t, err)

	// An unknown binding name.
	_, _, err = loader.LookupRaw("NoSuchBinding", "x")
	require.ErrorIs(t, err, ekconfig.ErrUnknownBinding)
}
