package unit

import (
	"context"
	"sync"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
)

// fakeConsumer is a driver.Consumer that replays a scripted sequence of events,
// so the loader can be exercised with no Kafka and no Docker.
//
// The script is consumed one event per Poll. Once it runs out, Poll returns
// Idle forever, which is what a real consumer does on a quiet topic — so a
// loader that has finished warming up keeps polling harmlessly rather than
// seeing the fake end.
type fakeConsumer struct {
	partitions []int32

	// assignErr, when set, fails assignment instead of returning partitions.
	assignErr error

	// blockUntilCancelled makes AssignAll wait for the context, standing in for
	// a broker that never answers.
	blockUntilCancelled bool

	mu       sync.Mutex
	script   []driver.Event
	polls    int
	closed   bool
	assigned bool

	// nextOffset is the offset each partition would write next, which is what a
	// partition's high watermark means. Tracked from the records actually
	// delivered so a scripted EOF can carry a realistic position.
	nextOffset map[int32]int64

	// live receives events pushed after the script is exhausted, which is how a
	// test delivers a change once the loader is serving.
	live chan driver.Event
}

func newFakeConsumer(partitions []int32, script ...driver.Event) *fakeConsumer {
	return &fakeConsumer{
		partitions: partitions,
		script:     script,
		nextOffset: make(map[int32]int64),
		live:       make(chan driver.Event, 64),
	}
}

// record builds a record event for the fake's topic.
func record(partition int32, offset int64, key, payload string) *driver.Record {
	return &driver.Record{
		Topic:     "fake",
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Payload:   []byte(payload),
		Timestamp: time.Now(),
	}
}

// tombstone builds a record with no payload, which the loader turns into a
// delete.
func tombstone(partition int32, offset int64, key string) *driver.Record {
	rec := record(partition, offset, key, "")
	rec.Payload = nil

	return rec
}

// eofAll returns one EOF per partition, which is what completes warm-up under
// the default detector.
//
// Offset is left unset here and filled in when the event is delivered, from the
// records that actually preceded it — see deliver. Scripting it by hand would
// mean restating every record's offset at the call site and keeping the two in
// step.
func eofAll(partitions ...int32) []driver.Event {
	out := make([]driver.Event, 0, len(partitions))
	for _, p := range partitions {
		out = append(out, driver.EOF{Partition: p})
	}

	return out
}

// script concatenates event groups, for readability at call sites.
func script(groups ...[]driver.Event) []driver.Event {
	var out []driver.Event
	for _, g := range groups {
		out = append(out, g...)
	}

	return out
}

// events wraps a variadic list as a group.
func events(evs ...driver.Event) []driver.Event {
	return evs
}

func (f *fakeConsumer) AssignAll(ctx context.Context) ([]int32, error) {
	if f.blockUntilCancelled {
		<-ctx.Done()

		return nil, ctx.Err()
	}
	if f.assignErr != nil {
		return nil, f.assignErr
	}

	f.mu.Lock()
	f.assigned = true
	f.mu.Unlock()

	return f.partitions, nil
}

func (f *fakeConsumer) Poll(timeout time.Duration) driver.Event {
	f.mu.Lock()
	f.polls++
	if len(f.script) > 0 {
		ev := f.script[0]
		f.script = f.script[1:]
		f.mu.Unlock()

		return f.deliver(ev)
	}
	f.mu.Unlock()

	// Script exhausted: behave like a quiet topic, but stay responsive to
	// events a test pushes in while the loader is serving.
	select {
	case ev := <-f.live:
		return f.deliver(ev)
	case <-time.After(timeout):
		return driver.Idle{}
	}
}

// deliver is the last step before an event leaves the fake, whether it came
// from the script or was pushed in afterwards. It exists so a scripted EOF
// carries the position a real one would.
//
// A real PartitionEOF reports the offset the consumer reached, which equals the
// partition's high watermark at that moment — one past its last record. Nothing
// in the library reads EOF.Offset today, only EOF.Partition, so a zero would go
// unnoticed; a watermark-based detector would read it, and would then be tested
// against a value no broker produces.
func (f *fakeConsumer) deliver(ev driver.Event) driver.Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch e := ev.(type) {
	case *driver.Record:
		if f.nextOffset == nil {
			f.nextOffset = make(map[int32]int64)
		}
		if next := e.Offset + 1; next > f.nextOffset[e.Partition] {
			f.nextOffset[e.Partition] = next
		}

	case driver.EOF:
		// Filled in only when unset, so a test that wants a specific position
		// can still say so. An untouched partition stays at 0, which is exactly
		// an empty partition's watermark.
		if e.Offset == 0 {
			e.Offset = f.nextOffset[e.Partition]

			return e
		}
	}

	return ev
}

// push delivers an event to a serving loader.
func (f *fakeConsumer) push(ev driver.Event) {
	f.live <- ev
}

func (f *fakeConsumer) Watermarks(int32) (int64, int64, error) {
	return 0, 0, nil
}

func (f *fakeConsumer) Positions() (map[int32]int64, error) {
	return map[int32]int64{}, nil
}

func (f *fakeConsumer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true

	return nil
}

func (f *fakeConsumer) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closed
}

// Compile-time assertion that the fake satisfies the interface the loader
// depends on. If the driver's contract changes, this fails here rather than
// leaving the loader untested against the real shape.
var _ driver.Consumer = (*fakeConsumer)(nil)
