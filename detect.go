package easykafkaconfig

import (
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
)

// DefaultWarmupPollTimeout is how long each poll waits while a topic is still
// being read. It only bounds how long the loop blocks with nothing to do —
// completion is decided by the detector, not by this timeout.
const DefaultWarmupPollTimeout = 500 * time.Millisecond

// Detection comes in two parts, because two different lifetimes are involved:
// a Detector is CONFIGURATION, created once and shared by every binding, while
// a detection is PER-BINDING STATE, started fresh for each one and mutated as
// its events arrive. A detector begins a detection; the detection observes.
//
// Collapsing them would mean the shared Detector holding that state, so ten
// bindings would have ten goroutines writing the same fields. The alternative —
// one Detector per binding — turns the option into a factory function, which is
// what Detector already is, only with worse ergonomics.

// Detector decides when a topic has been read to its end. CONFIGURATION: one
// value, shared by every binding, never mutated.
//
// Kafka gives a consumer no end-of-topic signal, so completion has to be
// inferred, and the available ways differ in how much they assume. This is a
// closed set: the interface's method is unexported, so only the constructors in
// this package implement it.
type Detector interface {
	// begin starts a detection over the partitions just assigned.
	//
	// Called once, after assignment and before the first poll — which is the
	// earliest it can be: the partition list does not exist until assignment,
	// and a detector may need to query the broker here, hence the error.
	begin(c driver.Consumer, partitions []int32) (detection, error)

	// pollTimeout is how long each poll waits while a topic is still being
	// read. It belongs here rather than on the detection because it is the same
	// for every binding and never changes: a timing-based detector can set its
	// own tempo without the loader knowing which detector it is running.
	pollTimeout() time.Duration
}

// detection is one binding's warm-up in progress: it folds polled events into a
// decision about whether that topic has been read whole.
// PER-BINDING STATE: one per binding, mutated by that binding's goroutine only.
type detection interface {
	// observe takes one event and reports whether warm-up is now complete.
	observe(ev driver.Event) bool
}

// PartitionEOF completes warm-up once every assigned partition has reported
// end-of-partition, meaning every record it held when the consumer started has
// been delivered.
//
// This is the default, and the only detector that makes no timing assumption:
// the broker states that a partition is exhausted rather than the consumer
// guessing from silence. It also finishes the instant the last partition
// catches up, and reports an empty topic in milliseconds rather than after a
// timeout.
//
// The one failure mode is a partition whose leader never answers, in which case
// no EOF arrives and warm-up would wait forever — bound it with
// WithWarmupTimeout.
func PartitionEOF() Detector {
	return eofDetector{}
}

// eofDetector is the configuration half: stateless, so one value serves every
// binding.
type eofDetector struct{}

func (eofDetector) pollTimeout() time.Duration {
	return DefaultWarmupPollTimeout
}

func (eofDetector) begin(_ driver.Consumer, partitions []int32) (detection, error) {
	remaining := make(map[int32]struct{}, len(partitions))
	for _, p := range partitions {
		remaining[p] = struct{}{}
	}

	return &eofDetection{remaining: remaining}, nil
}

// eofDetection is the state half: the partitions this binding is still waiting
// to hear from. Written only by the binding's own goroutine.
type eofDetection struct {
	remaining map[int32]struct{}
}

func (d *eofDetection) observe(ev driver.Event) bool {
	if eof, ok := ev.(driver.EOF); ok {
		delete(d.remaining, eof.Partition)
	}

	// An assignment with no partitions cannot happen — the driver rejects a
	// topic that reports none — but treating an empty set as complete keeps the
	// loop from hanging if that ever changes.
	return len(d.remaining) == 0
}
