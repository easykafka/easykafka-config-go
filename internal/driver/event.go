package driver

import "time"

// Event is the result of one Poll. Exactly one of the concrete types below is
// returned, and never nil, so callers switch on the type rather than checking
// for nil:
//
//	switch e := c.Poll(timeout).(type) {
//	case *Record: ...
//	case EOF:     ...
//	case Idle:    ...
//	case Failure: ...
//	}
//
// This is a closed set — the sealing method is unexported, so no other package
// can add a case a caller's switch would miss.
type Event interface {
	sealedEvent()
}

// Record is one message read from the topic.
//
// Key and Payload alias the driver's own buffers and are only valid until the
// next Poll on the same consumer. A caller that keeps either beyond that must
// copy it. In this library the record is decoded during the same iteration, so
// nothing is retained.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Payload   []byte
	Timestamp time.Time
}

func (*Record) sealedEvent() {}

// EOF reports that the consumer has reached the end of one partition: every
// record that partition held as of now has been delivered. Offset is the
// position reached, which equals the partition's high watermark at that moment.
//
// It is not a terminal event. More records may arrive afterwards, and EOF fires
// again each time the consumer catches up, so a caller that has finished
// warming up simply ignores it.
type EOF struct {
	Partition int32
	Offset    int64
}

func (EOF) sealedEvent() {}

// Idle reports that the poll timeout elapsed with nothing to deliver. It says
// nothing about whether the topic has been fully read — a slow broker and a
// drained partition look identical from here, which is why EOF exists.
type Idle struct{}

func (Idle) sealedEvent() {}

// Failure reports a Kafka-level error.
//
// Fatal distinguishes the two cases that matter. A fatal error means this
// consumer is unusable and must be closed; the caller decides what that means
// for the process. A non-fatal error is informational: librdkafka recovers on
// its own, most commonly a lost broker connection it is already reconnecting
// to, and polling should continue.
type Failure struct {
	Err   error
	Fatal bool
}

func (Failure) sealedEvent() {}

// Error lets a Failure be used as an error value directly.
func (f Failure) Error() string {
	if f.Err == nil {
		return "kafka failure"
	}

	return f.Err.Error()
}

// Unwrap exposes the underlying driver error to errors.Is and errors.As.
func (f Failure) Unwrap() error {
	return f.Err
}
