package unit

import (
	"testing"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fake must report the offset a real PartitionEOF would: the position the
// consumer reached, which equals the partition's high watermark at that moment,
// one past its last record.
//
// This is asserted here because nothing else would notice if it broke. The
// driver fills driver.EOF.Offset in from the broker's event, but no code reads
// it back — the detector looks only at EOF.Partition — so the fake could hand
// out zeros and every other test would still pass, until a watermark-based
// detector arrived and was tested against a value no broker produces.
func TestFakeConsumerReportsRealisticEOFOffsets(t *testing.T) {
	t.Parallel()

	// Partition 0 gets three records, partition 1 gets one, partition 2 none.
	scripted := script(
		events(
			record(0, 0, "a", `{"playerId":"a"}`),
			record(0, 1, "b", `{"playerId":"b"}`),
			record(0, 2, "c", `{"playerId":"c"}`),
			record(1, 0, "d", `{"playerId":"d"}`),
		),
		eofAll(0, 1, 2),
	)

	fake := newFakeConsumer([]int32{0, 1, 2}, scripted...)

	watermarks := map[int32]int64{}
	for range scripted {
		if eof, ok := fake.Poll(time.Millisecond).(driver.EOF); ok {
			watermarks[eof.Partition] = eof.Offset
		}
	}

	require.Len(t, watermarks, 3, "one EOF per assigned partition")
	assert.Equal(t, int64(3), watermarks[0], "records at 0, 1, 2 leave the watermark at 3")
	assert.Equal(t, int64(1), watermarks[1], "one record at 0 leaves it at 1")
	assert.Equal(t, int64(0), watermarks[2], "no records leaves 0, which is an empty partition's watermark")
}

// An offset scripted explicitly is honoured rather than overwritten, so a test
// that needs a particular position can still set one.
func TestFakeConsumerKeepsAnExplicitEOFOffset(t *testing.T) {
	t.Parallel()

	scripted := events(
		record(0, 0, "a", `{"playerId":"a"}`),
		driver.EOF{Partition: 0, Offset: 99},
	)

	fake := newFakeConsumer([]int32{0}, scripted...)

	var got driver.EOF
	for range scripted {
		if eof, ok := fake.Poll(time.Millisecond).(driver.EOF); ok {
			got = eof
		}
	}

	assert.Equal(t, int64(99), got.Offset, "an explicit offset must survive delivery")
}
