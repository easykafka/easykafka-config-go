package driver

import (
	"sync"
	"time"
)

// throttle rate-limits a repeating log line, counting what it suppressed.
//
// An unreachable broker produces an error on every single poll — thousands per
// minute at a 100 ms timeout — so the alternative to throttling is a log full
// of one identical message. Sleeping after each error would also quieten the
// logs, but it would stall the poll loop and delay recovery, so the wait
// happens here instead: the loop keeps running at full speed and only the
// logging is rationed.
type throttle struct {
	every time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func newThrottle(every time.Duration) *throttle {
	return &throttle{every: every}
}

// allow reports whether the caller should log now. When it returns true, it
// also returns how many calls were suppressed since the previous true and
// resets that count. The first call always allows, so a change of state is
// never withheld.
func (t *throttle) allow() (bool, int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if !t.last.IsZero() && now.Sub(t.last) < t.every {
		t.suppressed++

		return false, 0
	}

	suppressed := t.suppressed
	t.suppressed = 0
	t.last = now

	return true, suppressed
}

// reset forgets the throttle window, so the next allow returns true. Called
// when the condition being throttled has cleared, to guarantee that its return
// is reported immediately rather than being swallowed by a window opened while
// it was failing.
func (t *throttle) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.last = time.Time{}
	t.suppressed = 0
}
