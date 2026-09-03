package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validDriverConfig() driver.Config {
	return driver.Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "config.compact",
		GroupID: "config-reader",
	}
}

// New validates before touching the network, so a misconfigured consumer fails
// at construction rather than on first poll.
func TestDriverNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate  func(*driver.Config)
		wantMsg string
	}{
		"no brokers": {
			mutate:  func(c *driver.Config) { c.Brokers = nil },
			wantMsg: "at least one broker is required",
		},
		"empty broker": {
			mutate:  func(c *driver.Config) { c.Brokers = []string{" "} },
			wantMsg: "broker address cannot be empty",
		},
		"no topic": {
			mutate:  func(c *driver.Config) { c.Topic = "" },
			wantMsg: "topic is required",
		},
		"blank topic": {
			mutate:  func(c *driver.Config) { c.Topic = "\t" },
			wantMsg: "topic is required",
		},
		"no group id": {
			mutate:  func(c *driver.Config) { c.GroupID = "" },
			wantMsg: "group id is required",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := validDriverConfig()
			tc.mutate(&cfg)

			c, err := driver.New(cfg)
			require.Error(t, err)
			assert.Nil(t, c)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// Every problem is reported at once, so a misconfigured consumer takes one
// round trip to fix.
func TestDriverNewReportsAllConfigProblems(t *testing.T) {
	t.Parallel()

	_, err := driver.New(driver.Config{})
	require.Error(t, err)

	for _, want := range []string{
		"at least one broker is required",
		"topic is required",
		"group id is required",
	} {
		assert.Contains(t, err.Error(), want)
	}
}

// The properties that encode this library's semantics cannot be overridden from
// outside. Silently accepting an override of, say, enable.auto.commit would
// break the restart-rereads-everything guarantee with no visible symptom until
// a pod restarted.
func TestDriverNewRejectsReservedKafkaProperties(t *testing.T) {
	t.Parallel()

	reserved := []string{
		"bootstrap.servers",
		"group.id",
		"auto.offset.reset",
		"enable.auto.commit",
		"enable.auto.offset.store",
		"enable.partition.eof",
	}

	for _, key := range reserved {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			cfg := validDriverConfig()
			cfg.Extra = map[string]any{key: "whatever"}

			_, err := driver.New(cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), key)
			assert.Contains(t, err.Error(), "managed by this library")
		})
	}
}

// A property the library does not depend on passes through, so callers can tune
// fetch sizes and the like.
func TestDriverNewAcceptsUnreservedExtraProperties(t *testing.T) {
	t.Parallel()

	cfg := validDriverConfig()
	cfg.Extra = map[string]any{
		"fetch.min.bytes":         1024,
		"socket.keepalive.enable": true,
	}

	c, err := driver.New(cfg)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NoError(t, c.Close())
}

// Construction must not contact the broker: the address below is unroutable, so
// a successful New proves the first network call is deferred to AssignAll.
func TestDriverNewDoesNotConnect(t *testing.T) {
	t.Parallel()

	cfg := validDriverConfig()
	cfg.Brokers = []string{"broker.invalid:9092"}

	c, err := driver.New(cfg)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NoError(t, c.Close())
}

func TestDriverCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	c, err := driver.New(validDriverConfig())
	require.NoError(t, err)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "a second close must be a no-op, not an error")
	require.NoError(t, c.Close())
}

// Every operation on a closed consumer reports ErrClosed rather than panicking
// in cgo.
func TestDriverOperationsAfterCloseReportErrClosed(t *testing.T) {
	t.Parallel()

	c, err := driver.New(validDriverConfig())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, _, err = c.Watermarks(0)
	require.ErrorIs(t, err, driver.ErrClosed)

	_, err = c.Positions()
	require.ErrorIs(t, err, driver.ErrClosed)

	event := c.Poll(10 * time.Millisecond)
	failure, ok := event.(driver.Failure)
	require.True(t, ok, "poll on a closed consumer must report a failure, got %T", event)
	assert.True(t, failure.Fatal, "a closed consumer cannot recover, so the failure is fatal")
	assert.ErrorIs(t, failure, driver.ErrClosed)
}

// Positions needs the partition set, which only exists after a successful
// assignment.
func TestDriverPositionsBeforeAssignment(t *testing.T) {
	t.Parallel()

	c, err := driver.New(validDriverConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	_, err = c.Positions()
	require.ErrorIs(t, err, driver.ErrNotAssigned)
}

// Poll never returns nil, so callers switch on the event type without a nil
// check. With no broker reachable it returns either an idle tick or a
// non-fatal failure, and must keep being callable either way.
//
// No broker is contacted, and none is needed. RFC 2606 reserves the .invalid
// TLD so that it never resolves, so the address cannot connect to anything —
// what the events report is librdkafka's own background thread describing its
// failure to reach a broker, generated entirely locally:
//
//	poll 0 -> driver.Failure  Failed to resolve 'broker.invalid:9092': ...
//	poll 1 -> driver.Failure  1/1 brokers are down
//	poll 2 -> driver.Idle     {}
//	poll 3 -> driver.Idle     {}
//
// The failures stop after the first couple of polls: once librdkafka settles
// into reconnect backoff it stops re-reporting, so every later poll simply
// times out. Both outcomes are accepted below, since which one a given poll
// sees depends on that timing.
//
// Note this is not a hermetic test despite living beside the pure ones. New
// builds a real cgo client with real background threads, and resolving the
// address touches the system resolver. It sits here because "unit" in this
// repository means "needs no Docker", which is the split the Makefile and the
// two CI workflows enforce. On a network that hijacks NXDOMAIN the address
// broker.invalid would resolve, librdkafka would attempt a TCP connection and
// have it refused, and the test would still pass — for a different reason than
// intended.
//
// Using 127.0.0.1:1 instead would remove that dependency: it is a literal
// address, so no resolver is consulted and no such hijacking is possible, and
// port 1 is privileged and effectively never bound, so the connection is
// refused immediately by the local stack without a packet leaving the machine.
// The events would then be connection-refused rather than resolve failures,
// which exercises the same classification path. Kept as .invalid for now
// because the failure is more self-describing in the log line.
//
// librdkafka also logs its own FAIL line straight to stderr here, bypassing the
// configured logger; that noise in the test output is expected.
func TestDriverPollReturnsNonNilWithoutBroker(t *testing.T) {
	t.Parallel()

	cfg := validDriverConfig()
	cfg.Brokers = []string{"broker.invalid:9092"}

	c, err := driver.New(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	for range 3 {
		event := c.Poll(50 * time.Millisecond)
		require.NotNil(t, event)

		switch e := event.(type) {
		case driver.Idle:
		case driver.Failure:
			assert.False(t, e.Fatal, "an unreachable broker is recoverable, so not fatal: %v", e)
		default:
			t.Fatalf("unexpected event %T with no broker available", event)
		}
	}
}

// Failure behaves as an error value, so callers can match the driver error
// without unpacking the struct.
func TestDriverFailureIsAnError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	failure := driver.Failure{Err: sentinel, Fatal: true}

	var asError error = failure
	require.EqualError(t, asError, "boom")
	require.ErrorIs(t, asError, sentinel)

	assert.Equal(t, "kafka failure", driver.Failure{}.Error(), "a Failure with no cause still describes itself")
}

// AssignAll on an unreachable broker must fail rather than hang past the
// configured metadata timeout.
func TestDriverAssignAllFailsFastOnUnreachableBroker(t *testing.T) {
	t.Parallel()

	cfg := validDriverConfig()
	cfg.Brokers = []string{"broker.invalid:9092"}
	cfg.MetadataTimeout = 300 * time.Millisecond

	c, err := driver.New(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	start := time.Now()
	ids, err := c.AssignAll(t.Context())
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Nil(t, ids)
	assert.Less(t, elapsed, 5*time.Second, "must respect the metadata timeout rather than blocking bootstrap")
}

// A cancelled context is honoured before any broker call is made.
func TestDriverAssignAllHonoursCancelledContext(t *testing.T) {
	t.Parallel()

	c, err := driver.New(validDriverConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = c.AssignAll(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// The throttle is exercised through the exported behaviour it exists for: an
// unreachable broker errors on every poll, and the log must not carry one line
// per poll. Poll is called far more often than the log interval allows, so the
// consumer must survive it without stalling — the loop keeps full speed and
// only the logging is rationed.
//
// The obvious alternative is to sleep after each error, which quietens the log
// just as well and is a common way to handle this. It is rejected here because
// the sleep stalls the consumer: the loop stops fetching, a topic still being
// read stops making progress, and once the broker returns the recovery is
// delayed by however long the sleep had left to run. The assertion below is
// what pins that down — a one-second sleep per error, a plausible choice, would
// turn this twenty-poll loop into twenty seconds.
func TestDriverPollDoesNotStallOnRepeatedErrors(t *testing.T) {
	t.Parallel()

	cfg := validDriverConfig()
	cfg.Brokers = []string{"broker.invalid:9092"}
	cfg.ErrorLogInterval = time.Hour // suppress everything after the first line

	c, err := driver.New(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	const polls = 20

	start := time.Now()
	for range polls {
		require.NotNil(t, c.Poll(time.Millisecond))
	}
	elapsed := time.Since(start)

	// Generous bound: the work here is 20 polls of 1ms plus overhead, so this
	// fails only if something sleeps. A one-second sleep per error would take
	// 20 seconds.
	assert.Less(t, elapsed, 2*time.Second,
		"repeated errors must not sleep the poll loop; %d polls took %s", polls, elapsed)
}
