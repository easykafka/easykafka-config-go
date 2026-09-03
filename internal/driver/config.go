package driver

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Default timings. Exported so the public options layer can document the same
// values without duplicating literals.
const (
	// DefaultMetadataTimeout bounds the partition-discovery call made before
	// assignment.
	DefaultMetadataTimeout = 5 * time.Second

	// DefaultReconnectBackoff and DefaultReconnectBackoffMax bound librdkafka's
	// own reconnection attempts, which it performs without our involvement.
	DefaultReconnectBackoff    = 100 * time.Millisecond
	DefaultReconnectBackoffMax = 10 * time.Second

	// DefaultErrorLogInterval is how often a repeating non-fatal error is
	// logged. A broker that is down produces an error on every poll, so the
	// interesting information is the state change plus an occasional reminder,
	// not thousands of identical lines.
	DefaultErrorLogInterval = 30 * time.Second
)

// Config describes one consumer: a single compacted topic, read whole.
//
// The public options layer builds this; nothing here is a functional option, so
// the driver stays independent of the API surface above it.
type Config struct {
	// Brokers is the bootstrap server list. Required.
	Brokers []string

	// Topic is the compacted topic to read. Required. One consumer reads one
	// topic, which keeps warm-up state, ordering and failure isolation per
	// topic simple.
	Topic string

	// GroupID satisfies the driver's requirement for a group.id and nothing
	// else. Partitions are assigned explicitly, so no group is ever joined and
	// this value has no effect on what is read — see Consumer.AssignAll.
	// Required, because the driver refuses to construct a consumer without it.
	GroupID string

	// SecurityProtocol, SASLMechanism, SASLUsername and SASLPassword configure
	// SASL. They are applied only when SecurityProtocol is set, so a plaintext
	// broker needs none of them.
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string

	// MetadataTimeout bounds partition discovery. Defaults to
	// DefaultMetadataTimeout.
	MetadataTimeout time.Duration

	// ErrorLogInterval throttles repeated non-fatal error logging. Defaults to
	// DefaultErrorLogInterval.
	ErrorLogInterval time.Duration

	// Extra passes additional librdkafka properties through, applied last so a
	// caller can override any default set here. The keys this driver depends on
	// are rejected rather than silently overridden — see reservedKeys.
	Extra map[string]any

	// Logger receives connection state changes and throttled error reports.
	Logger zerolog.Logger
}

// reservedKeys are librdkafka properties the driver sets deliberately and a
// caller may not override, with the reason reported when one is attempted.
// Everything else may be passed through Config.Extra.
var reservedKeys = map[string]string{
	"bootstrap.servers":        "set from Config.Brokers",
	"group.id":                 "set from Config.GroupID",
	"auto.offset.reset":        "assignment pins the start offset explicitly, so this is never consulted",
	"enable.auto.commit":       "offsets are never committed: a restart must re-read the whole topic",
	"enable.auto.offset.store": "offsets are never committed, so there is nothing to store",
	"enable.partition.eof":     "end-of-partition events are how warm-up completion is detected",
}

// withDefaults returns a copy with unset optional fields filled in.
func (c Config) withDefaults() Config {
	if c.MetadataTimeout <= 0 {
		c.MetadataTimeout = DefaultMetadataTimeout
	}
	if c.ErrorLogInterval <= 0 {
		c.ErrorLogInterval = DefaultErrorLogInterval
	}

	return c
}

// validate reports every problem with the config rather than only the first.
func (c Config) validate() error {
	var errs []error

	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("at least one broker is required"))
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			errs = append(errs, errors.New("broker address cannot be empty"))

			break
		}
	}
	if strings.TrimSpace(c.Topic) == "" {
		errs = append(errs, errors.New("topic is required"))
	}
	if strings.TrimSpace(c.GroupID) == "" {
		errs = append(errs, errors.New("group id is required (the driver refuses to build a consumer without one)"))
	}
	for key := range c.Extra {
		if reason, ok := reservedKeys[key]; ok {
			errs = append(errs, fmt.Errorf("kafka property %q is managed by this library (%s)", key, reason))
		}
	}

	return errors.Join(errs...)
}
