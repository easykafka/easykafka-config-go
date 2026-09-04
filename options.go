package easykafkaconfig

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/easykafka/easykafka-config-go/internal/driver"
	"github.com/rs/zerolog"
)

// Defaults applied by NewLoader when an option is not given.
const (
	// DefaultClientGroupID is the group.id handed to the driver. Nothing joins
	// a consumer group, so the value only labels the client; see
	// WithClientGroupID.
	DefaultClientGroupID = "easykafka-config-go"

	// DefaultSteadyPollTimeout is how long each poll waits once a topic has
	// been read whole. Short, because it bounds how long a live change waits
	// before being applied.
	DefaultSteadyPollTimeout = 100 * time.Millisecond
)

// loaderConfig is the resolved configuration. Options write to it; the loader
// only reads it.
type loaderConfig struct {
	brokers  []string       // bootstrap servers, shared by every binding
	groupID  string         // the group.id the driver demands; inert, joins no group
	kafka    map[string]any // extra librdkafka properties, merged into each consumer
	security driver.Config  // SASL fields only, copied into each consumer

	detector          Detector      // how the end of a topic is detected during warm-up
	steadyPollTimeout time.Duration // poll wait once a topic has been read whole
	warmupTimeout     time.Duration // bound on Start; zero means wait indefinitely
	metadataTimeout   time.Duration // bound on the partition discovery before assigning

	logger   zerolog.Logger // lifecycle and connection events, never individual records
	observer Observer       // per-record reporting; NopObserver when unset
	onFatal  func(error)    // called once if a binding dies after warm-up; may be nil

	// consumerFactory builds each binding's consumer. Defaults to the real
	// driver; see WithConsumerFactory.
	consumerFactory func(driver.Config) (driver.Consumer, error)
}

func defaultLoaderConfig() loaderConfig {
	return loaderConfig{
		groupID:           DefaultClientGroupID,
		detector:          PartitionEOF(),
		steadyPollTimeout: DefaultSteadyPollTimeout,
		metadataTimeout:   driver.DefaultMetadataTimeout,
		observer:          NopObserver{},
		consumerFactory:   driver.New,
	}
}

func (c loaderConfig) validate() error {
	var errs []error

	if len(c.brokers) == 0 {
		errs = append(errs, errors.New("at least one broker is required (WithBrokers)"))
	}
	for _, b := range c.brokers {
		if strings.TrimSpace(b) == "" {
			errs = append(errs, errors.New("broker address cannot be empty"))

			break
		}
	}
	if strings.TrimSpace(c.groupID) == "" {
		errs = append(errs, errors.New("client group id cannot be empty"))
	}
	if c.detector == nil {
		errs = append(errs, errors.New("initial-load detector cannot be nil"))
	}
	if c.steadyPollTimeout <= 0 {
		errs = append(errs, errors.New("steady poll timeout must be positive"))
	}
	if c.warmupTimeout < 0 {
		errs = append(errs, errors.New("warm-up timeout cannot be negative"))
	}
	if c.observer == nil {
		errs = append(errs, errors.New("observer cannot be nil"))
	}
	if c.consumerFactory == nil {
		errs = append(errs, errors.New("consumer factory cannot be nil"))
	}

	return errors.Join(errs...)
}

// Option configures a Loader. Options are applied in order by NewLoader.
type Option func(*loaderConfig) error

// WithBrokers sets the bootstrap servers. Required.
func WithBrokers(brokers ...string) Option {
	return func(c *loaderConfig) error {
		if len(brokers) == 0 {
			return errors.New("WithBrokers needs at least one address")
		}
		c.brokers = append(c.brokers[:0:0], brokers...)

		return nil
	}
}

// WithClientGroupID sets the group.id the Kafka client is built with.
//
// The value is inert. Partitions are assigned explicitly, so no consumer group
// is ever joined and this does not affect what is read; it only labels the
// client in broker logs. It exists because the driver refuses to construct a
// consumer without one.
//
// In particular it does not need to be unique per replica — every replica reads
// every partition regardless. Defaults to DefaultClientGroupID.
func WithClientGroupID(id string) Option {
	return func(c *loaderConfig) error {
		if strings.TrimSpace(id) == "" {
			return errors.New("WithClientGroupID needs a non-empty id")
		}
		c.groupID = id

		return nil
	}
}

// WithSASL configures SASL authentication, for example
// WithSASL("SASL_SSL", "SCRAM-SHA-512", user, password). Omit it entirely for a
// plaintext broker.
func WithSASL(protocol, mechanism, username, password string) Option {
	return func(c *loaderConfig) error {
		if strings.TrimSpace(protocol) == "" {
			return errors.New("WithSASL needs a security protocol")
		}
		c.security.SecurityProtocol = protocol
		c.security.SASLMechanism = mechanism
		c.security.SASLUsername = username
		c.security.SASLPassword = password

		return nil
	}
}

// WithKafkaConfig passes additional librdkafka properties through to every
// consumer, for tuning things like fetch sizes.
//
// The properties this library's semantics depend on cannot be set here and are
// rejected with the reason — overriding offset committing or end-of-partition
// reporting would break the guarantees the API documents rather than adjusting
// them.
func WithKafkaConfig(cfg map[string]any) Option {
	return func(c *loaderConfig) error {
		if cfg == nil {
			return errors.New("WithKafkaConfig needs a non-nil map")
		}
		if c.kafka == nil {
			c.kafka = make(map[string]any, len(cfg))
		}
		maps.Copy(c.kafka, cfg)

		return nil
	}
}

// WithInitialLoadDetector chooses how the end of a topic is detected during
// warm-up. Defaults to PartitionEOF, which is the only option that makes no
// timing assumption.
func WithInitialLoadDetector(d Detector) Option {
	return func(c *loaderConfig) error {
		if d == nil {
			return errors.New("WithInitialLoadDetector needs a detector")
		}
		c.detector = d

		return nil
	}
}

// WithSteadyPollTimeout sets how long each poll waits once a topic has been
// read whole, which bounds how long a live change waits before it is applied.
// Defaults to DefaultSteadyPollTimeout.
func WithSteadyPollTimeout(d time.Duration) Option {
	return func(c *loaderConfig) error {
		if d <= 0 {
			return errors.New("WithSteadyPollTimeout needs a positive duration")
		}
		c.steadyPollTimeout = d

		return nil
	}
}

// WithWarmupTimeout bounds Start. Without it Start waits indefinitely, which is
// correct for a large topic but leaves a partition whose leader never answers
// able to hang startup forever. Zero, the default, means no bound.
func WithWarmupTimeout(d time.Duration) Option {
	return func(c *loaderConfig) error {
		if d < 0 {
			return errors.New("WithWarmupTimeout cannot be negative")
		}
		c.warmupTimeout = d

		return nil
	}
}

// WithMetadataTimeout bounds the partition-discovery call each binding makes
// before assigning. Defaults to driver.DefaultMetadataTimeout.
func WithMetadataTimeout(d time.Duration) Option {
	return func(c *loaderConfig) error {
		if d <= 0 {
			return errors.New("WithMetadataTimeout needs a positive duration")
		}
		c.metadataTimeout = d

		return nil
	}
}

// WithLogger sets the logger used for lifecycle and connection events. Records
// are never logged individually; that is what the Observer is for.
func WithLogger(l zerolog.Logger) Option {
	return func(c *loaderConfig) error {
		c.logger = l

		return nil
	}
}

// WithObserver sets the observer notified for every record applied, filtered or
// rejected. Defaults to NopObserver.
func WithObserver(o Observer) Option {
	return func(c *loaderConfig) error {
		if o == nil {
			return errors.New("WithObserver needs an observer")
		}
		c.observer = o

		return nil
	}
}

// WithFatalHandler registers a callback invoked once if a binding dies after
// warm-up succeeded — the push-based equivalent of watching Done and reading
// Err.
//
// It runs on the failing binding's goroutine, so it must not block. Marking the
// process unhealthy and letting the platform restart it is the expected use;
// the library itself never exits.
func WithFatalHandler(fn func(error)) Option {
	return func(c *loaderConfig) error {
		if fn == nil {
			return errors.New("WithFatalHandler needs a function")
		}
		c.onFatal = fn

		return nil
	}
}

// WithConsumerFactory replaces the function that builds each binding's Kafka
// consumer.
//
// This is a testing seam. It lets the whole loader — warm-up, the apply
// pipeline, the lifecycle — be driven by a scripted fake with no broker and no
// Docker, which is what keeps those tests fast and deterministic.
//
// It is exported only because the tests live in a separate package. It cannot
// be used from outside this module: its argument names a type under internal/,
// which Go forbids other modules from importing, so no caller can construct
// one. Production code has no reason to reach for it in any case.
func WithConsumerFactory(fn func(driver.Config) (driver.Consumer, error)) Option {
	return func(c *loaderConfig) error {
		if fn == nil {
			return errors.New("WithConsumerFactory needs a function")
		}
		c.consumerFactory = fn

		return nil
	}
}

// consumerConfig builds the driver configuration for one binding.
func (c loaderConfig) consumerConfig(topic string) driver.Config {
	return driver.Config{
		Brokers:          c.brokers,
		Topic:            topic,
		GroupID:          c.groupID,
		SecurityProtocol: c.security.SecurityProtocol,
		SASLMechanism:    c.security.SASLMechanism,
		SASLUsername:     c.security.SASLUsername,
		SASLPassword:     c.security.SASLPassword,
		MetadataTimeout:  c.metadataTimeout,
		Extra:            c.kafka,
		Logger:           c.logger,
	}
}

// newLoaderConfig resolves options over the defaults.
func newLoaderConfig(opts ...Option) (loaderConfig, error) {
	cfg := defaultLoaderConfig()
	for i, opt := range opts {
		if opt == nil {
			return loaderConfig{}, fmt.Errorf("option %d is nil", i)
		}
		if err := opt(&cfg); err != nil {
			return loaderConfig{}, err
		}
	}
	if err := cfg.validate(); err != nil {
		return loaderConfig{}, err
	}

	return cfg, nil
}
