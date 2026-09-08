package easykafkaconfig_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	ekconfig "github.com/easykafka/easykafka-config-go"
)

// PlayerConfig is the value one compacted topic carries, one record per player.
type PlayerConfig struct {
	PlayerID string `json:"playerId"`
	Limit    int    `json:"limit"`
}

// A complete service: load configuration from compacted topics, serve lookups
// from memory, and shut down cleanly.
//
// Compiled with the tests but never run — it has no broker to reach, which is
// why it declares no Output. Do not "fix" that by adding one: it would make the
// test suite depend on a live Kafka.
func Example() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds the actual work and returns an error rather than exiting, so that
// deferred cleanup runs. log.Fatal skips defers, and for a loader that means
// the context is never cancelled and the consumers never waited for — which is
// why the exit lives in Example above and nothing below it calls log.Fatal.
func run() error {
	// The context the loader lives under. It is the process context, and it must
	// carry no deadline of its own: one that fired would stop every consumer and
	// freeze the stores for the rest of the process's life. Use
	// WithWarmupTimeout to bound startup instead.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancelling on a signal is what stops the loader. Written out rather than
	// using signal.NotifyContext, which does the same thing, because the point
	// here is that something has to call cancel and this is where.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		log.Printf("received %v, shutting down", sig)
		cancel()
	}()

	loader, err := ekconfig.NewLoader(
		ekconfig.WithBrokers("localhost:9092"),
		ekconfig.WithClientGroupID("my-service"),

		// A binding dying after warm-up freezes its store, so the process
		// should go down rather than serve configuration that can no longer
		// change. Calling the same cancel the signal handler calls means Kafka
		// failing and SIGTERM take the identical path out.
		ekconfig.WithFatalHandler(func(error) { cancel() }),
	)
	if err != nil {
		return fmt.Errorf("configuring the loader: %w", err)
	}

	// One Bind per topic, all of them before Start. The returned store is
	// usable immediately and empty until warm-up fills it.
	players := loader.Bind(ekconfig.Binding[string, PlayerConfig]{
		Name:        "PlayerConfig",
		Topic:       "player-config.compact",
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[PlayerConfig],
	})

	// Blocks until every bound topic has been read to its end. A non-nil error
	// is the whole configuration's failure, with every misconfigured topic
	// reported together rather than only the first.
	if err := loader.Start(ctx); err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	// From here the stores are complete and the consumers keep applying changes
	// in the background. Lookups are O(1), typed, and need no lock: GetOrNil
	// returns nil for a key the topic does not hold.
	if cfg := players.GetOrNil("player-42"); cfg != nil {
		log.Printf("player 42 has limit %d", cfg.Limit)
	}

	// ... serve traffic ...

	<-ctx.Done()

	// Cancelling is what stops the loader; WaitUntilStopped only waits for it,
	// returning once every consumer has stopped and been closed. Written as two
	// defers these would run in the wrong order — defers unwind last in, first
	// out — and the wait would block forever.
	//
	// Its error is why the loader stopped: nil for a clean shutdown, the fatal
	// error if Kafka took it down.
	return loader.WaitUntilStopped()
}
