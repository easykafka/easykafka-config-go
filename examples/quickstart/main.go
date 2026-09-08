// Command quickstart loads a compacted topic into memory and prints changes as
// they arrive.
//
// It is a working program rather than a snippet: start a broker, run it, then
// produce records in another terminal and watch them land. See the README for
// the broker and topic commands.
//
//	go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	ekconfig "github.com/easykafka/easykafka-config-go"
)

// PlayerConfig is one record on the topic, keyed by player id.
type PlayerConfig struct {
	PlayerID string `json:"playerId"`
	Limit    int    `json:"limit"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// The process context. Cancelling it is what stops the loader, so it must
	// carry no deadline of its own.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		fmt.Println("\nshutting down")
		cancel()
	}()

	loader, err := ekconfig.NewLoader(
		ekconfig.WithBrokers("localhost:9092"),
		ekconfig.WithClientGroupID("quickstart"),

		// Printing every change is what makes this a demonstration rather than
		// a program that sits silently. A real service would drive metrics from
		// here instead.
		ekconfig.WithObserver(printingObserver{}),

		// A binding dying after warm-up freezes its store, so take the process
		// down the same way a signal would.
		ekconfig.WithFatalHandler(func(err error) {
			fmt.Printf("loader failed: %v\n", err)
			cancel()
		}),
	)
	if err != nil {
		return fmt.Errorf("configuring the loader: %w", err)
	}

	players := loader.Bind(ekconfig.Binding[string, PlayerConfig]{
		Name:        "PlayerConfig",
		Topic:       "player-config.compact",
		DecodeKey:   ekconfig.StringKey,
		DecodeValue: ekconfig.JSONValue[PlayerConfig],

		// A freshly created topic has nothing in it, and by default that is a
		// deployment fault rather than a state to accept. Here it is expected,
		// so say so — otherwise Start fails with ErrEmptyTopic before you have
		// produced anything.
		AllowEmpty: true,
	})

	fmt.Println("loading...")

	// Blocks until the whole topic has been read.
	if err := loader.Start(ctx); err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	fmt.Printf("loaded %d entries; watching for changes, ^C to stop\n\n", players.Len())
	for key, cfg := range players.All() {
		fmt.Printf("  %-12s limit=%d\n", key, cfg.Limit)
	}

	// Produce to the topic in another terminal and the lines appear here:
	//
	//	echo 'player-42:{"playerId":"player-42","limit":500}' | docker exec -i kafka \
	//	    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
	//	    --topic player-config.compact --property parse.key=true --property key.separator=:
	//
	// An empty value deletes the key, which is what a tombstone is: send
	// 'player-42:' the same way. A piped record rather than an interactive
	// producer, because a stray blank line makes that one exit with "No key
	// separator found".
	<-ctx.Done()

	// Cancelling stopped the consumers; this waits for them to finish and
	// reports why they stopped.
	if err := loader.WaitUntilStopped(); err != nil {
		return fmt.Errorf("loader stopped: %w", err)
	}

	fmt.Println("stopped cleanly")

	return nil
}

// printingObserver reports each change to stdout. Embedding NopObserver means
// it keeps compiling if the interface grows.
type printingObserver struct {
	ekconfig.NopObserver
}

func (printingObserver) OnUpsert(name string, warmup bool) {
	if !warmup {
		fmt.Printf("  [%s] upsert\n", name)
	}
}

func (printingObserver) OnDelete(name string, warmup bool) {
	if !warmup {
		fmt.Printf("  [%s] delete\n", name)
	}
}

func (printingObserver) OnDecodeError(name string, err error, raw []byte) {
	fmt.Printf("  [%s] skipped a record: %v (%s)\n", name, err, raw)
}

func (printingObserver) OnPhase(name string, phase ekconfig.Phase, applied int, took time.Duration) {
	fmt.Printf("  [%s] %s after %d records in %s\n", name, phase, applied, took.Round(time.Millisecond))
}
