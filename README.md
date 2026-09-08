<!-- Mirror notice. Kept as plain HTML on purpose: it renders as a bordered box on
     both GitHub and GitLab, whereas GitHub's "> [!IMPORTANT]" alert syntax would
     show up as literal text on the mirror — the one place it needs to be read. -->
<table>
  <tr>
    <td>
      <h3>⚠️ &nbsp;Not on <code>github.com/easykafka</code>? You are reading a mirror.</h3>
      <p>
        This copy is <strong>read-only</strong> and may lag behind. Issues, pull requests, releases and CI
        all live at the source of truth:<br><br>
        👉 &nbsp;<a href="https://github.com/easykafka/easykafka-config-go"><strong>github.com/easykafka/easykafka-config-go</strong></a>
      </p>
    </td>
  </tr>
</table>

# 📇 easykafka-config-go

[![Build & Lint](https://github.com/easykafka/easykafka-config-go/actions/workflows/build-lint.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/build-lint.yml)
[![Unit Tests](https://github.com/easykafka/easykafka-config-go/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/easykafka/easykafka-config-go/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/integration-tests.yml)
[![codecov](https://codecov.io/gh/easykafka/easykafka-config-go/branch/main/graph/badge.svg)](https://codecov.io/gh/easykafka/easykafka-config-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/easykafka/easykafka-config-go.svg)](https://pkg.go.dev/github.com/easykafka/easykafka-config-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Compacted Kafka topics as typed, thread-safe, in-memory maps.

* **[How it works](developer-doc.md)** — the deep dive: lifecycle, guarantees and their limits, failure
  behaviour, the design decisions and what they cost, and how it compares with Kafka Streams.
* **[Changelog](CHANGELOG.md)** — what each release contains, and what it deliberately does not.

> **Status: feature-complete, and built for services that must not start on incomplete configuration.**
> A loader reads compacted topics into typed stores — warm-up, tombstones, live updates, lifecycle,
> introspection — covered by 159 unit tests and 19 integration tests against a real broker, a broker
> outage among them. Designed for production use at scale: every replica reads every partition with no
> consumer group and no rebalance, so adding one costs the cluster nothing in coordination.

## 💡 Why easykafka-config-go?

A compacted topic is a table: each record is an absolute upsert for its key, and an empty record is a
delete. Services that read configuration this way all end up writing the same machinery — one consumer
per topic, read from the beginning, work out when the backlog is drained, decode into a map, keep
applying changes forever, and never commit an offset so a restart re-reads everything.

This library owns that machinery so a service declares *what* it wants instead of *how* to get it:

1. **Warm-up** — read every configured topic to its end, then let bootstrap continue.
2. **Apply** — decode each record into a typed value; upsert it, or delete on a tombstone.
3. **Serve lookups** — O(1), type-safe reads with no locking in the caller.

## 🛠 Installation

```bash
go get github.com/easykafka/easykafka-config-go
```

Requires Go 1.27+ (generic methods, iterators) and a C toolchain for `librdkafka` — see the
confluent-kafka-go docs for platform specifics.

### A broker to try it against

The usage below connects to `localhost:9092`. If you have no broker there:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0

# The library reads compacted topics, and a topic has to be created as one —
# auto-creation would give you a normal log, where nothing is ever superseded.
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic player-config.compact \
    --partitions 3 \
    --config cleanup.policy=compact
```

That image runs in KRaft mode and advertises `localhost:9092` with no configuration, so nothing else is
needed. `docker rm -f kafka` when you are done.

Then run the worked example against it — it loads the topic, prints what it holds, and prints each
change as it arrives:

```bash
go run ./examples/quickstart
```

In another terminal, produce a record and watch it land:

```bash
echo 'player-42:{"playerId":"player-42","limit":500}' | docker exec -i kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic player-config.compact --property parse.key=true --property key.separator=:
```

An empty value is a tombstone, so this removes the key again:

```bash
echo 'player-42:' | docker exec -i kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic player-config.compact --property parse.key=true --property key.separator=:
```

Each is a single command that reads its record from a pipe. Running the producer interactively and
typing the records works too, but a stray blank line ends it with `No key separator found on line
number 1`, which is a confusing way to find that out.

## 🚀 Usage

```go
// The process context. Cancelling it is what stops the loader, so it must
// carry no deadline of its own — use WithWarmupTimeout to bound startup.
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

signals := make(chan os.Signal, 1)
signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

go func() {
    <-signals
    cancel()                                // this is what shuts the loader down
}()

loader, err := ekconfig.NewLoader(
    ekconfig.WithBrokers("localhost:9092"),
    ekconfig.WithClientGroupID("my-config-reader"),
)
if err != nil {
    log.Fatal(err)
}

players := loader.Bind(ekconfig.Binding[string, PlayerConfig]{
    Name:        "PlayerConfig",
    Topic:       "player-config.compact",
    DecodeKey:   ekconfig.StringKey,
    DecodeValue: ekconfig.JSONValue[PlayerConfig],
})

if err := loader.Start(ctx); err != nil {   // blocks until every topic is drained
    log.Fatal(err)
}

cfg := players.GetOrNil("player-42")        // typed; no assertions, no lock

// ... serve traffic; the loader keeps applying changes in the background ...

<-ctx.Done()                                // a signal arrived and cancel ran

// WaitUntilStopped returns once every consumer has stopped and been closed,
// and reports why — nil for a clean shutdown, the error if Kafka took it down.
if err := loader.WaitUntilStopped(); err != nil {
    log.Printf("config loader stopped: %v", err)
}
```

## 🛑 Graceful shutdown

Stopping is the caller's job, in two steps that must happen in that order:

```go
<-ctx.Done()                                // the signal handler above called cancel
err := loader.WaitUntilStopped()            // waits for the consumers, and says why they stopped
```

`WaitUntilStopped` only **waits**. Cancelling the context passed to `Start` is what stops anything, and
calling the wait without cancelling blocks until the process ends. Written as two `defer`s they reverse
— defers unwind last-in-first-out — so the wait would run first and never return.

When it returns, three things are true: no polling goroutine is running, every librdkafka handle has
been released, and no store is being written any more. That last one is why it exists: without it,
shutdown races a record still being applied, possibly into an `Observer` whose metrics registry the
service has already torn down.

It takes no timeout and needs none — a consumer notices cancellation between polls, so the wait is
bounded by one steady poll timeout (100 ms by default) plus the consumer's own close.

Wire `WithFatalHandler` into the same `cancel` and a broker failure takes the same path as `SIGTERM`:

```go
ekconfig.WithFatalHandler(func(error) { cancel() })
```

## ⚙️ Configuration reference

Every option is optional except `WithBrokers`.

| Option | Default | What it does |
|---|---|---|
| `WithBrokers(...)` | — | **Required.** Bootstrap servers. |
| `WithClientGroupID(id)` | `easykafka-config-go` | The `group.id` the client is built with. Inert: no group is ever joined, so it only labels the client in broker logs and need not be unique per replica. |
| `WithSASL(protocol, mechanism, user, pass)` | none | SASL authentication. Omit entirely for a plaintext broker. |
| `WithKafkaConfig(map)` | none | Extra librdkafka properties. Properties the library's semantics depend on are rejected with the reason. |
| `WithInitialLoadDetector(d)` | `PartitionEOF()` | How the end of a topic is detected during warm-up, and the poll timeout used while reading it. `PartitionEOF` is currently the only detector — see the note below. |
| `WithSteadyPollTimeout(d)` | 100 ms | How long each poll waits once a topic has been read whole. Bounds how long a live change waits to be applied, and how long shutdown takes. |
| `WithWarmupTimeout(d)` | 0 — no bound | Bounds `Start`. Without it a partition whose leader never answers hangs startup indefinitely; with it, that surfaces as `ErrWarmupTimeout`. |
| `WithMetadataTimeout(d)` | 5 s | Bounds the partition-discovery call each binding makes before assigning. |
| `WithLogger(l)` | disabled | zerolog logger for lifecycle and connection events. Individual records are never logged here. |
| `WithObserver(o)` | `NopObserver{}` | Per-record reporting: upserts, deletes, filtered records, decode errors, key mismatches, load progress, phase changes, Kafka errors. This is the metrics seam. |
| `WithErrorLogging()` | off | Also log a warning for each record skipped as undecodable, and each key mismatch. Those two reach only the `Observer`, so without this a service that wires none skips bad records in silence. |
| `WithFatalHandler(fn)` | none | Called once if a binding dies after warm-up. Runs on that binding's goroutine, so it must not block. |
| `WithConsumerFactory(fn)` | the real driver | A testing seam, usable only from inside this module. |

**Only one warm-up detector exists.** `PartitionEOF` waits for the broker to report every assigned
partition exhausted — no timing assumption, and an empty topic reported in milliseconds rather than
after a timeout. Detectors based on idle polls and on watermarks are designed and deliberately not
built: an idle-poll heuristic cannot tell a stalled partition from a drained topic, which is precisely
the failure this one rules out.

## 🧭 Design notes

* **Partitions are assigned manually** (`Assign` at `OffsetBeginning`), never subscribed. Nothing joins
  a consumer group, so every replica reads *all* partitions, there is no rebalance, and no group is
  created in the cluster. A `group.id` is still configured because the driver demands one — it is inert.
* **Offsets are never committed.** A restart re-reads the topic by construction, which is what makes the
  in-memory map reproducible.
* **Warm-up completion is detected, not guessed.** `PartitionEOF` waits for an end-of-partition report
  on every assigned partition, so an empty topic is reported in milliseconds rather than after a
  timeout, and a slow broker is never mistaken for a drained one. Watermark- and idle-poll-based
  detectors were designed and deliberately not shipped: an idle-poll heuristic cannot tell a stalled
  partition from a drained topic, which is the failure this one exists to rule out.
* **Stopping belongs to the caller.** There is no cancellation inside the loader and nothing to close:
  cancel the context you passed to `Start`, then call `WaitUntilStopped`. Each consumer is closed by
  the goroutine that was polling it, so no librdkafka handle is ever closed from another goroutine.
* **Nothing is fatal inside the library.** Empty required topic, bad payload, broker loss — all surface
  as errors, lifecycle state, or observer callbacks. Only the service decides to exit.

## 🔀 Relationship to [easykafka-go](https://github.com/easykafka/easykafka-go)

Sibling libraries, no dependency between them. `easykafka-go` is the right tool for **event** consumers,
where handler functions, retry topics, DLQs and circuit breakers are exactly what you want. This library
covers the one pattern that abstraction does not fit: a compacted topic read to its end, keyed by record
key, with no offset commits and no retry semantics. It talks to `confluent-kafka-go` directly.

## 🆚 Compared with Kafka Streams

If you know Kafka Streams, the thing this library resembles is a **`GlobalKTable`**: every replica reads
every partition of a topic into a local store, so any instance can answer for any key. That is the same
topology, down to the detail that Streams' global consumer also assigns partitions manually and joins no
consumer group.

The first thing to say, though, is that **Kafka Streams is a JVM framework and there is no Go
implementation** — so for a Go service this is not a choice between the two.

| | `easykafka-config-go` | Kafka Streams `GlobalKTable` |
|---|---|---|
| Runtime | a Go library: a loader and a map | a stream-processing framework on the JVM |
| Available from Go | yes | no |
| State store | in-memory map of decoded Go values | RocksDB on disk by default, in-memory optional |
| Dataset size | bounded by the replica's memory | bounded by disk — RocksDB spills, so far larger tables are practical |
| Restart | always re-reads the topic; the store is identical by construction, since no offset is ever committed | restores from the local state directory when it survives, re-reads when it does not |
| Startup | read to end, then serve | restore the global store, then start processing |
| Lookup | typed map read, no serdes, no lock | store read through the framework's serdes |
| Derived state | none | the point of the framework: joins, aggregations, windows, repartitioning, output topics |
| Delivery semantics | at-least-once application of an idempotent upsert | configurable, including exactly-once |
| To operate | memory | heap, RocksDB tuning, state directories, standby replicas, changelog topics |

The full version of this table, with the reasoning, is in [developer-doc.md](developer-doc.md).

**This library does one thing Streams also does, and none of the rest.** If the state you need is a
compacted topic read into a map, the two agree on the architecture and this is the smaller way to get
there. If you need anything *derived* from that state — a join, an aggregate, a window, an output topic
— this has no answer, and Streams is a real framework for that job.

Two claims it would be easy to make and wrong to: this is not "faster than Kafka Streams", which nothing
here has measured, and it does not "avoid network IO" — a global store is local in both, and both keep
consuming in the background. What it avoids is RocksDB, a serde layer and a JVM.

## 🧪 Development

```bash
make install-tools     # pinned golangci-lint + gotestsum into ./bin
make build-lint        # what CI runs
make test-unit         # no Docker needed
make test-integration  # requires Docker (testcontainers-go)
make coverage          # unit + integration, -coverpkg=./...
make help              # list all targets
```

## 📜 Releases

Version history is in [CHANGELOG.md](CHANGELOG.md); tagged releases appear on
[pkg.go.dev](https://pkg.go.dev/github.com/easykafka/easykafka-config-go). Pre-1.0, the public API may
still change in a minor release.

## 📄 Licence

MIT — see [MIT-LICENSE.md](MIT-LICENSE.md).
