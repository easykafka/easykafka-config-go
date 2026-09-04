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

# 🗺️ easykafka-config-go

[![Build & Lint](https://github.com/easykafka/easykafka-config-go/actions/workflows/build-lint.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/build-lint.yml)
[![Unit Tests](https://github.com/easykafka/easykafka-config-go/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/easykafka/easykafka-config-go/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/easykafka/easykafka-config-go/actions/workflows/integration-tests.yml)
[![codecov](https://codecov.io/gh/easykafka/easykafka-config-go/branch/main/graph/badge.svg)](https://codecov.io/gh/easykafka/easykafka-config-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/easykafka/easykafka-config-go.svg)](https://pkg.go.dev/github.com/easykafka/easykafka-config-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Compacted Kafka topics as typed, thread-safe, in-memory maps.

> **Status: work in progress.** `Store`, `Binding` and the codecs are implemented and tested; the
> `Loader`, the driver layer and the warm-up detectors are still to come, so the library cannot yet
> read a topic.

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

## 🚀 Intended usage

```go
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
defer loader.Close(context.WithoutCancel(ctx))

cfg := players.GetOrNil("player-42")        // typed; no assertions, no lock
```

## 🧭 Design notes

* **Partitions are assigned manually** (`Assign` at `OffsetBeginning`), never subscribed. Nothing joins
  a consumer group, so every replica reads *all* partitions, there is no rebalance, and no group is
  created in the cluster. A `group.id` is still configured because the driver demands one — it is inert.
* **Offsets are never committed.** A restart re-reads the topic by construction, which is what makes the
  in-memory map reproducible.
* **Warm-up completion is detected, not guessed.** The default waits for `PartitionEOF` on every
  assigned partition; watermark-based and idle-poll detectors are also available.
* **Nothing is fatal inside the library.** Empty required topic, bad payload, broker loss — all surface
  as errors, lifecycle state, or observer callbacks. Only the service decides to exit.

## 🔀 Relationship to [easykafka-go](https://github.com/easykafka/easykafka-go)

Sibling libraries, no dependency between them. `easykafka-go` is the right tool for **event** consumers,
where handler functions, retry topics, DLQs and circuit breakers are exactly what you want. This library
covers the one pattern that abstraction does not fit: a compacted topic read to its end, keyed by record
key, with no offset commits and no retry semantics. It talks to `confluent-kafka-go` directly.

## 🧪 Development

```bash
make install-tools     # pinned golangci-lint + gotestsum into ./bin
make build-lint        # what CI runs
make test-unit         # no Docker needed
make test-integration  # requires Docker (testcontainers-go)
make coverage          # unit + integration, -coverpkg=./...
make help              # list all targets
```

## 📄 Licence

MIT — see [MIT-LICENSE.md](MIT-LICENSE.md).
