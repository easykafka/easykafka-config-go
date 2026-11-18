# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-11-18

The first stable release. The API and the loader's behaviour are the ones 0.1.1 shipped; what
changes is the promise. From here on, a breaking change to them waits for 2.0.0.

### What 1.0.0 covers

These stay compatible across every 1.x release:

- **The exported API of the package**: `Loader`, `Store`, `Binding`, the codecs and tombstone
  policies, `Observer`, `Stats`, `LookupRaw`, the options and the sentinel errors, and what each is
  documented to do.
- **The behaviour the README's *Design notes* describe.** `Start` returns only once every bound
  topic has been read to its end, and warm-up is all-or-nothing. Partitions are assigned explicitly,
  so no consumer group is joined. Offsets are never committed.

These are not covered:

- **Anything under `internal/`.** Go already stops other modules from importing it.
- **`WithConsumerFactory`.** It is the unit tests' seam onto the fake consumer. Its argument names an
  `internal/` type, so no other module can call it anyway.

### Changed

- **Requires `confluent-kafka-go` 2.16.1**, up from 2.16.0: a patch release of the Kafka client. It
  is the same version `easykafka-go` 1.0.0 requires, so a service using both libraries builds against
  one librdkafka.

## [0.1.1] - 2026-10-13

A maintenance release: no change to the API or to the loader's behaviour.

### Changed

- **Requires `confluent-kafka-go` 2.16.0** (librdkafka 2.16), up from 2.15.0. It brings 2.15.1's
  fix for `Consumer.Close()` hanging indefinitely after a fatal consumer error, and librdkafka 2.16's
  fixes, among them re-bootstrapping when every known broker has gone stale and bundled OpenSSL and
  libcurl security updates. The same version as `easykafka-go`, so one librdkafka build is in play.

## [0.1.0]

First release.

### Added

- **`Loader`** — reads compacted Kafka topics into typed, in-memory stores and keeps them live.
  `Start` blocks until every bound topic has been read to its end, so a service that gets past it holds
  complete configuration. Warm-up is all-or-nothing, and reports every failing topic together rather
  than only the first.
- **`Store[K, V]`** — the typed concurrent map behind each binding: `Get`, `GetOrNil`, `Has`, `Len`,
  `All`, `Keys`, `Snapshot`. Lookups take no lock the caller can see.
- **`Binding[K, V]`** — one topic's declaration: how to decode its keys and values, an optional
  `Filter`, an optional `KeyFromValue` with `VerifyKeyAgreement`, a tombstone policy, and `AllowEmpty`.
- **Ready-made codecs** — `StringKey`, `IntKey`, `Int64Key`, `JSONValue[V]`, and the tombstone policies
  `TombstoneOnBlankPayload`, `TombstoneOnNilPayload`, `TombstoneNever`.
- **`PartitionEOF`** warm-up detection: wait for the broker to report every assigned partition
  exhausted. No timing assumption, and an empty topic reported in milliseconds rather than after a
  timeout.
- **`Observer`** for per-record reporting and **`Stats()`** for per-binding snapshots — push for
  counters, pull for gauges.
- **`LookupRaw`** for debug endpoints that serve any binding by name and undecoded key.
- **Thirteen options**, from `WithBrokers` to `WithErrorLogging`. See the README's configuration
  reference.

### Design decisions worth knowing

- **Partitions are assigned explicitly and no consumer group is joined**, so every replica reads every
  partition, there is no rebalance, and no group is created in the cluster. The configured `group.id`
  is inert.
- **Offsets are never committed.** A restart rebuilds the stores by construction, which is what makes
  them reproducible.
- **The library never terminates the process.** Every failure is an error, a lifecycle state, or an
  observer callback.

### Not in this release

These are named because the design documents describe them and a reader may come looking:

- **`IdlePolls` and `Watermarks` warm-up detectors are designed and deliberately not built.**
  `PartitionEOF` is the only detector. An idle-poll heuristic cannot tell a stalled partition from a
  drained topic, which is the failure `PartitionEOF` exists to rule out; a watermark detector's value
  is a consumer-lag figure, and there is nowhere to publish one from yet. `WithInitialLoadDetector`
  remains, because those designs stand and because it carries the warm-up poll timeout.

[1.0.0]: https://github.com/easykafka/easykafka-config-go/compare/v0.1.1...v1.0.0
[0.1.1]: https://github.com/easykafka/easykafka-config-go/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/easykafka/easykafka-config-go/releases/tag/v0.1.0
