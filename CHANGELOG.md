# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the usual pre-1.0 caveat that the
public API may still change in a minor release.

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

[0.1.0]: https://github.com/easykafka/easykafka-config-go/releases/tag/v0.1.0
