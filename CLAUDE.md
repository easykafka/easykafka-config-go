# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`easykafka-config-go` projects compacted Kafka topics into typed, thread-safe, in-memory maps and keeps
them live for the lifetime of the process. A service declares one binding per topic, blocks once at
startup until every topic is read to its end, then does O(1) type-safe lookups while the library applies
changes in the background.

**Status: phase P0 (scaffolding).** Tooling and CI are in place; the API is designed, not implemented.
The authoritative design and phased plan live in the `srm-specs` repo:
`specs/easykafka/001-config-from-compact-topics/{requirements,api-design,implementation-plan}.md`.
Read the design before adding code — the decisions below are settled there, not open for re-litigation
in a code review.

## Commands

### Testing
```bash
# Unit tests (no Docker)
go test -v -count=1 -race ./tests/unit/...

# Integration tests (require Docker — uses testcontainers-go)
go test -v -count=1 -timeout 1000s ./tests/integration/...

# Human-readable output (what make uses)
make test-unit
make test-integration

# Coverage (note: -coverpkg=./... is required to instrument library packages, not just test packages)
go test -count=1 -timeout 1000s -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./tests/...

# Single test by name
go test -v -count=1 -run TestName ./tests/unit/...
```

### Build
```bash
go build ./...
go mod download
```

### Tooling & Linting
Dev tools are installed into `./bin` (gitignored) at pinned versions — never globally, and never via
brew. Run this once after cloning:
```bash
make install-tools        # golangci-lint + gotestsum into ./bin
make lint                 # ./bin/golangci-lint run
```

Version pins are single-sourced:
- `.golangci-lint-version` — read by both the `Makefile` and `.github/workflows/build-lint.yml` (via the
  action's `version-file` input), so local and CI always run the same linter build. Bump that one file.
- `GOTESTSUM_VERSION` in the `Makefile` — CI installs it via `make install-test-tools` and runs tests
  through `make test-unit` / `make test-integration`, so flags cannot drift between local and CI.

`.golangci.yml` is kept in sync with `easykafka-go` and the other SRM Go repos (`srm-common`,
`srm-overask`, `srm-transactional-core`); `tests/` is excluded from the churn-heavy linters (`mnd`,
`lll`, `dupl`, `gocognit`, `gosec`, `errcheck`, `unparam`).

## Architecture

### Public API (root package `easykafkaconfig`, imported as `ekconfig`)
- `store.go` — `Store[K,V]`: the typed concurrent map (`Get`/`GetOrNil`/`Len`/`All`/`Keys`)
- `binding.go` — `Binding[K,V]` plus ready-made codecs (`StringKey`, `IntKey`, `JSONValue`) and
  tombstone policies
- `loader.go` — `Loader`: `Bind`/`BindTo` (generic methods), `Start`/`Ready`/`Done`/`Err`/`Close`,
  `Stats`, `LookupRaw`
- `options.go` — functional options
- `detect.go` — warm-up detectors: `PartitionEOF` (default), `IdlePolls`, `Watermarks`
- `observer.go` — `Observer`, `NopObserver`, `LogObserver`, `BindingStats`

### Internal packages
- `internal/driver/` — **the only package that may import `confluent-kafka-go`**: consumer construction
  and SASL, partition discovery, `Assign`, the poll loop, event classification, reconnect logging

### Settled decisions that constrain the code
- **Direct driver, no `easykafka-go` dependency** in either direction (design decision D1).
- **Manual partition assignment** (`Assign` at `OffsetBeginning`), never `SubscribeTopics` (D2). No
  rebalance callback, no `group.instance.id`. The configured `group.id` is inert.
- **Never commit offsets.** No `Commit`/`CommitOffsets` call anywhere. This is load-bearing twice: it is
  what makes a restart re-read the whole topic, and what makes a shared inert `group.id` safe across
  replicas.
- **The library never terminates the process** — no `log.Fatal`, no `os.Exit`. Failures are errors,
  lifecycle state (`Err()`), or `Observer` callbacks.
- **Generic methods** (Go 1.27) let `Bind` be a method on `*Loader`; the loader itself stays non-generic
  and stores closures, since it holds bindings with different `K`/`V` pairs.

### Testing approach
- `tests/unit/` — pure Go logic, no Kafka dependency. The loader and detectors are tested against a fake
  driver, with `testing/synctest` for anything time-dependent.
- `tests/integration/` — real Kafka via testcontainers-go: warm-up via partition EOF, manual-assign
  behaviour (including the `SubscribeTopics` control case), tombstones, restart re-read, empty topic,
  live updates, decode errors, reconnection.

### Key dependencies
- `confluent-kafka-go/v2` — underlying Kafka client (pinned to the same version as `easykafka-go` and
  `srm-common` so one librdkafka build is in play)
- `zerolog` — structured logging
- `testcontainers-go/modules/kafka` — integration test containers
- `testify` — test assertions

### CI/CD
GitHub Actions workflows in `.github/workflows/`, mirroring `easykafka-go`:
- `build-lint.yml` — build + golangci-lint (version from `.golangci-lint-version`)
- `unit-tests.yml` — unit tests with `-race`
- `integration-tests.yml` — integration tests with Docker/testcontainers
- `coverage.yml` — uploads coverage to Codecov

### The Kafka image mirror is shared across the organisation

Integration tests and the coverage job pull Kafka from
`ghcr.io/<owner>/mirror-confluentinc-cp-kafka:7.5.0` via the `KAFKA_IMAGE` environment variable, which
exists to avoid Docker Hub rate limits. Locally the helper falls back to Docker Hub, so no login or
mirror is needed for `make test-integration`.

**This repository does not mirror the image, on purpose.** A GHCR package is owned by the
*organisation*, not by a repository, so `ghcr.io/easykafka/mirror-...` is one shared package and
mirroring it a second time from here would only re-push the same digest on a second weekly schedule.
`easykafka-go` owns that job (`mirror-images.yml`) for the whole org; every other repository consumes
the result.

The catch worth knowing when adding a new repository: although the package is org-owned, each package
keeps its **own per-repository access list**, and a package created by one repo's workflow starts out
linked to that repo alone. A new repo's `GITHUB_TOKEN` is then denied — `permission_denied:
write_package` when pushing, or a bare `denied` from the Docker daemon when testcontainers tries to
pull. Note that `permissions: packages: read` only grants the token the right to ask; the package's
access list decides whether it gets in. Fix it in the org's package settings (**Manage Actions
access**), or make the mirror public, since it is a byte-identical copy of a public image.
