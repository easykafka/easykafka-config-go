module github.com/easykafka/easykafka-config-go

go 1.27.0

// Version constraints and rationale
// - confluent-kafka-go/v2: pinned to the same version as easykafka-go and srm-common so a single
//   librdkafka build is in play across the SRM binaries (see api-design.md, R9). This library talks
//   to the driver directly and takes no dependency on easykafka-go (decision D1).
//   The first real import lands in P2 (internal/driver); the pin is fixed here so P2 does not have
//   to make the version decision.
require github.com/confluentinc/confluent-kafka-go/v2 v2.15.0
