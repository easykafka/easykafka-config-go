// Package integration holds tests that require Docker (Kafka via testcontainers-go).
//
// This placeholder exists so `make test-integration` and the CI workflow are
// exercised before any real test does. It skips rather than starting a
// container: there is nothing to drive until the driver layer lands.
//
// TODO: replace with the real suite — warm-up via partition EOF, manual
// assignment (including the SubscribeTopics control case), tombstones, restart
// re-read, empty topic, live updates, decode errors and reconnection — plus a
// shared cluster helper in tests/integration/helpers/.
package integration

import "testing"

func TestScaffolding(t *testing.T) {
	t.Skip("no integration tests yet: nothing to drive until the driver layer lands")
}
