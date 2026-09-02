// Package integration holds tests that require Docker (Kafka via testcontainers-go).
//
// Phase P0 ships this placeholder so `make test-integration` and the CI workflow
// are exercised before any real test exists. It skips rather than starting a
// container: there is nothing to test until the driver layer lands in P2.
// TODO: Replace it with the P6 suite (see implementation-plan.md, T-6.1…T-6.12) and the
// shared testcontainers helper in tests/integration/helpers/.
package integration

import "testing"

func TestScaffolding(t *testing.T) {
	t.Skip("no integration tests yet — the suite lands in phase P6 (see implementation-plan.md)")
}
