// Package unit holds tests that need neither Kafka nor Docker.
//
// This file only proves the scaffolding works end to end (build, vet, race
// detector, gotestsum, coverage instrumentation) on an otherwise empty module.
// Delete it once phase P1 lands real store/codec tests.
package unit

import (
	"testing"

	ekconfig "github.com/easykafka/easykafka-config-go"
)

func TestScaffolding(t *testing.T) {
	t.Parallel()

	// The assertion is deliberately trivial: the point is that the unit-test
	// target compiles against the library package and runs under -race.
	if got := ekconfig.Version; got == "" {
		t.Fatal("ekconfig.Version is empty")
	}
}
