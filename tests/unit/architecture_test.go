package unit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Structural rules over the module's own source, checked by parsing it rather
// than by exercising behaviour. They cover invariants that no behavioural test
// can reach: that something is absent everywhere, and that a dependency runs in
// only one direction.
//
// The scans deliberately cover the whole module, not one package, so a rule
// keeps applying to code added later.

const (
	modulePath     = "github.com/easykafka/easykafka-config-go"
	driverPkgPath  = "internal/driver"
	testsPath      = "tests/"
	kafkaImportPkg = "github.com/confluentinc/confluent-kafka-go"
)

// forbiddenCalls are driver methods that would commit or stage offsets. Names
// only: the check is syntactic, so it catches the call regardless of receiver.
var forbiddenCalls = map[string]string{
	"Commit":        "commits the current offsets",
	"CommitOffsets": "commits explicit offsets",
	"CommitMessage": "commits a message's offset",
	"StoreOffsets":  "stores offsets for a later commit",
	"StoreMessage":  "stores a message's offset for a later commit",
}

// findForbiddenCalls reports the forbidden method names called in a file, in
// source order. Split out from the repository scan below so it can be tested on
// synthetic input — a guard that cannot fail is worse than no guard.
func findForbiddenCalls(file *ast.File) []string {
	var found []string

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			if _, bad := forbiddenCalls[sel.Sel.Name]; bad {
				found = append(found, sel.Sel.Name)
			}
		}

		return true
	})

	return found
}

// findImports reports every import path in a file.
func findImports(file *ast.File) []string {
	paths := make([]string, 0, len(file.Imports))
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		paths = append(paths, path)
	}

	return paths
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := filepath.Abs(".")
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "walked past the filesystem root without finding go.mod")
		dir = parent
	}
}

// eachGoFile parses every non-test .go file in the module and calls fn with its
// module-relative path and AST.
func eachGoFile(t *testing.T, fn func(relPath string, file *ast.File)) {
	t.Helper()

	root := repoRoot(t)
	fset := token.NewFileSet()
	visited := 0

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip the pinned tool binaries and anything version-control owned.
			if name := d.Name(); name == "bin" || name == ".git" {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsed, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		visited++
		fn(filepath.ToSlash(rel), parsed)

		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, visited, "no source files were scanned, so the guard proved nothing")
}

// Offsets must never be committed. This is load-bearing twice over: it is what
// makes a restart re-read the whole topic and rebuild an identical store, and
// what makes one inert group id safe to share across replicas — with no
// committed offsets there is no per-(group, topic, partition) state for two
// replicas to overwrite.
//
// A committed offset would not fail loudly; it would silently truncate
// configuration after a restart. Hence a structural guard rather than a
// comment: this scans the source, so it catches a commit added anywhere in the
// library, including in code paths no other test covers.
func TestNoOffsetCommitAnywhere(t *testing.T) {
	t.Parallel()

	eachGoFile(t, func(relPath string, file *ast.File) {
		for _, name := range findForbiddenCalls(file) {
			assert.Failf(t, "forbidden call",
				"%s calls %s(), which %s; this library must never commit offsets",
				relPath, name, forbiddenCalls[name])
		}
	})
}

// Proves the guard above can actually fail. Without this, deleting the detector
// body would leave a green test suite and a silent invariant.
func TestOffsetCommitGuardDetectsViolations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		src  string
		want []string
	}{
		"clean": {
			src: `package p
func f(c consumer) { c.Poll(100) }`,
			want: nil,
		},
		"commit": {
			src: `package p
func f(c consumer) { _, _ = c.Commit() }`,
			want: []string{"Commit"},
		},
		"commit offsets": {
			src: `package p
func f(c consumer) { _, _ = c.CommitOffsets(nil) }`,
			want: []string{"CommitOffsets"},
		},
		"store offsets": {
			src: `package p
func f(c consumer) { _, _ = c.StoreOffsets(nil) }`,
			want: []string{"StoreOffsets"},
		},
		"nested in a closure": {
			src: `package p
func f(c consumer) { go func() { _, _ = c.CommitMessage(nil) }() }`,
			want: []string{"CommitMessage"},
		},
		"several": {
			src: `package p
func f(c consumer) { _, _ = c.Commit(); _, _ = c.StoreMessage(nil) }`,
			want: []string{"Commit", "StoreMessage"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			file, err := parser.ParseFile(token.NewFileSet(), "probe.go", tc.src, parser.SkipObjectResolution)
			require.NoError(t, err)

			assert.Equal(t, tc.want, findForbiddenCalls(file))
		})
	}
}

// Within the library, confluent-kafka-go must be reachable from exactly one
// package. Keeping the driver surface to a single package is what makes it
// auditable and swappable; once kafka types leak into the layers above, "the
// only package that talks to librdkafka" stops being true and those layers can
// no longer be tested without a broker.
//
// Test support code under tests/ is exempt: the integration suite has to
// produce records and manage topics on a real broker, which means using the
// client directly. That is test infrastructure standing in for a producer, not
// the library reaching around its own driver layer.
func TestKafkaDriverIsImportedOnlyByDriverPackage(t *testing.T) {
	t.Parallel()

	var libraryImporters, testImporters []string

	eachGoFile(t, func(relPath string, file *ast.File) {
		for _, path := range findImports(file) {
			if !strings.HasPrefix(path, kafkaImportPkg) {
				continue
			}
			if strings.HasPrefix(relPath, testsPath) {
				testImporters = append(testImporters, relPath)
			} else {
				libraryImporters = append(libraryImporters, relPath)
			}

			break
		}
	})

	require.NotEmpty(t, libraryImporters,
		"no library file imports the kafka client — has the driver package moved?")

	for _, path := range libraryImporters {
		assert.True(t, strings.HasPrefix(path, driverPkgPath+"/"),
			"%s imports %s; within the library only %s/ may do so", path, kafkaImportPkg, driverPkgPath)
	}

	// Recorded rather than asserted: the exemption is deliberate, and printing
	// the list keeps it visible if it ever grows beyond the suite's helpers.
	if len(testImporters) > 0 {
		t.Logf("test support importing the kafka client directly (exempt): %v", testImporters)
	}
}

// The driver package must not import the library's public API, so the
// dependency runs one way only: the API depends on the driver, never the
// reverse.
func TestDriverDoesNotImportPublicAPI(t *testing.T) {
	t.Parallel()

	eachGoFile(t, func(relPath string, file *ast.File) {
		if !strings.HasPrefix(relPath, driverPkgPath+"/") {
			return
		}

		for _, path := range findImports(file) {
			assert.NotEqual(t, modulePath, path,
				"%s imports the public API, creating a cycle in the intended layering", relPath)
		}
	})
}
