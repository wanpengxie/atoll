package archtest

import (
	"fmt"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// isRuntimeRootFile reports whether slash names a file directly in the
// runtime ROOT package (../runtime/*.go, no deeper path segment) — the pure
// assembly package, as opposed to a runtime SIBLING subpackage.
func isRuntimeRootFile(slash string) bool {
	if !strings.HasPrefix(slash, runtimeTreePrefix) {
		return false
	}
	return !strings.Contains(strings.TrimPrefix(slash, runtimeTreePrefix), "/")
}

// TestInternalStoreImportConfinedToRuntimeRoot — Go's internal/ rule already
// blocks everything OUTSIDE runtime/... from importing runtime/internal/store
// at compile time. This wall narrows the remaining width: WITHIN the runtime
// tree, only the ROOT assembly package (storeopen.go's OpenChannel is the one
// legitimate consumer) may import the concrete store. A runtime SIBLING
// (harness / actorrt / accessdoor / schedule) importing it could call
// store.OpenChannel(...).Log.Append and write truth around the harness gate —
// compile-legal, previously CI-green. The siblings speak the contract leaves
// (storespec / resourcespec / timerspec), never sqlite.
//
// (Test files are excluded as everywhere in this package — harness/_test
// fixtures legitimately open a real store.)
func TestInternalStoreImportConfinedToRuntimeRoot(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		slash := filepath.ToSlash(path)
		if isRuntimeRootFile(slash) {
			return nil // the assembly root — the one legitimate importer
		}
		if strings.HasPrefix(slash, "../runtime/internal/") {
			return nil // the store subtree itself
		}
		for _, imp := range importsOf(t, fset, path) {
			if imp == internalStorePkg || strings.HasPrefix(imp, internalStorePkg+"/") {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — only the runtime ROOT assembly package may reach the concrete store; siblings speak the contract leaves (storespec/resourcespec/timerspec), never sqlite (a raw Log.Append here bypasses the harness write gate)", slash, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk runtime: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("internal/store import confinement (runtime root only):\n  %s", strings.Join(violations, "\n  "))
	}
}
