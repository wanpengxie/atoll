package archtest

import (
	"fmt"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The time-axis (timer) purity wall. Only one lock is load-bearing here —
// mirroring the resourcespec precedent, not the accessdoor one:
//
//   - timerspec is the kernel-only durable-pending-timer CONTRACT leaf (dual
//     to resourcespec on the object plane, storespec on the message plane). If
//     domain code could import it, it could implement its own TimerStore /
//     construct a raw pending-timer table reachable outside the engine — a
//     delayed forged-author write path around the pen. The confinement is
//     enforced at the compile layer, not by convention.
//
//   - runtime/schedule is deliberately NOT walled the same way: it is the
//     downstream-facing surface (ScheduleHandle / Minter), the schedule-package
//     analogue of accessdoor — platform is meant to import it. Only timerspec
//     is sealed; schedule stays open to downstream. It only gets a narrower
//     containment check below (no reach into the concrete store or the
//     platform assembly it must stay below).
//
// Like the rest of this package these are structural boundaries, not drift
// tripwires: an import path is a mandatory string literal, so the AST sees
// every edge and there is no computed-import escape hatch.

// timerspecPkg / schedulePkg are the time-axis packages these walls reason
// about. (internalStorePkg / platformPkg are declared in access_purity_test.go,
// reused here.)
const (
	timerspecPkg = platformModulePrefix + "runtime/timerspec"
	schedulePkg  = platformModulePrefix + "runtime/schedule"
)

// TestTimerspecImportedOnlyWithinRuntime — timerspec (the kernel-only durable
// pending-timer contract leaf) may be imported ONLY from inside the runtime
// tree. A downstream importer could implement TimerStore / construct a raw
// pending table directly, opening a delayed forged-author write path around
// the pen. The closed store-implementor set is thereby closed by the
// compile layer: downstream sees only schedule.ScheduleHandle / the welded
// Minter, never the raw TimerStore.
func TestTimerspecImportedOnlyWithinRuntime(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
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
		if strings.HasPrefix(slash, runtimeTreePrefix) {
			return nil // within the runtime tree — the legitimate implementor
		}
		for _, imp := range importsOf(t, fset, path) {
			if imp == timerspecPkg || strings.HasPrefix(imp, timerspecPkg+"/") {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — timerspec is the kernel-only durable pending-timer contract; only the runtime tree may import it (downstream speaks schedule.ScheduleHandle / the welded Minter, never the raw TimerStore)", slash, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("timerspec import confinement (runtime tree only):\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestScheduleImportContainment — the schedule engine package may not import
// the concrete store or the platform assembly. It speaks the timerspec
// contract only; a concrete-store edge would fuse it to sqlite, a platform
// edge would let it back-flow into the layer that is meant to assemble it.
func TestScheduleImportContainment(t *testing.T) {
	forbidden := map[string]string{
		internalStorePkg: "the concrete store (the engine speaks the timerspec contract, never sqlite)",
		platformPkg:      "the platform assembly (the engine must stay below it)",
	}

	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime/schedule", func(path string, d fs.DirEntry, err error) error {
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
		for _, imp := range importsOf(t, fset, path) {
			for pkg, why := range forbidden {
				if imp == pkg || strings.HasPrefix(imp, pkg+"/") {
					violations = append(violations, fmt.Sprintf("%s imports %q — %s", slash, imp, why))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("schedule import containment (no store / platform):\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestTimerspecDoesNotImportStore — the contract leaf must not import its own
// concrete implementor (mirrors resourcespec ⊄ internal/store): a contract
// that reached down to sqlite would invert the leaf relationship and drag the
// store backend into every contract consumer, including runtime/schedule.
func TestTimerspecDoesNotImportStore(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime/timerspec", func(path string, d fs.DirEntry, err error) error {
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
		for _, imp := range importsOf(t, fset, path) {
			if imp == internalStorePkg || strings.HasPrefix(imp, internalStorePkg+"/") {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — timerspec is a contract leaf and must not import its concrete store implementor", slash, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("timerspec ⊄ internal/store:\n  %s", strings.Join(violations, "\n  "))
	}
}
