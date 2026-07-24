package archtest

import (
	"fmt"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The plane-2 (access / resource) purity walls. Three import-direction locks
// that the type system alone cannot enforce, mirroring the storespec / harness
// confinement discipline:
//
//   - resourcespec is the kernel-only R + driver CONTRACT leaf. If domain code
//     could import it, it could implement its own Driver / build a DriverTable /
//     construct a raw Registry — an open plane-2 extension point and a
//     bypass-the-door write surface. The closed-but-additive driver set stays
//     closed by the COMPILE layer, not by convention: only the runtime tree may
//     import resourcespec.
//   - accessdoor is the door. It speaks proto + resourcespec only; reaching for
//     the concrete store, the platform assembly, or the fat-daemon registry
//     would fuse the door to an implementation and let it back-flow into layers
//     it must stay above.
//
// Like the rest of this package these are structural boundaries, not drift
// tripwires: an import path is a mandatory string literal, so the AST sees every
// edge and there is no computed-import escape hatch.

// runtimeTreePrefix is the runtime subtree, relative to the archtest walk root
// ("..").  A file under it is "within the runtime tree".
const runtimeTreePrefix = "../runtime/"

// resourcespecPkg / accessdoorPkg / internalStorePkg / registryPkg are the
// plane-2 packages these walls reason about.
// (registryPkg is declared in agent_layering_test.go, reused here.)
const (
	resourcespecPkg  = platformModulePrefix + "runtime/resourcespec"
	internalStorePkg = platformModulePrefix + "runtime/internal/store"
	platformPkg      = platformModulePrefix + "platform"
)

// TestResourcespecImportedOnlyWithinRuntime — resourcespec (the kernel-only R +
// driver contract leaf) may be imported by Runtime implementations and the
// Platform/Home composition root only. Platform needs the raw leaf ports once,
// to assemble peer Runtime organs; no other downstream package may receive
// them or build a bypass around AccessDoor.
func TestResourcespecImportedOnlyWithinRuntime(t *testing.T) {
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
		if strings.HasPrefix(slash, runtimeTreePrefix) ||
			strings.HasPrefix(slash, "../platform/home/") {
			return nil // implementation or the sole composition root
		}
		for _, imp := range importsOf(t, fset, path) {
			if imp == resourcespecPkg || strings.HasPrefix(imp, resourcespecPkg+"/") {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — resourcespec is confined to Runtime implementations and Platform/Home assembly", slash, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("resourcespec import confinement:\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestAccessdoorImportContainment — the door may not import the concrete store,
// the platform assembly, or the fat-daemon registry. It speaks proto +
// resourcespec (contracts) only; a concrete-store edge would fuse it to sqlite,
// a platform / registry edge would let the door back-flow into layers above it.
func TestAccessdoorImportContainment(t *testing.T) {
	forbidden := map[string]string{
		internalStorePkg: "the concrete store (the door speaks the resourcespec contract, never sqlite)",
		platformPkg:      "the platform assembly (the door must stay below it)",
		registryPkg:      "the fat-daemon registry (a downstream self-registration point)",
	}

	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime/accessdoor", func(path string, d fs.DirEntry, err error) error {
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
		t.Fatalf("accessdoor import containment (no store / platform / registry):\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestResourcespecDoesNotImportStore — the contract leaf must not import its own
// concrete implementor (mirrors storespec ⊄ internal/store): a contract that
// reached down to sqlite would invert the leaf relationship and drag the driver
// backend into every contract consumer.
func TestResourcespecDoesNotImportStore(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime/resourcespec", func(path string, d fs.DirEntry, err error) error {
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
					"%s imports %q — resourcespec is a contract leaf and must not import its concrete store implementor", slash, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("resourcespec ⊄ internal/store:\n  %s", strings.Join(violations, "\n  "))
	}
}
