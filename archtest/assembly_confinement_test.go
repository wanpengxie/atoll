package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// platformPathPrefix is the allowlist: only the platform tree (the channel-home
// and attached-compute assembly layer) may reach for the base-layer assembly
// surfaces guarded below. Path-based so it survives batch A (link/host/tap
// sinking to platform/internal/ stay under this prefix).
const platformPathPrefix = "../platform/"

// TestRuntimeAssemblyConfinedToPlatform — PACKAGE-level lock.
//
// The runtime ROOT package (github.com/wanpengxie/atoll/runtime) is the public
// Store-organ facade. OpenChannel hands back a ChannelStores whose .Log is the raw
// MessageLog (Append writes the messages table directly) and whose .Membership
// mutates the actor_registry projection — both bypass the harness write gate
// and the actor composition root. Because the package exposes raw store ports, the lock is at
// package granularity: nobody outside platform may import it at all. Everyone
// else writes truth through harness.Pen (the seam), never cs.Log.Append.
//
// (Subpackages runtime/actorrt, runtime/harness, runtime/ipc, runtime/storespec
// are NOT locked here — they carry legitimate downstream seams. Only the root
// store facade is confined.)
func TestRuntimeAssemblyConfinedToPlatform(t *testing.T) {
	const runtimeRoot = platformModulePrefix + "runtime"

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
		if strings.HasPrefix(slash, "../platform/home/") {
			return nil // Home = the sole channel composition root
		}
		file, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		for _, imp := range file.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				continue
			}
			if p == runtimeRoot {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — the channel-store assembly facade may only be assembled by platform; downstream writes truth through harness.Pen, never cs.Log.Append / cs.Membership directly", slash, p))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("runtime store-facade confinement (only platform/home may import package runtime):\n  %s",
			strings.Join(violations, "\n  "))
	}
}

// TestRuntimeRootOpensStoreOnly prevents the Store facade from regrowing into
// a second composition root. Access, Scheduler, actor control and execution
// organs are assembled as peers by Platform/Home.
func TestRuntimeRootOpensStoreOnly(t *testing.T) {
	forbidden := map[string]bool{
		platformModulePrefix + "runtime/accessdoor": true,
		platformModulePrefix + "runtime/actorctl":   true,
		platformModulePrefix + "runtime/actorhost":  true,
		platformModulePrefix + "runtime/harness":    true,
		platformModulePrefix + "runtime/schedule":   true,
	}
	fset := token.NewFileSet()
	var violations []string
	entries, err := filepath.Glob("../runtime/*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		for _, imp := range importsOf(t, fset, path) {
			if forbidden[imp] {
				violations = append(violations, fmt.Sprintf("%s imports %q", path, imp))
			}
		}
	}
	if len(violations) != 0 {
		t.Fatalf("runtime root assembled peer organs:\n  %s", strings.Join(violations, "\n  "))
	}
}

// harnessConstructionSymbols are the write-gate construction + mint surface:
//
//   - New builds the bare write chain from Deps. Referencing it (or Deps)
//     assembles a write gate WITHOUT the platform wiring (commit signal at the
//     store append chokepoint, closure reconciler, DownWatcher) — a
//     half-wired home.
//   - Chain is the un-welded bare writer. It is now unexported inside harness
//     (the seam exports opaque Pen / Minter only), so this entry is a DEAD lock
//     kept as a tripwire: should Chain ever be re-exported, the wall re-arms.
//   - Minter is the Minter (Mint(actorID, chID) Pen) — the single most dangerous
//     capability in the system: holding it = minting a pen for ANY identity
//     (impersonate anyone), strictly worse than a factory stashing its own
//     welded pen (which can only impersonate itself). The seam must therefore
//     confine the TYPE reference (field / param / return / var) to platform, one
//     layer earlier than locking the .Mint() call site. Pen is deliberately NOT
//     locked — every actor legitimately holds a welded Pen.
var harnessConstructionSymbols = map[string]bool{"New": true, "Deps": true, "Chain": true, "Minter": true}

// TestHarnessConstructionConfinedToPlatform — SYMBOL-level lock.
//
// Unlike the runtime root, package harness is MIXED: it exports the construction
// + mint surface (New/Deps/Chain/Minter) AND the write contract the whole system
// speaks (Pen / WriteResult / the reject reasons — many legitimate downstream
// references). So the package CANNOT be banned wholesale; the lock is at symbol
// granularity — only platform may reference harness.New/Deps/Chain/Minter, while
// the contract seam (opaque Pen + WriteResult + reject reasons) stays freely
// importable. (Pen is held by every actor; the bare chain / caller ctx setter
// are unexported inside harness, so they cannot leak and need no lock here.)
//
// Lock granularity follows leak granularity: a wholly-assembly package (runtime
// root) is banned by import; an assembly+contract package (harness) is banned by
// symbol.
//
// Mechanism: resolve each file's local name for the runtime/harness import
// (default "harness" or its alias), then walk for selector expressions
// <local>.New / .Deps / .Chain / .Minter. A type reference (Minter as a field /
// param / return / var type) is itself a SelectorExpr, so the same walk catches
// it. An import path is a mandatory string literal, so the import resolution is
// exact; the residual evasion (shadowing the package name with a local var, or a
// dot-import) is a review-layer offence, not worth a go/types-grade pass
// pre-launch — same tripwire stance as the rest of this package.
func TestHarnessConstructionConfinedToPlatform(t *testing.T) {
	const harnessPkg = platformModulePrefix + "runtime/harness"

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
		if strings.HasPrefix(slash, platformPathPrefix) ||
			strings.HasPrefix(slash, "../runtime/managedcaps/") ||
			strings.HasPrefix(slash, "../runtime/systemcaps/") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}

		// Local name this file binds runtime/harness to (alias or default).
		local := ""
		for _, imp := range file.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil || p != harnessPkg {
				continue
			}
			if imp.Name != nil {
				local = imp.Name.Name
			} else {
				local = "harness"
			}
			break
		}
		if local == "" || local == "_" || local == "." {
			return nil // not imported under a qualifier we can match
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != local {
				return true
			}
			if harnessConstructionSymbols[sel.Sel.Name] {
				pos := fset.Position(sel.Pos())
				violations = append(violations, fmt.Sprintf(
					"%s:%d references %s.%s — the write-gate construction + Minter may only be assembled by platform; downstream speaks the opaque harness.Pen/WriteResult seam, never builds the chain itself (no commit signal / reconciler / death-edge closure) nor holds a Minter (= minting a pen for any identity)",
					slash, pos.Line, local, sel.Sel.Name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("harness construction confinement (only platform may reference harness.New/Deps/Chain/Minter):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
