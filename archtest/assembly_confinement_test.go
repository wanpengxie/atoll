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
// The runtime ROOT package (github.com/wanpengxie/ActOS/runtime) is PURE
// assembly: its entire export surface is ChannelStores / OpenChannelOptions /
// OpenChannel. OpenChannel hands back a ChannelStores whose .Log is the raw
// MessageLog (Append writes the messages table directly) and whose .Membership
// mutates the actor_registry projection — both BYPASS the harness 9-step write
// gate and Home.Spawn. Because the package is wholly assembly, the lock is at
// package granularity: nobody outside platform may import it at all. Everyone
// else writes truth through harness.Writer (the seam), never cs.Log.Append.
//
// (Subpackages runtime/actorrt, runtime/harness, runtime/ipc, runtime/storespec
// are NOT locked here — they carry legitimate downstream seams. Only the root
// assembly facade is confined.)
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
		if strings.HasPrefix(slash, platformPathPrefix) {
			return nil // platform = the legitimate assembler
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
					"%s imports %q — the channel-store assembly facade may only be assembled by platform; downstream writes truth through harness.Writer, never cs.Log.Append / cs.Membership directly", slash, p))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("runtime assembly confinement (only platform may import package runtime):\n  %s",
			strings.Join(violations, "\n  "))
	}
}

// harnessConstructionSymbols are the write-门 construction surface: New builds
// the 9-step Chain from Deps. Referencing them assembles a write gate WITHOUT
// the platform wiring (commit signal at the store append chokepoint, closure
// reconciler, PresenceWatcher) — a half-wired home.
var harnessConstructionSymbols = map[string]bool{"New": true, "Deps": true, "Chain": true}

// TestHarnessConstructionConfinedToPlatform — SYMBOL-level lock.
//
// Unlike the runtime root, package harness is MIXED: it exports the construction
// surface (New/Deps/Chain) AND the write contract the whole system speaks
// (Writer / WriteResult / CtxWithCaller / CallerContext / the reject reasons —
// 80+ legitimate downstream references). So the package CANNOT be banned
// wholesale; the lock is at symbol granularity — only platform may reference
// harness.New/Deps/Chain, while the contract seam stays freely importable.
//
// Lock granularity follows leak granularity: a wholly-assembly package (runtime
// root) is banned by import; an assembly+contract package (harness) is banned by
// symbol.
//
// Mechanism: resolve each file's local name for the runtime/harness import
// (default "harness" or its alias), then walk for selector expressions
// <local>.New / .Deps / .Chain. An import path is a mandatory string literal, so
// the import resolution is exact; the residual evasion (shadowing the package
// name with a local var, or a dot-import) is a review-layer offence, not worth a
// go/types-grade pass pre-launch — same tripwire stance as the rest of this
// package.
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
		if strings.HasPrefix(slash, platformPathPrefix) {
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
					"%s:%d references %s.%s — the write-门 construction may only be assembled by platform; downstream speaks the harness.Writer/WriteResult seam, never builds the 9-step chain itself (a chain built outside platform has no commit signal / reconciler / presence closure)",
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
		t.Fatalf("harness construction confinement (only platform may reference harness.New/Deps/Chain):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
