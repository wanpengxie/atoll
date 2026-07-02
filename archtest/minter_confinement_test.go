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

// Capability-minter TYPE confinement — the plane-2 / time-axis twins of the
// harness.Minter entry in TestHarnessConstructionConfinedToPlatform.
//
// harness.Minter is symbol-locked because holding a minter = minting a welded
// capability for ANY identity (impersonate anyone). The other two planes
// export the SAME capability class, previously without the lock:
//
//   - accessdoor.AccessMinter — Mint(caller) hands back a caller-welded
//     AccessHandle: holding the minter = acting on plane-2 (resources, grants,
//     private state) as any actor.
//   - schedule.Minter — Mint(author) hands back an author-welded
//     ScheduleHandle: a fire ultimately appends truth AS that author through
//     the FireSink-minted pen, so holding the minter = a delayed forged-author
//     write path (the legalised twin of 红线❹❻'s raw TimerStore).
//   - schedule.Engine rides along: its free-author faces are unexported
//     (engine.schedule/cancel — the un-welded twin of harness's bare chain),
//     so what remains outward is Start/Close, the run-loop lifecycle. That is
//     an assembly-root responsibility (synchronised with the channel's own
//     open/close): a downstream *Engine could stop the channel's time axis.
//
// The VALUES are already confined — all three are born inside OpenChannel /
// OpenScheduler, and package runtime is import-locked to platform
// (TestRuntimeAssemblyConfinedToPlatform). This wall adds the TYPE-reference
// lock, one layer earlier: without it, a downstream field/param typed
// accessdoor.AccessMinter compiles and passes CI, and one "convenient"
// platform hand-off later the mint-for-anyone capability has escaped. With
// it, the hand-off target cannot even be declared.
//
// Lock granularity follows leak granularity (assembly_confinement 同款): both
// packages export legitimate downstream contracts (AccessHandle / Outcome /
// ScheduleHandle / ScheduleReq / FireSink …), so the ban is per-symbol, not
// per-import. Construction faces (accessdoor.New / schedule.New and their
// Deps) are deliberately NOT locked: they are inert downstream — a useful
// Deps needs resourcespec / timerspec values whose contract leaves are
// import-confined to the runtime tree, and a nil-dep Deps is rejected by
// New's fail-fast — so locking them would ban nothing dangerous (拒绝集 =
// 真危险集, 一个不多).
//
// Allowlist: the runtime ROOT package only (OpenChannel / OpenScheduler
// produce these values, so their signatures name the types — no runtime
// SIBLING package has any business holding a minter: harness/actorrt/
// accessdoor/schedule reference their own symbols unqualified, which this
// selector walk never matches) and the platform tree (the assembler that
// receives and holds them). Same shadowing / dot-import caveat as the
// harness lock: a review-layer offence, not worth a go/types-grade pass
// pre-launch.
var minterConfinements = []struct {
	pkg          string // import path of the guarded package
	defaultLocal string // qualifier when the import carries no alias
	symbols      map[string]bool
	why          string
}{
	{
		pkg:          platformModulePrefix + "runtime/accessdoor",
		defaultLocal: "accessdoor",
		symbols:      map[string]bool{"AccessMinter": true},
		why:          "holding an AccessMinter = minting a caller-welded AccessHandle for ANY identity (plane-2 impersonation); downstream holds its own welded AccessHandle, never the minter",
	},
	{
		pkg:          schedulePkg,
		defaultLocal: "schedule",
		symbols:      map[string]bool{"Minter": true, "Engine": true},
		why:          "holding a schedule.Minter = authoring delayed truth AS anyone through the FireSink-minted pen (红线❹❻), and holding *Engine = the channel's time-axis lifecycle (Start/Close); downstream holds its own welded ScheduleHandle, never the minter/engine",
	},
}

// TestMinterTypeConfinement — SYMBOL-level lock (see the block comment above).
// Only the runtime and platform trees may reference the minter/engine types of
// the plane-2 and time-axis capability mints.
func TestMinterTypeConfinement(t *testing.T) {
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
		if isRuntimeRootFile(slash) || strings.HasPrefix(slash, platformPathPrefix) {
			return nil // the legitimate producer (runtime root) / assembler (platform)
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}

		// Resolve this file's local qualifier for each guarded package.
		type guard struct {
			symbols map[string]bool
			why     string
		}
		locals := map[string]guard{}
		for _, imp := range file.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				continue
			}
			for _, mc := range minterConfinements {
				if p != mc.pkg {
					continue
				}
				local := mc.defaultLocal
				if imp.Name != nil {
					local = imp.Name.Name
				}
				if local == "_" || local == "." {
					continue // not matchable by qualifier; dot-import is a review-layer offence
				}
				locals[local] = guard{symbols: mc.symbols, why: mc.why}
			}
		}
		if len(locals) == 0 {
			return nil
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			g, ok := locals[x.Name]
			if !ok || !g.symbols[sel.Sel.Name] {
				return true
			}
			pos := fset.Position(sel.Pos())
			violations = append(violations, fmt.Sprintf(
				"%s:%d references %s.%s — %s", slash, pos.Line, x.Name, sel.Sel.Name, g.why))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("minter type confinement (only the runtime/platform trees may reference the capability mints):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
