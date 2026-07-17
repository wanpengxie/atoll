package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestIPCKindClosedSetExhaustive is the AST-level guard the hand-maintained
// in-package runtime/ipc TestKindClosedSet map structurally CANNOT be: it scans
// EVERY `Kind`-typed const declared in package ipc and asserts the exact set.
//
// Why the in-package map cannot catch a regression: TestKindClosedSet checks a
// hand-written `want` map against its own len — a 17th Kind added to frame.go that
// the author forgot to add to the map is simply ABSENT from the map, so the map
// still has 16 entries and the test stays green. This test closes that hole by
// enforcing the closed set against the SOURCE OF TRUTH (the const declarations),
// not against a parallel hand-list: a new Kind const not acknowledged in `known`
// below turns THIS red.
func TestIPCKindClosedSetExhaustive(t *testing.T) {
	// The canonical closed set. Adding a Kind const to runtime/ipc
	// without adding it here is the intended tripwire (keep this in lockstep with
	// the in-package TestKindClosedSet wire-spelling map).
	known := map[string]bool{
		"KindHandshake": true, "KindHandshakeAck": true,
		"KindDeliver": true,
		"KindEmit":    true, "KindEmitAck": true,
		"KindDown":   true,
		"KindCancel": true,
		"KindObs":    true,
		"KindAccess": true, "KindAccessAck": true,
		"KindSchedule": true, "KindScheduleAck": true,
		"KindDetach": true, "KindDespawn": true,
		"KindDeliverResult": true,
		"KindCancelRequest": true,
		"KindSpawn":         true, "KindSpawnAck": true,
		"KindEnd": true, "KindEndAck": true,
		"KindIdle": true, "KindIdleAck": true,
	}

	fset := token.NewFileSet()
	found := map[string]string{} // const name -> file:line
	err := filepath.WalkDir("../runtime/ipc", func(path string, d fs.DirEntry, err error) error {
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
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				// Only Kind-typed consts are members of the wire set. A const that
				// omits the explicit `Kind` type is an untyped constant, not a member
				// of the closed set — so requiring the type ident here is the correct
				// semantic, not a leniency.
				id, ok := vs.Type.(*ast.Ident)
				if !ok || id.Name != "Kind" {
					continue
				}
				for _, name := range vs.Names {
					found[name.Name] = fmt.Sprintf("%s:%d", filepath.ToSlash(path), fset.Position(name.Pos()).Line)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk runtime/ipc: %v", err)
	}

	// Any declared Kind const not in the known set = an unaccounted-for wire kind
	// (the exact failure the in-package map test misses).
	var extra []string
	for name, at := range found {
		if !known[name] {
			extra = append(extra, fmt.Sprintf("%s (%s)", name, at))
		}
	}
	// Any known kind that vanished from the source = the closed set shrank silently.
	var missing []string
	for name := range known {
		if _, ok := found[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(extra) > 0 || len(missing) > 0 {
		t.Fatalf("runtime/ipc Kind closed-set drift (update the canonical set here AND the in-package TestKindClosedSet wire map together):\n  unaccounted new kinds: %s\n  missing known kinds: %s",
			strings.Join(extra, ", "), strings.Join(missing, ", "))
	}
	if len(found) != len(known) {
		t.Fatalf("expected exactly %d Kind consts, found %d: %v", len(known), len(found), found)
	}
}
