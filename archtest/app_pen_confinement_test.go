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

// TestAppTreeHasNoHarnessPen — the WRITE-GATE confinement, tightened one notch by
// the subjectgate door (S4). A person writes truth ONLY through the door's welded
// pen (Home.Human → HumanHandle), which lives INSIDE the wall; the app never holds
// a harness.Pen. The old humanFront (the last app-side pen holder) was整删 with
// the door, so the app tree's NON-TEST source must carry zero harness.Pen type
// references — any reappearance (an app handler reaching for a raw pen to write
// truth directly, bypassing the door + its户籍校验) turns this red.
//
// Scope: NON-TEST source only (!_test.go). Live tests may mint actor doubles;
// those are test fixtures, not production write paths, and are listed in the S4
// cleanup ledger — the compile-time wall is over what ships.
func TestAppTreeHasNoHarnessPen(t *testing.T) {
	const harnessPkg = platformModulePrefix + "runtime/harness"

	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir("../app", func(path string, d fs.DirEntry, err error) error {
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
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}

		// Resolve this file's local name for the runtime/harness import.
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
			return nil // harness not imported under a matchable qualifier
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != local || sel.Sel.Name != "Pen" {
				return true
			}
			pos := fset.Position(sel.Pos())
			violations = append(violations, fmt.Sprintf(
				"%s:%d references %s.Pen — the app holds no write pen; a subject writes truth only through the subjectgate door (Home.Human → HumanHandle), never an app-held harness.Pen",
				slash, pos.Line, local))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	failViolations(t, "app tree ⊥ harness.Pen (non-test source; door-only writes)", violations)
}
