package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// actorcaps names capabilities and their data contracts. Implementations belong
// to their owning layers; runtime must not implement an application projection
// merely because its capability interface lives here.
func TestActorCapsContainsContractsOnly(t *testing.T) {
	for _, source := range productionFiles(t) {
		if source.dir != "runtime/actorcaps" {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join("..", source.path), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if node.Body != nil {
					t.Errorf("%s: actorcaps holds contracts, not implementations", fset.Position(node.Pos()))
				}
			case *ast.FuncLit:
				t.Errorf("%s: actorcaps holds contracts, not implementations", fset.Position(node.Pos()))
			}
			return true
		})
	}
}
