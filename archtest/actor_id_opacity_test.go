package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/storespec"
)

// TestDeclaredAdmissionIsTheOnlyDurableBirthVerb pins the narrowed public
// durable birth contract after the generic membership writer was removed.
func TestDeclaredAdmissionIsTheOnlyDurableBirthVerb(t *testing.T) {
	typ := reflect.TypeOf((*storespec.DeclAdmissionStore)(nil)).Elem()
	if typ.NumMethod() != 1 || typ.Method(0).Name != "AdmitDeclared" {
		t.Fatalf("DeclAdmissionStore methods changed: %v", typ)
	}
}

// TestAppDoesNotMintOrParseActorIDs keeps substrate-minted instance ids opaque
// at the application layer. Historical SQL migration literals are data repair,
// not runtime authority, and are intentionally outside this Go-expression wall.
func TestAppDoesNotMintOrParseActorIDs(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir("../app", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BinaryExpr:
				if x.Op == token.ADD && actorPrefixLiteral(x.X) {
					violations = append(violations, fmt.Sprintf("%s constructs an actor id from a prefix", fset.Position(x.Pos())))
				}
			case *ast.CallExpr:
				sel, ok := x.Fun.(*ast.SelectorExpr)
				if ok && (sel.Sel.Name == "Split" || sel.Sel.Name == "SplitN" || sel.Sel.Name == "TrimPrefix" || sel.Sel.Name == "Cut") {
					for _, arg := range x.Args {
						if actorPrefixLiteral(arg) {
							violations = append(violations, fmt.Sprintf("%s parses an actor id segment", fset.Position(x.Pos())))
						}
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("app must treat actor ids as opaque:\n%s", strings.Join(violations, "\n"))
	}
}

func actorPrefixLiteral(expr ast.Expr) bool {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	v, err := strconv.Unquote(lit.Value)
	return err == nil && (v == "agent:" || v == "human:" || v == "tool:" || v == "user:")
}
