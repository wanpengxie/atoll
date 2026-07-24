package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func recvBaseTypeName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	expr := fn.Recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func walkProductionGo(t *testing.T, fn func(path string, f *ast.File, fset *token.FileSet)) {
	t.Helper()
	fset := token.NewFileSet()
	for _, root := range []string{"../app", "../lib", "../platform", "../runtime"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			fn(filepath.ToSlash(path), f, fset)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func enclosingFunc(f *ast.File, pos token.Pos) string {
	for _, declaration := range f.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Pos() <= pos && pos <= fn.End() {
			return fn.Name.Name
		}
	}
	return ""
}

func TestActorModelBundleCallsitesAreClosed(t *testing.T) {
	allowedAdmit := map[string]bool{
		"../platform/home/actor_store.go:Admit":              true,
		"../platform/home/open.go:Open":                      true,
		"../platform/home/open.go:seedBootstrap":             true,
		"../platform/home/open.go:admitBootstrapDeclaration": true,
	}
	allowedCascade := map[string]bool{
		"../platform/home/actor_store.go:CommitTerminal": true,
	}
	var admit, endCascade, rawCommit []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := path + ":" + fn.Name.Name
			rawCommitAllowed := path == "../runtime/accessdoor/overlay.go" &&
				recvBaseTypeName(fn) == "door" && fn.Name.Name == "commitReservationLocked"
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				at := fset.Position(call.Pos()).String()
				switch selector.Sel.Name {
				case "AdmitDeclared":
					if !allowedAdmit[key] {
						admit = append(admit, at)
					}
				case "EndCascade":
					if !allowedCascade[key] {
						endCascade = append(endCascade, at)
					}
				case "CommitReservation":
					receiver, ok := selector.X.(*ast.SelectorExpr)
					if ok && receiver.Sel.Name == "Registry" && !rawCommitAllowed {
						rawCommit = append(rawCommit, at)
					}
				}
				return true
			})
		}
	})
	if len(admit)+len(endCascade)+len(rawCommit) != 0 {
		t.Fatalf("bundle callsite drift: AdmitDeclared=%v EndCascade=%v CommitReservation(raw)=%v",
			admit, endCascade, rawCommit)
	}
}

func TestChannelOwnerProductionChokepointsAreClosed(t *testing.T) {
	var roleAssignments, protectedReturns, bootstrapAssignments []string
	walkProductionGo(t, func(path string, file *ast.File, _ *token.FileSet) {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.CompositeLit:
					for _, element := range value.Elts {
						field, ok := element.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						name, ok := field.Key.(*ast.Ident)
						if !ok {
							continue
						}
						switch name.Name {
						case "Role":
							roleAssignments = append(roleAssignments, path+":"+fn.Name.Name)
						case "Bootstrap":
							bootstrapAssignments = append(bootstrapAssignments, path+":"+fn.Name.Name)
						}
					}
				case *ast.ReturnStmt:
					for _, result := range value.Results {
						selector, ok := result.(*ast.SelectorExpr)
						if ok && selector.Sel.Name == "ErrChannelOwnerProtected" {
							protectedReturns = append(protectedReturns, path+":"+fn.Name.Name)
						}
					}
				}
				return true
			})
		}
	})
	wantRole := []string{
		"../platform/home/actor_store.go:Admit",
		"../platform/home/census.go:admitChannelOwner",
		"../platform/home/open.go:seedBootstrap",
		"../runtime/actorctl/types.go:definitionFromStored",
		"../runtime/actorctl/types.go:rowFromActive",
	}
	wantProtected := []string{
		"../platform/home/actor_store.go:ResolveTerminal",
		"../runtime/internal/store/cascade.go:EndCascade",
	}
	wantBootstrap := []string{"../platform/channelhost/channelhost.go:openHome"}
	sort.Strings(roleAssignments)
	sort.Strings(protectedReturns)
	sort.Strings(bootstrapAssignments)
	sort.Strings(wantRole)
	sort.Strings(wantProtected)
	if !reflect.DeepEqual(roleAssignments, wantRole) ||
		!reflect.DeepEqual(protectedReturns, wantProtected) ||
		!reflect.DeepEqual(bootstrapAssignments, wantBootstrap) {
		t.Fatalf("channel-owner chokepoint drift: Role=%v want %v; protected=%v want %v; Bootstrap=%v want %v",
			roleAssignments, wantRole, protectedReturns, wantProtected, bootstrapAssignments, wantBootstrap)
	}
}

func TestActorModelAuthorityHasNoInlineVersionFallback(t *testing.T) {
	var inlineVersionCompare []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			expression, ok := node.(*ast.BinaryExpr)
			if !ok {
				return true
			}
			if path == "../platform/home/readface.go" && enclosingFunc(file, expression.Pos()) == "CheckAuthor" {
				return true
			}
			names := map[string]bool{}
			ast.Inspect(expression, func(child ast.Node) bool {
				if selector, ok := child.(*ast.SelectorExpr); ok {
					names[selector.Sel.Name] = true
				}
				return true
			})
			if names["BirthVersion"] && names["CurrentDeclVersion"] {
				inlineVersionCompare = append(inlineVersionCompare, fset.Position(expression.Pos()).String())
			}
			return true
		})
	})
	if len(inlineVersionCompare) != 0 {
		t.Fatalf("inline author-version comparisons escaped CheckAuthor: %v", inlineVersionCompare)
	}
}

func TestActorModelDataFlowChokepoints(t *testing.T) {
	var mintState, fireAndMark, ackTimer []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fset.Position(call.Pos()).String()
			switch selector.Sel.Name {
			case "MintState":
				allowed := path == "../platform/home/open.go" && enclosingFunc(file, call.Pos()) == "Open"
				allowed = allowed || path == "../runtime/accessdoor/memstate.go" &&
					enclosingFunc(file, call.Pos()) == "Resolve"
				if !allowed {
					mintState = append(mintState, at)
				}
			case "FireAndMark":
				if !(path == "../runtime/schedule/firepen.go" && enclosingFunc(file, call.Pos()) == "Fire") {
					fireAndMark = append(fireAndMark, at)
				}
			case "AckTimer":
				ackTimer = append(ackTimer, at)
			}
			return true
		})
	})
	if len(mintState)+len(fireAndMark)+len(ackTimer) != 0 {
		t.Fatalf("actor-model data-flow drift: MintState=%v FireAndMark=%v AckTimer=%v",
			mintState, fireAndMark, ackTimer)
	}
}

func TestRetiredHomeLivenessOwnerIsAbsent(t *testing.T) {
	retiredType := "liveness" + "Ledger"
	var declarations []string
	walkProductionGo(t, func(_ string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if ok && spec.Name.Name == retiredType {
				declarations = append(declarations, fset.Position(spec.Pos()).String())
			}
			return true
		})
	})
	if len(declarations) != 0 {
		t.Fatalf("retired Home liveness owner returned: %v", declarations)
	}
}
