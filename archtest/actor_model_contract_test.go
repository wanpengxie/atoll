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
	// The durable record verbs replaced AdmitDeclared/EndCascade outright; both
	// names must stay absent from the whole tree.
	allowedAdmit := map[string]bool{}
	allowedCascade := map[string]bool{}
	var admit, endCascade, rawCommit []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := path + ":" + fn.Name.Name
			rawCommitAllowed :=
				(path == "../runtime/accessdoor/completion.go" &&
					recvBaseTypeName(fn) == "resourceCompletion" &&
					fn.Name.Name == "CommitReservation") ||
					(path == "../runtime/accessdoor/query.go" &&
						recvBaseTypeName(fn) == "door" &&
						fn.Name.Name == "create")
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

// Owner has ONE home: the immutable genesis pointer. There is no Role field on
// any record, no owner column, and every owner judgement is derived at the
// Platform door from that pointer — never from a second account.
func TestChannelOwnerHasOnlyTheGenesisPointer(t *testing.T) {
	var roleAssignments, ownerDerivations, bootstrapAssignments []string
	walkProductionGo(t, func(path string, file *ast.File, _ *token.FileSet) {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				value, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
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
				return true
			})
			if strings.Contains(fn.Name.Name, "isOwner") ||
				strings.Contains(fn.Name.Name, "guardOwnerTerminal") {
				ownerDerivations = append(ownerDerivations, path+":"+fn.Name.Name)
			}
		}
	})
	wantOwner := []string{
		"../platform/home/owner.go:guardOwnerTerminal",
		"../platform/home/owner.go:isOwner",
	}
	wantBootstrap := []string{"../platform/channelhost/channelhost.go:openHome"}
	sort.Strings(ownerDerivations)
	sort.Strings(bootstrapAssignments)
	if len(roleAssignments) != 0 {
		t.Fatalf("a Role field reappeared on the actor record: %v", roleAssignments)
	}
	if !reflect.DeepEqual(ownerDerivations, wantOwner) ||
		!reflect.DeepEqual(bootstrapAssignments, wantBootstrap) {
		t.Fatalf("channel-owner chokepoint drift: owner=%v want %v; Bootstrap=%v want %v",
			ownerDerivations, wantOwner, bootstrapAssignments, wantBootstrap)
	}
}

func TestActorModelDataFlowChokepoints(t *testing.T) {
	var mintState, fireAndMark, markFired, ackTimer []string
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
				fireAndMark = append(fireAndMark, at)
			case "MarkFired":
				if !(path == "../runtime/schedule/engine.go" && enclosingFunc(file, call.Pos()) == "fireDue") {
					markFired = append(markFired, at)
				}
			case "AckTimer":
				ackTimer = append(ackTimer, at)
			}
			return true
		})
	})
	if len(mintState)+len(fireAndMark)+len(markFired)+len(ackTimer) != 0 {
		t.Fatalf("actor-model data-flow drift: MintState=%v FireAndMark=%v MarkFired=%v AckTimer=%v",
			mintState, fireAndMark, markFired, ackTimer)
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
