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

func TestActorModelUniqueMutationAndConstructionChokepoints(t *testing.T) {
	forbiddenTypes := map[string]bool{"MembershipWriter": true, "MembershipControlPlane": true}
	forbiddenCalls := map[string]bool{
		"ApplyMemberTransitions": true, "EnsureSystemActor": true,
		"Deregister": true, "IntroduceComposition": true,
	}
	var violations []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.TypeSpec:
				if forbiddenTypes[x.Name.Name] {
					violations = append(violations, fmt.Sprintf("%s declares retired type %s", fset.Position(x.Pos()), x.Name.Name))
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok && forbiddenCalls[sel.Sel.Name] {
					violations = append(violations, fmt.Sprintf("%s calls retired verb %s", fset.Position(x.Pos()), sel.Sel.Name))
				}
			case *ast.CompositeLit:
				sel, ok := x.Type.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "PlanActor" && path != "../platform/home/plan.go" {
					violations = append(violations, fmt.Sprintf("%s constructs PlanActor outside PlanForDaemon", fset.Position(x.Pos())))
				}
			}
			return true
		})
	})
	if len(violations) > 0 {
		t.Fatalf("actor-model chokepoint violations:\n%s", strings.Join(violations, "\n"))
	}
}

func TestActorModelBundleCallsitesAreClosed(t *testing.T) {
	allowedAdmit := map[string]bool{
		"../platform/home/census.go": true, "../platform/home/declaration_api.go": true,
		"../platform/home/open.go": true,
	}
	var admit, endCascade, rawCommit []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fset.Position(call.Pos()).String()
			switch sel.Sel.Name {
			case "AdmitDeclared":
				if !allowedAdmit[path] {
					admit = append(admit, at)
				}
			case "EndCascade":
				if path != "../platform/home/end.go" {
					endCascade = append(endCascade, at)
				}
			case "CommitReservation":
				// These are all narrow completion/outbox calls. The raw registry
				// delegation is confined to accessdoor/overlay.go.
				allowed := path == "../runtime/accessdoor/overlay.go" || path == "../runtime/accessdoor/query.go" ||
					path == "../runtime/storeopen.go" || path == "../platform/home/storagehost.go"
				if !allowed {
					rawCommit = append(rawCommit, at)
				}
			}
			return true
		})
	})
	if len(admit)+len(endCascade)+len(rawCommit) != 0 {
		t.Fatalf("bundle callsite drift: AdmitDeclared=%v EndCascade=%v CommitReservation=%v", admit, endCascade, rawCommit)
	}
}

func TestActorModelAuthorityAndLivenessHaveNoFallbackMechanism(t *testing.T) {
	var withL, inlineVersionCompare []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.Ident:
				if x.Name == "withL" {
					withL = append(withL, fset.Position(x.Pos()).String())
				}
			case *ast.BinaryExpr:
				if path == "../platform/home/readface.go" {
					return true
				}
				text := exprSelectorNames(x)
				if text["BirthVersion"] && text["CurrentDeclVersion"] {
					inlineVersionCompare = append(inlineVersionCompare, fset.Position(x.Pos()).String())
				}
			}
			return true
		})
	})
	if len(withL)+len(inlineVersionCompare) != 0 {
		t.Fatalf("authority/liveness fallback drift: withL=%v inline author comparisons=%v", withL, inlineVersionCompare)
	}
}

func exprSelectorNames(root ast.Node) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(root, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			out[sel.Sel.Name] = true
		}
		return true
	})
	return out
}

func TestActorModelDataFlowChokepoints(t *testing.T) {
	var mintState, fireAndMark, ackTimer, snapshot []string
	var views []string
	var fenceDecls []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					at := fset.Position(x.Pos()).String()
					switch sel.Sel.Name {
					case "MintState":
						if path != "../runtime/accessdoor/memstate.go" {
							mintState = append(mintState, at)
						}
					case "FireAndMark":
						if path != "../runtime/schedule/firepen.go" {
							fireAndMark = append(fireAndMark, at)
						}
					case "AckTimer":
						ackTimer = append(ackTimer, at)
					}
				}
			case *ast.Ident:
				if path == "../platform/home/liveness.go" && x.Name == "snapshot" {
					snapshot = append(snapshot, fset.Position(x.Pos()).String())
				}
			case *ast.FuncDecl:
				if path != "../platform/home/liveness.go" || x.Recv == nil {
					break
				}
				switch x.Name.Name {
				case "AttachmentIntent", "WakeStanding":
					views = append(views, x.Name.Name)
				case "prepareAttachmentFence":
					fenceDecls = append(fenceDecls, x.Name.Name)
					if x.Type.Results == nil || len(x.Type.Results.List) == 0 {
						fenceDecls = append(fenceDecls, "missing-result")
					} else if id, ok := x.Type.Results.List[0].Type.(*ast.Ident); !ok || id.Name != "attachmentFence" {
						fenceDecls = append(fenceDecls, "state-bearing-result")
					}
				}
			}
			return true
		})
	})
	if len(mintState)+len(fireAndMark)+len(ackTimer)+len(snapshot) != 0 ||
		!sameStrings(views, []string{"AttachmentIntent", "WakeStanding"}) ||
		!sameStrings(fenceDecls, []string{"prepareAttachmentFence"}) {
		t.Fatalf("actor-model data-flow drift: MintState=%v FireAndMark=%v AckTimer=%v snapshot=%v views=%v fences=%v",
			mintState, fireAndMark, ackTimer, snapshot, views, fenceDecls)
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]int, len(got))
	for _, s := range got {
		seen[s]++
	}
	for _, s := range want {
		seen[s]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

func TestActorModelEndAuthorityIsWeldedAtMintPoints(t *testing.T) {
	allowedAuthorStamp := map[string]bool{
		"../platform/home/caps.go": true, "../platform/home/end.go": true,
		"../platform/home/fork.go": true, "../platform/home/open.go": true,
		"../platform/home/remote_lifecycle.go": true, "../platform/home/spawnhandle.go": true,
		"../platform/home/sysanchorcaps.go": true,
	}
	var exportedHomeEnd, authorLiterals []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncDecl:
				if path == "../platform/home/end.go" && x.Name.Name == "EndIdentity" {
					exportedHomeEnd = append(exportedHomeEnd, fset.Position(x.Pos()).String())
				}
			case *ast.CompositeLit:
				sel, ok := x.Type.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "AuthorStamp" && strings.HasPrefix(path, "../platform/home/") && !allowedAuthorStamp[path] {
					authorLiterals = append(authorLiterals, fset.Position(x.Pos()).String())
				}
			}
			return true
		})
	})
	if len(exportedHomeEnd)+len(authorLiterals) != 0 {
		t.Fatalf("end-authority drift: exported EndIdentity=%v AuthorStamp literals=%v", exportedHomeEnd, authorLiterals)
	}
}
