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

// recvBaseTypeName extracts the unqualified receiver type name from a
// FuncDecl's receiver field list (e.g. "*door" or "door" -> "door"). Returns
// "" for a receiver-less (free) function.
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
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// raw CommitReservation is call-site gated below (not
			// file-level): only the resourceCompletion/door wrapper's own
			// commitReservationLocked body may reach
			// deps.Registry.CommitReservation directly (DoD 32: "调用位点级
			// 白名单，不许文件级放行" — a sibling method in the SAME file,
			// e.g. door.invoke or door.create, must NOT get a free pass just
			// because it shares overlay.go with the wrapper).
			rawCommitAllowedHere := path == "../runtime/accessdoor/overlay.go" &&
				recvBaseTypeName(fn) == "door" && fn.Name.Name == "commitReservationLocked"
			ast.Inspect(fn.Body, func(n ast.Node) bool {
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
					// Only a call whose IMMEDIATE receiver is literally
					// "....Registry.CommitReservation(...)" is the raw store
					// call this red line guards (deps.Registry is the
					// resourceRegistry — the accessdoor Deps field, not the
					// ResourceCompletion/ResourceOutbox wrapper interfaces).
					// A call like completion.CommitReservation(...) or
					// h.outbox.CommitReservation(...) shares the method NAME
					// but targets the WRAPPER, which is unrestricted by
					// design (it exists precisely so callers outside this
					// package can reach the gated completion path) — conflating
					// the two by name alone is exactly the file-level
					// over-approximation DoD 32 forbids.
					recvSel, ok := sel.X.(*ast.SelectorExpr)
					isRawRegistryCall := ok && recvSel.Sel.Name == "Registry"
					if isRawRegistryCall && !rawCommitAllowedHere {
						rawCommit = append(rawCommit, at)
					}
				}
				return true
			})
		}
	})
	if len(admit)+len(endCascade)+len(rawCommit) != 0 {
		t.Fatalf("bundle callsite drift: AdmitDeclared=%v EndCascade=%v CommitReservation(raw)=%v", admit, endCascade, rawCommit)
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

// lifecycleAuthorMintPoints is the S9 closed set of three minting loci
// (actor/system/wire 三道) that may weld a fresh AuthorStamp into a
// lifecycleEndHandle{...} literal — the "生死动词作者一律铸造时焊死" red
// line. It is call-site (function) precise, not file-level: a sibling
// function sharing the same file (e.g. any future helper added to
// end.go/open.go) gets NO free pass merely by being declared alongside a
// real mint point — every entry names both the file AND the exact
// function/method that may construct the literal.
//   - spawnhandle.go/newSpawnHandle: actor 道 — welds a live incarnation's
//     own identity as its Fork/DespawnChild/EndSelf author.
//   - end.go/systemEndHandle: system 道 — the lazy self-heal re-mint of the
//     fixed system author (mirrors open.go's genesis mint below; both exist
//     because Home.systemEnd is populated once at Open but re-derivable if
//     ever unset, never a THIRD independent system identity).
//   - open.go/Open: system 道 genesis mint (h.systemEnd assembly).
//   - remote_lifecycle.go/handleRemoteEnd: wire 道 — welds the wire
//     attach-authenticated incarnation's identity for a remote End frame.
var lifecycleAuthorMintPoints = map[[2]string]bool{
	{"../platform/home/spawnhandle.go", "newSpawnHandle"}:       true,
	{"../platform/home/end.go", "systemEndHandle"}:              true,
	{"../platform/home/open.go", "Open"}:                        true,
	{"../platform/home/remote_lifecycle.go", "handleRemoteEnd"}: true,
}

func TestActorModelEndAuthorityIsWeldedAtMintPoints(t *testing.T) {
	var exportedHomeEnd, authorLiterals []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if path == "../platform/home/end.go" && fn.Name.Name == "EndIdentity" {
				exportedHomeEnd = append(exportedHomeEnd, fset.Position(fn.Pos()).String())
			}
			if fn.Body == nil {
				continue
			}
			allowedHere := lifecycleAuthorMintPoints[[2]string{path, fn.Name.Name}]
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				cl, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				// Only an AuthorStamp{...} literal welded AS the `author`
				// field of a lifecycleEndHandle{...} literal is the S9
				// generation-authority weld this red line governs — an
				// AuthorStamp built to weld a resource/schedule CAPABILITY
				// (caps.go/sysanchorcaps.go's Access/Schedule Mint calls) or
				// a throwaway CheckAuthor verification stamp (fork.go's
				// parent-liveness recheck, mirroring the same pattern
				// runtime/schedule/firepen.go and
				// runtime/harness/step_author_gate.go already use elsewhere
				// in the tree) is a structurally different, unrestricted
				// concern — conflating the two under one file-level ban is
				// exactly the over-approximation DoD 32 forbids.
				ident, ok := cl.Type.(*ast.Ident)
				if !ok || ident.Name != "lifecycleEndHandle" {
					return true
				}
				for _, elt := range cl.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok || key.Name != "author" {
						continue
					}
					authorCl, ok := kv.Value.(*ast.CompositeLit)
					if !ok {
						continue
					}
					sel, ok := authorCl.Type.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "AuthorStamp" {
						continue
					}
					if !allowedHere {
						authorLiterals = append(authorLiterals, fset.Position(authorCl.Pos()).String())
					}
				}
				return true
			})
		}
	})
	if len(exportedHomeEnd)+len(authorLiterals) != 0 {
		t.Fatalf("end-authority drift: exported EndIdentity=%v lifecycle AuthorStamp welds outside the closed mint-point set=%v", exportedHomeEnd, authorLiterals)
	}
}

// TestActorModelLivenessReadFaceIsClosedToTwoViews enforces §2.6's "读面 =
// 两个目的视图" red line as an EXPORTED-METHOD-ENUMERATION closed set, not
// merely an existence check (the pre-v1.4 shape: "does AttachmentIntent
// exist, does WakeStanding exist" — which stays green even if a THIRD
// exported read projection gets bolted on beside them). Every exported
// method on *livenessLedger is enumerated; it must be a member of the full
// §2.6 method-set closure (the seven write paths' named events + the two
// read views), and the two read views specifically must be exactly
// {AttachmentIntent, WakeStanding} — a third one appearing anywhere turns
// this red.
func TestActorModelLivenessReadFaceIsClosedToTwoViews(t *testing.T) {
	wantExported := map[string]bool{
		"Bootstrap": true, "Close": true,
		"AdmitIdentity": true, "EndIdentity": true,
		"AcceptDelivery": true, "AcceptFiredDelivery": true,
		"ApproveIdle": true, "ObserveDown": true, "Retire": true,
		"RetireIfVersionSkew": true, "RetireIfTicketMatches": true,
		"BeginEnsure": true, "PublishLocal": true, "Attach": true, "AbortEnsure": true,
		"AttachmentIntent": true, "WakeStanding": true,
	}
	wantReadFace := []string{"AttachmentIntent", "WakeStanding"}
	seenReadFace := map[string]bool{}
	var extra []string
	walkProductionGo(t, func(path string, f *ast.File, fset *token.FileSet) {
		if path != "../platform/home/liveness.go" {
			return
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() || recvBaseTypeName(fn) != "livenessLedger" {
				continue
			}
			name := fn.Name.Name
			if !wantExported[name] {
				extra = append(extra, fmt.Sprintf("%s: unexpected exported livenessLedger method %s (not in the §2.6 closed method set)", fset.Position(fn.Pos()), name))
			}
			if name == "AttachmentIntent" || name == "WakeStanding" {
				seenReadFace[name] = true
			}
		}
	})
	var missingReadFace []string
	for _, name := range wantReadFace {
		if !seenReadFace[name] {
			missingReadFace = append(missingReadFace, name)
		}
	}
	if len(extra)+len(missingReadFace) != 0 {
		t.Fatalf("liveness read-face closure drift: extra exported methods=%v missing read views=%v", extra, missingReadFace)
	}
}
