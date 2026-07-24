package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func readAuthorityContractFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestActorAuthorityHasNoRuntimeCompositionRoot(t *testing.T) {
	var retired []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if ok && spec.Name.Name == "ChannelActors" {
				retired = append(retired, fset.Position(spec.Pos()).String())
			}
			return true
		})
	})
	if len(retired) != 0 {
		t.Fatalf("Runtime Channel composition root returned: %v", retired)
	}

	home := readAuthorityContractFile(t, "../platform/home/home.go")
	for _, peer := range []string{
		"controller   *actorctl.Controller",
		"serverHost   *actorhost.HostSupervisor",
		"systemKernel *systemkernel.Kernel",
		"managedCaps  *managedcaps.Minter",
		"systemCaps   *systemcaps.Minter",
	} {
		if !strings.Contains(home, peer) {
			t.Errorf("Home does not hold Runtime peer %q directly", peer)
		}
	}

	for _, path := range []string{
		"../runtime/actorctl",
		"../runtime/actorhost",
		"../runtime/actorrt",
	} {
		files := parseProductionPackage(t, path)
		for filename, file := range files {
			for _, spec := range file.Imports {
				importPath, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				switch path {
				case "../runtime/actorctl":
					for _, forbidden := range []string{
						"/platform/",
						"/runtime/accessdoor",
						"/runtime/harness",
						"/runtime/managedcaps",
						"/runtime/schedule",
						"/runtime/systemkernel",
					} {
						if strings.Contains(importPath, forbidden) {
							t.Errorf("%s imports composition/capability owner %s", filename, importPath)
						}
					}
				case "../runtime/actorhost":
					for _, forbidden := range []string{"/platform/", "/runtime/actorctl", "/runtime/storespec"} {
						if strings.Contains(importPath, forbidden) {
							t.Errorf("%s imports logical/composition owner %s", filename, importPath)
						}
					}
				case "../runtime/actorrt":
					for _, forbidden := range []string{"/platform/", "/runtime/actorctl", "/runtime/actorhost"} {
						if strings.Contains(importPath, forbidden) {
							t.Errorf("%s imports aggregate owner %s", filename, importPath)
						}
					}
				}
			}
		}
	}
}

func TestActorAuthorityManagedBodyUsesOneBundleMint(t *testing.T) {
	var managedCalls, systemCalls, escapedArmMints []string
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
			if selector.Sel.Name == "Mint" {
				if receiver, ok := selector.X.(*ast.SelectorExpr); ok {
					switch receiver.Sel.Name {
					case "managedCaps":
						managedCalls = append(managedCalls, at)
					case "systemCaps":
						systemCalls = append(systemCalls, at)
					}
				}
			}
			if selector.Sel.Name == "MintAuthority" || selector.Sel.Name == "ResolveAuthority" {
				if path != "../runtime/managedcaps/minter.go" &&
					path != "../runtime/systemcaps/minter.go" {
					escapedArmMints = append(escapedArmMints, at)
				}
			}
			return true
		})
	})
	if len(managedCalls) != 1 || len(systemCalls) != 1 || len(escapedArmMints) != 0 {
		t.Fatalf(
			"bundle mint drift: managed=%v system=%v escaped per-arm authority mints=%v",
			managedCalls, systemCalls, escapedArmMints,
		)
	}

	managed := readAuthorityContractFile(t, "../runtime/managedcaps/minter.go")
	for _, required := range []string{
		"m.pen.MintAuthority(prepared.Run()",
		"m.access.MintAuthority(prepared.Run())",
		"m.state.ResolveAuthority(prepared.Identity(), prepared.World())",
		"m.schedule.MintAuthority(prepared.Identity())",
		"attempt:    prepared.AttemptKey()",
	} {
		if !strings.Contains(managed, required) {
			t.Errorf("managed bundle lifetime mapping missing %q", required)
		}
	}
	for _, forbidden := range []string{"ActualCurrent", "BirthVersion", "managedInvocation"} {
		if strings.Contains(managed, forbidden) {
			t.Errorf("managed bundle contains obsolete permission coordinate %q", forbidden)
		}
	}
}

func TestActorAuthorityDaemonFacadesUseAAndAGButNotC(t *testing.T) {
	outbound := readAuthorityContractFile(t, "../platform/compute/outbound.go")
	for _, required := range []string{
		"func (s *OutboundSlot) loadIdentity()",
		"!s.identity.IsCurrent()",
		"func (s *OutboundSlot) loadAttempt()",
		"!s.attempt.IsCurrent()",
		"func (p outboundPen) Write(",
		"p.slot.loadAttempt()",
		"func (a outboundState) Invoke(",
		"a.slot.loadIdentity()",
		"func (s outboundSchedule) Schedule(",
		"s.slot.loadIdentity()",
		"func (l outboundLifecycle) Fork(",
		"l.slot.loadConnected()",
	} {
		if !strings.Contains(outbound, required) {
			t.Errorf("daemon capability lifetime mapping missing %q", required)
		}
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../platform/compute/outbound.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		receiver := recvBaseTypeName(fn)
		if receiver != "outboundPen" && receiver != "outboundState" &&
			receiver != "outboundResourceAccess" && receiver != "outboundSchedule" &&
			receiver != "outboundLifecycle" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "loadPhysical" {
				t.Errorf("%s uses physical C as business capability authority",
					fset.Position(selector.Pos()))
			}
			return true
		})
	}
}

func TestActorAuthoritySchedulerHomeIsNotActorIncarnation(t *testing.T) {
	types := readAuthorityContractFile(t, "../runtime/schedule/types.go")
	for _, required := range []string{
		"type TimerHome string",
		"TimerHomeDurable TimerHome = \"durable\"",
		"TimerHomeMemory TimerHome = \"memory\"",
		"Home    TimerHome",
	} {
		if !strings.Contains(types, required) {
			t.Errorf("Scheduler storage-home vocabulary missing %q", required)
		}
	}

	forbidden := []string{
		"Birth" + "Version",
		"Author" + "VersionStale",
		"Bind" + "Incarnation",
		"Bind" + "Identity",
		"Mint" + "Current",
		"managed" + "Invocation",
		"current" + "Pen",
		"current" + "Access",
		"current" + "ResourceAccess",
		"current" + "Schedule",
	}
	for _, root := range []string{"../app", "../lib", "../platform", "../runtime"} {
		paths, err := productionFiles(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, token := range forbidden {
				if strings.Contains(string(body), token) {
					t.Errorf("%s retains obsolete actor-authority token %q", filepath.ToSlash(path), token)
				}
			}
		}
	}
}

func TestActorAuthorityCollaborationIngressUsesOneAdmittedSnapshot(t *testing.T) {
	cases := []struct {
		path     string
		required []string
	}{
		{
			path: "../platform/internal/link/accept.go",
			required: []string{
				"AdmitIdentity(ctx, id)",
				"MintAdmitted(admission",
				"ResolveAdmitted(admission)",
			},
		},
		{
			path: "../platform/home/scheduler.go",
			required: []string{
				"AdmitIdentity(ctx, author)",
				"MintAdmitted(admission",
			},
		},
	}
	for _, tc := range cases {
		source := readAuthorityContractFile(t, tc.path)
		for _, required := range tc.required {
			if !strings.Contains(source, required) {
				t.Errorf("%s lacks admitted collaboration seam %q", tc.path, required)
			}
		}
	}
}

func TestActorAuthoritySystemCapsUseIndependentRoot(t *testing.T) {
	source := readAuthorityContractFile(t, "../runtime/systemcaps/minter.go")
	for _, required := range []string{
		"type rootAuthority struct{}",
		"func (rootAuthority) ActorID() actor.ActorID { return actor.SystemActorID }",
		"func (rootAuthority) Admit() error           { return nil }",
		"Lifecycle: nil",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("System root capability path missing %q", required)
		}
	}
	for _, forbidden := range []string{"AttemptKey", "RunAuthority", "BirthVersion"} {
		if strings.Contains(source, forbidden) {
			t.Errorf("System root capability path contains managed coordinate %q", forbidden)
		}
	}
}
