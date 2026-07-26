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
		"m.state.ResolveAuthority(ctx, prepared.Identity())",
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

func TestSchedulerDoesNotOwnIdentityHomeOrRegistryAuthority(t *testing.T) {
	handle := readAuthorityContractFile(t, "../runtime/schedule/handle.go")
	for _, forbidden := range []string{"IdentityHome", "HomeOf(", "HomeDurable", "HomeMemory"} {
		if strings.Contains(handle, forbidden) {
			t.Errorf("Schedule handle retains actor-world policy %q", forbidden)
		}
	}
	store := readAuthorityContractFile(t, "../runtime/internal/store/timers.go")
	if strings.Contains(store, "actor_registry") {
		t.Error("TimerStore re-authorizes welded ActorID through actor_registry")
	}
}

func TestActorIdentityStorageHomeIsPhysicallyConfined(t *testing.T) {
	// The actor record store is a runtime organ: only its own assembly point
	// names it, and nothing else in the tree may import it.
	allowedImports := map[string]bool{
		"../platform/home/actor_organ.go": true,
	}
	var escapedImports []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(importPath, "/runtime/actorstore") &&
				!allowedImports[path] {
				escapedImports = append(
					escapedImports, fset.Position(spec.Pos()).String(),
				)
			}
		}
	})
	if len(escapedImports) != 0 {
		t.Fatalf("actor record store escaped its assembly point: %v", escapedImports)
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
			for _, forbidden := range []string{
				"ActorWorld",
				"WorldRun",
				"WorldDurable",
				"WorldOf",
				"GrantOverlay",
				"BirthChannelOwned",
				"BirthCreatorIdentity",
				"HomeReader",
				"HomeOf",
				"identitystore",
				"ActorControlRow",
				// Spec §9.2: names that must never come back. Absence is
				// verified here, not assumed — a same-meaning wrapper or a
				// test double reintroducing one of them is the failure mode.
				"CheckAuthor",
				"AuthorStamp",
				"firepen",
				"currentPen",
				"ChannelActors",
			} {
				if strings.Contains(string(body), forbidden) {
					t.Errorf(
						"%s retains actor-kind/storage-home policy %q",
						filepath.ToSlash(path), forbidden,
					)
				}
			}
		}
	}
}

// Fork settles inside the ledger lock and has ZERO durable footprint: every
// fallible step (digest, admission, key mint) precedes the settle, and the
// three settled writes — entry install, ledger publish, replay-table row —
// cannot fail.
func TestForkPublicationHasNoPostCommitFailureTail(t *testing.T) {
	source := readAuthorityContractFile(t, "../runtime/actorctl/fork.go")
	settle := strings.Index(source, "// Settled: nothing below can fail.")
	if settle < 0 {
		t.Fatal("fork settle marker missing")
	}
	before, after := source[:settle], source[settle:]
	if end := strings.Index(after, "\n}\n"); end >= 0 {
		after = after[:end]
	}
	for _, required := range []string{
		"channel.Digest(",
		"c.checkCurrentLocked(",
		"mintAttempt()",
	} {
		if !strings.Contains(before, required) {
			t.Errorf("fallible fork step %q must precede the settle", required)
		}
	}
	for _, required := range []string{
		"c.store.InstallEntry(record)",
		"c.actors[child] = managedActor{",
		"c.forks[key] = forkEntry{",
	} {
		if !strings.Contains(after, required) {
			t.Errorf("settled fork step %q missing", required)
		}
	}
	if strings.Contains(after, "err") {
		t.Error("fork carries a failure tail after the change settled")
	}

	// The entry install itself is birth semantics: no context, no error, and a
	// colliding id fails the process rather than last-wins.
	store := readAuthorityContractFile(t, "../runtime/actorstore/store.go")
	installStart := strings.Index(store, "func (s *Store) InstallEntry(record storespec.ActorRecord) {")
	if installStart < 0 {
		t.Fatal("actorstore.InstallEntry must be an infallible operation")
	}
	installBody := store[installStart:]
	if end := strings.Index(installBody, "\n}"); end >= 0 {
		installBody = installBody[:end]
	}
	for _, forbidden := range []string{"context.Context", "error", "s.registry."} {
		if strings.Contains(installBody, forbidden) {
			t.Errorf("actorstore.InstallEntry contains failure tail %q", forbidden)
		}
	}
	if !strings.Contains(installBody, "panic(") {
		t.Error("actorstore.InstallEntry must fail-stop on a colliding entry")
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
				"ResolvePhysical(ctx, id)",
				"AdmitIdentity(ctx, id)",
				"MintAdmitted(admission",
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

func TestActorSystemStartsKernelBeforePublishingHostDesired(t *testing.T) {
	source := readAuthorityContractFile(t, "../platform/home/actor_system.go")
	start := strings.Index(source, "func (a *actorSystem) start(")
	if start < 0 {
		t.Fatal("actorSystem.start function not found")
	}
	end := strings.Index(source[start:], "\nfunc ")
	if end < 0 {
		t.Fatal("actorSystem.start function end not found")
	}
	body := source[start : start+end]
	kernel := strings.Index(body, "a.home.systemKernel.Start(systemUnit)")
	desired := strings.Index(body, "a.readServerDesired()")
	if kernel < 0 || desired < 0 {
		t.Fatal("actorSystem.start lacks kernel start or desired publication")
	}
	if kernel > desired {
		t.Fatal("ServerHost desired is published before SystemKernel is live")
	}
}
