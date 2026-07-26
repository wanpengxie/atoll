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
				// The authority-shaped mints have exactly three homes: the two
				// capability bundles (one body, one mint) and the remote
				// ingress (one operation, one mint). Anywhere else is a fourth
				// place deciding when a capability comes into being.
				if path != "../runtime/managedcaps/minter.go" &&
					path != "../runtime/systemcaps/minter.go" &&
					path != "../runtime/remoteingress/ingress.go" {
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

// A remote operation is frame → door → one verdict + execution. The link
// decodes and calls the ingress with its endpoint's own coordinate; it holds no
// Controller, no minter and no resolver, so it has no parts to assemble a
// verdict out of. The timer fire path is the one remaining admitted-snapshot
// source boundary and stays as it is (its author gate IS the verdict).
func TestActorAuthorityRemoteIngressIsTheOnlyRemoteDoor(t *testing.T) {
	link := readAuthorityContractFile(t, "../platform/internal/link/accept.go")
	for _, required := range []string{
		"Ingress   remoteingress.RemoteIngress",
		"a.ingress.Emit(ctx, id, key, env)",
		"a.ingress.Access(ctx, id, key, call)",
		"a.ingress.Schedule(ctx, id, call)",
		"fork:          a.ingress.Fork",
		"endSelf:       a.ingress.EndSelf",
	} {
		if !strings.Contains(link, required) {
			t.Errorf("link accept.go lacks remote ingress seam %q", required)
		}
	}
	for _, forbidden := range []string{
		"admitIdentity", "AdmitIdentity", "MintAdmitted", "MintAuthority",
		"ResolvePhysical", "StateHandles", "actorctl.",
	} {
		if strings.Contains(link, forbidden) {
			t.Errorf("link accept.go assembles capability work itself: %q", forbidden)
		}
	}
	// Package-wide: the link may speak contract vocabulary (harness.Pen,
	// accessdoor.Outcome — a daemon-side proxy IS one of those), but it may
	// name no minter, no resolver and no Controller. Those are the parts a
	// verdict could be assembled from.
	for _, path := range phaseAProductionFiles(t, "../platform/internal/link") {
		body := readAuthorityContractFile(t, path)
		for _, forbidden := range []string{
			"harness.Minter", "harness.AdmittedMinter",
			"accessdoor.AccessMinter", "accessdoor.AdmittedMinter",
			"accessdoor.StateHandleResolver",
			"schedule.Minter", "schedule.AdmittedMinter",
			"storespec.CollaborationAuthority",
			"/runtime/actorctl",
		} {
			if strings.Contains(body, forbidden) {
				t.Errorf("%s names a judgment owner %q — link holds one ingress interface", path, forbidden)
			}
		}
	}

	// Each ingress arm judges at its own precision and then enters the real
	// organ: A/G for the pen and channel resources, A for state and schedule.
	ingress := readAuthorityContractFile(t, "../runtime/remoteingress/ingress.go")
	for _, required := range []string{
		"admission, err := i.controller.AdmitRun(id, attempt)",
		"i.pen.MintAuthority(admission.Run, admission.Kind)",
		"i.access.MintAuthority(i.controller.RunAuthorityFor(id, attempt))",
		"i.state.StateIngress(",
		"i.schedule.MintAuthority(i.controller.IdentityAuthorityFor(id))",
		"i.controller.Fork(ctx, actorctl.ForkRequest{",
		"i.controller.End(ctx, actorctl.EndRequest{",
	} {
		if !strings.Contains(ingress, required) {
			t.Errorf("remote ingress lacks organ entry %q", required)
		}
	}
	if strings.Contains(ingress, "ChannelID") || strings.Contains(ingress, "channel.ID") {
		t.Error("remote ingress handles a channel id")
	}

	scheduler := readAuthorityContractFile(t, "../platform/home/scheduler.go")
	for _, required := range []string{
		"AdmitIdentity(ctx, author)",
		"MintAdmitted(admission)",
	} {
		if !strings.Contains(scheduler, required) {
			t.Errorf("timer fire lacks admitted collaboration seam %q", required)
		}
	}
}

// Channel identity has ONE knower: the harness, whose Deps.ChannelID is its own
// binding constant and whose only use for it is stamping the row it writes. It
// used to travel — assembly point → minter field → mint parameter → two equality
// self-checks — so that the value could be compared against the place it came
// from. Nothing in the mint / control / remote-entry chain carries it now.
func TestChannelIdentityIsOnlyTheHarnessOwnBinding(t *testing.T) {
	for _, pkg := range []string{
		"../runtime/managedcaps",
		"../runtime/systemcaps",
		"../runtime/remoteingress",
		"../runtime/actorctl",
		"../runtime/schedule",
	} {
		// Code only — a comment may still explain who welds the stamp.
		for path, file := range parseProductionPackage(t, pkg) {
			ast.Inspect(file, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				switch ident.Name {
				case "channelID", "ChannelID", "chID":
					t.Errorf("%s handles channel identity (%s) — only the harness knows it",
						path, ident.Name)
				}
				return true
			})
		}
	}

	pen := readAuthorityContractFile(t, "../runtime/harness/pen.go")
	for _, required := range []string{
		"func (m *minter) MintAdmitted(admission storespec.IdentityAdmission) Pen {",
		"func (m *minter) MintAuthority(authority capauth.Authority, kind actor.Kind) Pen {",
		"env.ChannelID = p.chain.deps.ChannelID",
	} {
		if !strings.Contains(pen, required) {
			t.Errorf("harness pen lacks channel-stamp shape %q", required)
		}
	}
	for _, path := range []string{
		"../runtime/harness/step_caller_auth.go",
		"../runtime/harness/step_envelope_shape.go",
	} {
		body := readAuthorityContractFile(t, path)
		if strings.Contains(body, "!= s.deps.ChannelID") {
			t.Errorf("%s still compares the channel stamp against its own source", path)
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
