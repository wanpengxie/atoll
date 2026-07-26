package archtest

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func productionFiles(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if skipDirs[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			out = append(out, filepath.ToSlash(path))
		}
		return nil
	})
	return out, err
}

func expressionText(fset *token.FileSet, expression ast.Expr) string {
	var buffer bytes.Buffer
	_ = format.Node(&buffer, fset, expression)
	return buffer.String()
}

func parseProductionPackage(t *testing.T, root string) map[string]*ast.File {
	t.Helper()
	paths, err := productionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]*ast.File, len(paths))
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files[path] = file
	}
	return files
}

func TestHarden03BActorRTIsAnExactUnitLeaf(t *testing.T) {
	forbiddenImports := []string{
		"/runtime/actorctl",
		"/runtime/actorhost",
		"/runtime/ipc",
		"/runtime/storespec",
		"/platform/internal/link",
	}
	files := parseProductionPackage(t, "../runtime/actorrt")
	fset := token.NewFileSet()
	for path, file := range files {
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range forbiddenImports {
				if strings.Contains(importPath, forbidden) {
					t.Errorf("%s imports manager/wire owner %s", path, importPath)
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			typeSpec, ok := node.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "Unit" {
				return true
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Errorf("%s: Unit is not a struct", path)
				return false
			}
			for _, field := range structure.Fields.List {
				switch field.Type.(type) {
				case *ast.MapType, *ast.ArrayType:
					t.Errorf("%s: Unit owns a collection field %s", path, expressionText(fset, field.Type))
				}
			}
			return false
		})
	}

	for _, retired := range []string{
		"Run" + "time",
		"Port" + "Owner",
		"Zombie" + "Info",
	} {
		for path, file := range files {
			ast.Inspect(file, func(node ast.Node) bool {
				spec, ok := node.(*ast.TypeSpec)
				if ok && spec.Name.Name == retired {
					t.Errorf("%s declares retired aggregate owner %s", path, retired)
				}
				return true
			})
		}
	}
}

func TestHarden03BHostOwnsExactActualAndRetiringState(t *testing.T) {
	body, err := os.ReadFile("../runtime/actorhost/host.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	for _, required := range []string{
		"type bodyActual struct {\n\tkey  AttemptKey\n\tunit *actorrt.Unit\n}",
		"type routeActual struct {\n\tkey     AttemptKey\n\tbinding Binding",
		"retiring map[*actorrt.Unit]*retireEntry",
		"func (h *HostSupervisor) retireLocked(",
		"h.watcherWG.Add(1)",
		"delete(state.retiring, task.entry.unit)",
		"if state != nil && state.empty()",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("actorhost exact ownership wall missing %q", required)
		}
	}
	hostStart := strings.Index(source, "type HostSupervisor struct")
	if hostStart < 0 {
		t.Fatal("HostSupervisor declaration not found")
	}
	hostEnd := strings.Index(source[hostStart:], "\n}\n")
	if hostEnd < 0 {
		t.Fatal("HostSupervisor declaration is unterminated")
	}
	hostDecl := source[hostStart : hostStart+hostEnd]
	if strings.Contains(hostDecl, "retiring map[") {
		t.Fatal("Retiring escaped per-ActorID HostState into Host-global state")
	}
	routeStart := strings.Index(source, "type routeActual struct")
	if routeStart < 0 {
		t.Fatal("routeActual declaration not found")
	}
	routeEnd := strings.Index(source[routeStart:], "\n}\n")
	if routeEnd < 0 {
		t.Fatal("routeActual declaration is unterminated")
	}
	routeDecl := source[routeStart : routeStart+routeEnd]
	if strings.Contains(routeDecl, "Unit") || strings.Contains(routeDecl, "Incarnation") {
		t.Fatal("remote route actual owns a local execution object")
	}

	typesBody, err := os.ReadFile("../runtime/actorhost/types.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(typesBody), "type "+("Carrier"+"Actual")) {
		t.Fatal("zero-resource remote actual representation returned")
	}
}

func TestHarden03BControllerContainerAndGateOrderAreSingular(t *testing.T) {
	controller, err := os.ReadFile("../runtime/actorctl/controller.go")
	if err != nil {
		t.Fatal(err)
	}
	// ONE ledger lock guards the entire ledger state (phase, member ledger, fork
	// replay table). The per-actor gate shards and the compensating lock family
	// they existed to coordinate are gone.
	for _, required := range []string{
		"ledger sync.RWMutex",
		"actors map[actor.ActorID]managedActor",
		"forks  map[forkKey]forkEntry",
		"store Store",
	} {
		if !strings.Contains(string(controller), required) {
			t.Errorf("Controller single-ledger-lock wall missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"placementGate", "controlGates", "lockActorSet", "gates ",
	} {
		if strings.Contains(string(controller), forbidden) {
			t.Errorf("Controller retains compensating lock %q", forbidden)
		}
	}
	if _, err := os.Stat("../runtime/actorctl/gates.go"); err == nil {
		t.Error("per-actor control gate shards returned")
	}

	files := parseProductionPackage(t, "../runtime/actorctl")
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			if fn, ok := node.(*ast.FuncDecl); ok && fn.Name.Name == "lockActorSet" {
				t.Error("multi-gate acquisition returned")
			}
			if spec, ok := node.(*ast.TypeSpec); ok && spec.Name.Name == "ChannelActors" {
				t.Error("runtime/actorctl retained a Channel composition root")
			}
			return true
		})
	}

	// Every lifecycle command takes the one ledger lock and performs exactly one
	// complete change under it.
	commands, err := os.ReadFile("../runtime/actorctl/commands.go")
	if err != nil {
		t.Fatal(err)
	}
	fork, err := os.ReadFile("../runtime/actorctl/fork.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(commands) + string(fork)
	for _, name := range []string{
		"func (c *Controller) Admit(",
		"func (c *Controller) Introduce(",
		"func (c *Controller) Fork(",
		"func (c *Controller) Restart(",
		"func (c *Controller) ApplyDeclaration(",
		"func (c *Controller) Terminal(",
	} {
		start := strings.Index(source, name)
		if start < 0 {
			t.Errorf("lifecycle command %q missing", name)
			continue
		}
		end := strings.Index(source[start:], "\n}\n")
		if end < 0 {
			t.Fatalf("command %q is unterminated", name)
		}
		body := source[start : start+end]
		if !strings.Contains(body, "c.ledger.Lock()") ||
			!strings.Contains(body, "defer c.ledger.Unlock()") {
			t.Errorf("command %q does not run under the one ledger lock", name)
		}
	}
}

func TestHarden03BHomeCloseCannotCrossCommandOwnerBarrier(t *testing.T) {
	body, err := os.ReadFile("../platform/home/close.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	barrier := strings.Index(source, "if err := h.actors.Quiesce(joinCtx); err != nil {")
	teardown := strings.Index(source, "h.closeOnce.Do(func()")
	storeClose := strings.Index(source, "if h.cs != nil && !h.storeCloseDone.Load()")
	if barrier < 0 || teardown < 0 || storeClose < 0 ||
		!(barrier < teardown && teardown < storeClose) {
		t.Fatalf(
			"Home close order must be owner barrier → one-shot teardown → Store close; positions=%d/%d/%d",
			barrier, teardown, storeClose,
		)
	}
	barrierEnd := strings.Index(source[barrier:], "\n\t}")
	if barrierEnd < 0 || !strings.Contains(source[barrier:barrier+barrierEnd], "return err") {
		t.Fatal("Home treats command-owner timeout as advisory instead of stopping teardown")
	}
	if strings.Contains(source, "appendIfError(faults, h.actors.Quiesce(ctx))") {
		t.Fatal("Home may aggregate a failed command-owner barrier and continue teardown")
	}
}

func TestHarden03BPhysicalOwnersUseExactObjectIdentity(t *testing.T) {
	outbound, err := os.ReadFile("../platform/compute/outbound.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"slots    map[*OutboundSlot]struct{}",
		"arms atomic.Pointer[OutboundArmsBundle]",
		"old := slot.arms.Swap(next)",
		"bundle := s.arms.Load()",
	} {
		if !strings.Contains(string(outbound), required) {
			t.Errorf("daemon exact-slot wall missing %q", required)
		}
	}
	if strings.Contains(string(outbound), "map[actor.ActorID]*OutboundSlot") {
		t.Fatal("DaemonOutbound regressed to a by-ActorID slot owner")
	}

	physical, err := os.ReadFile("../platform/internal/link/physical.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"bindings map[*Binding]struct{}",
		"streams  map[*ActorStream]struct{}",
		"type Binding struct {",
		"type ActorStream struct {",
	} {
		if !strings.Contains(string(physical), required) {
			t.Errorf("session exact-child wall missing %q", required)
		}
	}
	for _, retired := range []string{
		"Has" + "Stream(",
		"Detach" + "Stream(",
	} {
		if strings.Contains(string(physical), retired) {
			t.Errorf("physical owner retains by-ID child API %q", retired)
		}
	}
}

func TestHarden03BP2OwnershipAndRetryWalls(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}

	// Fork replay is answered by an in-process table, never by a durable
	// receipt: the restore path knows only the durable registry.
	store := read("../runtime/actorstore/store.go")
	if strings.Contains(store, "LookupFork") || strings.Contains(store, "LookupCompleted") {
		t.Fatal("durable fork receipt lookup returned")
	}
	restoreStart := strings.Index(store, "func (s *Store) RestoreActive(")
	if restoreStart < 0 {
		t.Fatal("RestoreActive implementation not found")
	}
	restoreEnd := strings.Index(store[restoreStart:], "\n}\n")
	if restoreEnd < 0 {
		t.Fatal("RestoreActive implementation is unterminated")
	}
	if strings.Contains(store[restoreStart:restoreStart+restoreEnd], "s.entries") {
		t.Fatal("restore reconstructs the process entry table")
	}

	types := read("../runtime/actorhost/types.go")
	for _, required := range []string{
		"type BindingResource interface {",
		"type Binding struct {\n\tref *bindingRef\n}",
	} {
		if !strings.Contains(types, required) {
			t.Errorf("opaque Binding wall missing %q", required)
		}
	}

	commands := read("../runtime/actorctl/commands.go")
	if strings.Contains(commands, "c.valueEffects.RunActorsEnded") {
		t.Fatal("terminal path invokes external ended tail while Controller gates may be held")
	}

	outbound := read("../platform/compute/outbound.go")
	for _, required := range []string{
		"retryAt   time.Time",
		"func (d *DaemonOutbound) Seal(",
		"func (d *DaemonOutbound) CloseResidual(",
	} {
		if !strings.Contains(outbound, required) {
			t.Errorf("DaemonOutbound P2 wall missing %q", required)
		}
	}

	compute := read("../platform/compute/compute.go")
	seal := strings.Index(compute, "outbound.Seal(")
	hostClose := strings.Index(compute, "host.Close(")
	residual := strings.Index(compute, "outbound.CloseResidual(")
	sessionClose := strings.Index(compute, "currentSession.Close(")
	if seal < 0 || hostClose < seal || residual < hostClose || sessionClose < residual {
		t.Fatal("daemon close DAG is not outbound seal → Host close → residual slots → session close")
	}

	presence := read("../platform/internal/presence/presence.go")
	if !strings.Contains(presence, "row.remote == key && row.route == route") {
		t.Fatal("remote presence down is not fenced by exact Binding")
	}
}

func TestHarden03BCollaborationDTOsDoNotCarryExecutionIdentity(t *testing.T) {
	forbiddenFields := map[string]bool{"AttemptKey": true, "Incarnation": true}
	roots := []string{"../protocol/message"}
	for _, root := range roots {
		files := parseProductionPackage(t, root)
		for path, file := range files {
			ast.Inspect(file, func(node ast.Node) bool {
				field, ok := node.(*ast.Field)
				if !ok {
					return true
				}
				for _, name := range field.Names {
					if forbiddenFields[name.Name] {
						t.Errorf("%s collaboration DTO exposes %s", path, name.Name)
					}
				}
				return true
			})
		}
	}

	fset := token.NewFileSet()
	frame, err := parser.ParseFile(fset, "../runtime/ipc/frame.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	businessPayloads := map[string]bool{
		"Frame": true, "DeliverPayload": true, "EmitPayload": true,
		"DeliverResultPayload": true, "CancelPayload": true,
	}
	ast.Inspect(frame, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || !businessPayloads[spec.Name.Name] {
			return true
		}
		structure, ok := spec.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, field := range structure.Fields.List {
			for _, name := range field.Names {
				if forbiddenFields[name.Name] {
					t.Errorf("%s.%s exposes execution identity", spec.Name.Name, name.Name)
				}
			}
		}
		return false
	})
}

func TestHarden03BOpenRequestQueriesStayOnClosureWhitelist(t *testing.T) {
	allowed := map[string]bool{
		"../lib/behavior/death.go:MaterialiseReceiverUnavailable": true,
		"../lib/behavior/death.go:ReconcileReceiverUnavailable":   true,
		"../platform/home/humancell_wiring.go:isRequestOpen":      true,
	}
	var calls []string
	walkProductionGo(t, func(path string, file *ast.File, _ *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name != "OpenRequestsForActor" &&
				selector.Sel.Name != "DistinctOpenRequestReceivers" {
				return true
			}
			key := path + ":" + enclosingFunc(file, call.Pos())
			if !allowed[key] {
				calls = append(calls, key)
			}
			return true
		})
	})
	if len(calls) != 0 {
		t.Fatalf("open-request query escaped closure/status whitelist: %v", calls)
	}
}

func TestHarden03BNoReplayOrLegacyExecutionOwnerInProduction(t *testing.T) {
	forbidden := []string{
		"redeliver" + "OpenRequests",
		"Rebindable" + "Arms",
		"retry" + "Lifecycle",
		"Spawn" + "IfAbsent",
		"Stop" + "All",
		"Drain" + "Zombies",
		"Desired" + "Dormant",
		"Request" + "Idle",
		"Kind" + "Idle",
		"Idle" + "Timeout",
		"Wake" + "Grace",
		"Ensure" + "Run",
		"Deliver" + "Committed",
		"T" + "Idle",
		"t_" + "idle_ms",
	}
	for _, root := range []string{"../app", "../cmd", "../drivers", "../lib", "../platform", "../registry", "../runtime"} {
		paths, err := productionFiles(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, symbol := range forbidden {
				if strings.Contains(string(body), symbol) {
					t.Errorf("%s retains retired execution/replay owner %q", path, symbol)
				}
			}
		}
	}
}

func TestHarden03BActorHostDependencyDirection(t *testing.T) {
	forbiddenImports := []string{"/platform/compute", "/platform/internal/link", "/runtime/actorctl"}
	files := parseProductionPackage(t, "../runtime/actorhost")
	for path, file := range files {
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range forbiddenImports {
				if strings.Contains(importPath, forbidden) {
					t.Errorf("%s imports higher owner %s", path, importPath)
				}
			}
		}
	}
}

func TestHarden03BBodyConstructionUsesOnlyExactSnapshot(t *testing.T) {
	cases := []struct {
		path      string
		required  []string
		forbidden []string
	}{
		{
			path:      "../platform/home/open.go",
			required:  []string{"factories.LookupByClass(", "input.ExecutionSpec.Class", "input.ExecutionSpec.Config"},
			forbidden: []string{"factories.Lookup(input.ActorID)"},
		},
		{
			path:      "../platform/compute/compute.go",
			required:  []string{"source.LookupExact(", "input.AttemptKey", "input.ExecutionSpec"},
			forbidden: []string{"source.Lookup(input.ActorID)"},
		},
		{
			path:      "../runtime/actorctl/authority.go",
			required:  []string{"func (c *Controller) PrepareRun(", "value.Attempt != key", "!executionSpec(value.Record).Equal(spec)"},
			forbidden: []string{"ActualCurrent"},
		},
		{
			path:      "../runtime/managedcaps/minter.go",
			required:  []string{"func (m *Minter) Mint(", "prepared.Identity()", "prepared.Run()"},
			forbidden: []string{"ActualCurrent", "managedInvocation"},
		},
	}
	for _, tc := range cases {
		body, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		for _, required := range tc.required {
			if !strings.Contains(string(body), required) {
				t.Errorf("%s missing exact construction seam %q", tc.path, required)
			}
		}
		for _, forbidden := range tc.forbidden {
			if strings.Contains(string(body), forbidden) {
				t.Errorf("%s retains ActorID-only construction lookup %q", tc.path, forbidden)
			}
		}
	}
}

// Sponsor is gone entirely: there is no lineage field, no cascade closure and
// no sponsor-based End authority anywhere in the tree.
func TestHarden03BSponsorIsFullyRipped(t *testing.T) {
	for _, root := range []string{"../runtime", "../platform", "../app", "../lib"} {
		paths, err := productionFiles(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"Sponsor", "ErrEndNotSponsor"} {
				if strings.Contains(string(body), forbidden) {
					t.Errorf("%s retains sponsor vocabulary %q", path, forbidden)
				}
			}
		}
	}
}

func TestHarden03BTerminalCommandSetIsClosed(t *testing.T) {
	body, err := os.ReadFile("../runtime/actorctl/types.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"TerminalEnd", "TerminalRemove"} {
		if !strings.Contains(string(body), required) {
			t.Errorf("terminal command set missing %s", required)
		}
	}
	if strings.Contains(string(body), "TerminalDetachDaemon") {
		t.Error("detach returned as a terminal word; it is a wiring-domain action")
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../runtime/actorctl/types.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for _, name := range spec.Names {
			if strings.HasPrefix(name.Name, "Terminal") {
				count++
			}
		}
		return true
	})
	if count != 2 {
		t.Fatalf("TerminalKind constants=%d, want 2", count)
	}
}

func TestHarden03BNoExportedLifecycleBinder(t *testing.T) {
	for _, root := range []string{"../runtime", "../platform", "../lib"} {
		paths, err := productionFiles(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range file.Decls {
				switch value := declaration.(type) {
				case *ast.GenDecl:
					for _, item := range value.Specs {
						typeSpec, ok := item.(*ast.TypeSpec)
						if ok && typeSpec.Name.IsExported() &&
							strings.Contains(typeSpec.Name.Name, "LifecycleAuthority") {
							t.Errorf("%s exports lifecycle authority %s", path, typeSpec.Name.Name)
						}
					}
				case *ast.FuncDecl:
					if value.Name.IsExported() && strings.Contains(value.Name.Name, "BindActor"+"Lifecycle") {
						t.Errorf("%s exports lifecycle binder %s", path, value.Name.Name)
					}
				}
			}
		}
	}
	if _, err := os.Stat("../runtime/actorspec"); !os.IsNotExist(err) {
		t.Fatalf("runtime/actorspec must not exist: %v", err)
	}
}

// TestHarden03BServerManagedCapsUseOneAuthorityBundleMinter pins the final
// split: Platform invokes one bundle minter, while no business permission
// depends on Host ActualCurrent.
func TestHarden03BServerManagedCapsUseOneAuthorityBundleMinter(t *testing.T) {
	homeFiles, err := productionFiles("../platform/home")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range homeFiles {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "ActualCurrent") {
			t.Errorf("%s references actorhost.ActualCurrent as capability authority", filepath.ToSlash(path))
		}
		if strings.Contains(string(body), "buildManagedCaps") {
			t.Errorf("%s retains the obsolete per-arm managed caps assembler", filepath.ToSlash(path))
		}
	}

	caps, err := os.ReadFile("../runtime/managedcaps/minter.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"func (m *Minter) Mint(",
		"m.pen.MintAuthority(prepared.Run()",
		"m.access.MintAuthority(prepared.Run())",
		"m.state.ResolveAuthority(ctx, prepared.Identity())",
		"m.schedule.MintAuthority(prepared.Identity())",
	} {
		if !strings.Contains(string(caps), required) {
			t.Errorf("runtime/managedcaps/minter.go missing authority bundle anchor %q", required)
		}
	}
	for _, forbidden := range []string{"managedInvocation", "ActualCurrent", "BirthVersion"} {
		if strings.Contains(string(caps), forbidden) {
			t.Errorf("runtime/managedcaps/minter.go retained obsolete authority %q", forbidden)
		}
	}
}

func TestHarden03BSubjectSlotDeleteOwnerIsLevelReconcile(t *testing.T) {
	body, err := os.ReadFile("../platform/home/reconcile.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	for _, required := range []string{
		"keys := slots.Keys()",
		"authority.LookupActive(ctx, id)",
		"slots.Remove(id)",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("subject-slot terminal-recheck wall missing %q", required)
		}
	}
	if strings.Count(source, "slots.Keys()") != 1 {
		t.Fatalf("subject-slot key snapshots=%d, want exactly one per pass",
			strings.Count(source, "slots.Keys()"))
	}

	calls := 0
	files := parseProductionPackage(t, "../platform/home")
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Remove" {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok || receiver.Name != "slots" {
				return true
			}
			calls++
			if path != "../platform/home/reconcile.go" ||
				enclosingFunc(file, call.Pos()) != "sweepSubjectSlots" {
				t.Errorf("%s removes subject slot outside level reconcile", path)
			}
			return true
		})
	}
	if calls != 1 {
		t.Fatalf("subject-slot physical delete callsites=%d, want 1", calls)
	}
}
