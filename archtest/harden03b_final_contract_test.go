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
	for _, required := range []string{
		"stateMu sync.RWMutex",
		"actors  map[actor.ActorID]ActiveActor",
		"store        Store",
		"valueEffects controllerValueEffects",
		"c.actors = maps.Clone(boot.managed)",
		"return cloneActive(value), ok, nil",
	} {
		if !strings.Contains(string(controller), required) {
			t.Errorf("Controller coherent-container wall missing %q", required)
		}
	}
	gates, err := os.ReadFile("../runtime/actorctl/gates.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"func (g *controlGates) lockActorSet(",
		"bytes.Compare([]byte(out[i]), []byte(out[j])) < 0",
	} {
		if !strings.Contains(string(gates), required) {
			t.Errorf("canonical multi-gate wall missing %q", required)
		}
	}

	count := 0
	files := parseProductionPackage(t, "../runtime/actorctl")
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			fn, ok := node.(*ast.FuncDecl)
			if ok && fn.Name.Name == "lockActorSet" {
				count++
			}
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "ChannelActors" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range structType.Fields.List {
				for _, name := range field.Names {
					if name.Name == "store" {
						t.Error("ChannelActors command facade retained Controller's Store port")
					}
				}
			}
			return true
		})
	}
	if count != 1 {
		t.Fatalf("multi-control-gate acquisition implementations=%d, want 1", count)
	}

	commands, err := os.ReadFile("../runtime/actorctl/commands.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"a.store.",
		"a.controller.gates",
		"a.controller.placementGate",
		"a.controller.stateMu",
		"a.controller.actors",
	} {
		if strings.Contains(string(commands), forbidden) {
			t.Errorf("command facade bypasses Controller transition owner with %q", forbidden)
		}
	}
	for _, required := range []string{
		"a.controller.admit(ctx, request)",
		"a.controller.introduce(ctx, request)",
		"a.controller.fork(ctx, request)",
		"a.controller.restart(ctx, request)",
		"a.controller.applyDefinitionChange(ctx, change)",
		"a.controller.attachDaemon(ctx, request)",
		"a.controller.terminal(ctx, command)",
	} {
		if !strings.Contains(string(commands), required) {
			t.Errorf("command facade does not delegate complete transition %q", required)
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

func TestHarden03BTerminalCommandSetIsClosed(t *testing.T) {
	body, err := os.ReadFile("../runtime/actorctl/types.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"TerminalEnd", "TerminalRemove", "TerminalDetachDaemon"} {
		if !strings.Contains(string(body), required) {
			t.Errorf("terminal command set missing %s", required)
		}
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
	if count != 3 {
		t.Fatalf("TerminalKind constants=%d, want 3", count)
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

// TestHarden03BServerManagedCapsGateOwnedByActorctl pins the value-ledger gate
// collection: the Server managed Caps (and its physical-current membrane) are
// constructed ONLY inside runtime/actorctl, so platform/home holds no
// ActualCurrent reference and no managed-current facade of its own.
func TestHarden03BServerManagedCapsGateOwnedByActorctl(t *testing.T) {
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
			t.Errorf("%s references actorhost.ActualCurrent — the physical-current fence lives only in runtime/actorctl now", filepath.ToSlash(path))
		}
		if strings.Contains(string(body), "buildManagedCaps") {
			t.Errorf("%s still constructs managed Caps — actorctl is the sole Server managed Caps constructor", filepath.ToSlash(path))
		}
	}

	caps, err := os.ReadFile("../runtime/actorctl/caps.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"type managedInvocation struct {",
		"func (g *managedInvocation) admit() error",
		"func (a *ChannelActors) buildManagedCaps(",
	} {
		if !strings.Contains(string(caps), required) {
			t.Errorf("runtime/actorctl/caps.go missing value-ledger gate anchor %q", required)
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
