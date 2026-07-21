package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const homeImport = "github.com/wanpengxie/atoll/platform/home"

func phaseAProductionFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// .claude/worktrees holds sibling agent checkouts (other branches) that
			// are not this tree's production source; walking them pollutes every
			// repo-root ("..") wall with foreign copies.
			if d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == "vendor" || d.Name() == ".claude" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func phaseAParse(t *testing.T, path string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return fset, f
}

func phaseAImports(f *ast.File, wanted string) bool {
	for _, imp := range f.Imports {
		value, _ := strconv.Unquote(imp.Path.Value)
		if value == wanted {
			return true
		}
	}
	return false
}

// W1: app/gateway/cmd see ChannelHost capabilities, never Home ownership.
func TestChannelRealmW1BundleBoundary(t *testing.T) {
	for _, root := range []string{"../app", "../drivers/gateway", "../cmd/server"} {
		for _, path := range phaseAProductionFiles(t, root) {
			_, f := phaseAParse(t, path)
			if phaseAImports(f, homeImport) {
				t.Errorf("%s imports platform/home", path)
			}
		}
	}
	for _, path := range phaseAProductionFiles(t, "../platform/channelhost") {
		fset, f := phaseAParse(t, path)
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() && strings.Contains(nodeText(fset, d.Type), "home.Home") {
					t.Errorf("%s exports Home in %s", path, d.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() && strings.Contains(nodeText(fset, ts.Type), "*home.Home") {
						t.Errorf("%s exports Home in type %s", path, ts.Name)
					}
				}
			}
		}
	}
}

// W2: the serving operation entry is private and sysactor is its only narrow caller.
func TestChannelRealmW2OperationEntryConfinement(t *testing.T) {
	entryDecls := 0
	for _, path := range phaseAProductionFiles(t, "../platform") {
		_, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if ts.Name.Name == "OpEntry" {
				t.Errorf("%s exports OpEntry", path)
			}
			if ts.Name.Name == "opEntry" {
				entryDecls++
				if path != "../platform/home/opentry.go" {
					t.Errorf("opEntry declared outside home: %s", path)
				}
			}
			return true
		})
	}
	if entryDecls != 1 {
		t.Fatalf("opEntry declarations=%d, want 1", entryDecls)
	}

	// Second wall sentence — "sysactor is the only inter-package caller": the
	// serving-time operate execution crosses the package boundary through exactly
	// two seats. Home binds the sysactor operate contract in one file only
	// (opentry.go implements OperateExecutor), and ChannelHost is the sole
	// assembly point that bridges Home's private opEntry out as SystemOps. Pinning
	// this closed set turns CI red the moment a new caller appears.
	operateContractRefs := map[string]int{}
	for _, path := range phaseAProductionFiles(t, "..") {
		if strings.HasPrefix(path, "../platform/internal/sysactor/") {
			continue // sysactor owns the contract and is its sole invoker.
		}
		_, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != "sysactor" || sel.Sel.Name != "OperateExecutor" {
				return true
			}
			operateContractRefs[path]++
			if path != "../platform/home/opentry.go" {
				t.Errorf("%s references sysactor.OperateExecutor outside Home's sole binding", path)
			}
			return true
		})
	}
	if operateContractRefs["../platform/home/opentry.go"] == 0 {
		t.Error("Home no longer binds sysactor.OperateExecutor in opentry.go")
	}

	systemOpsCallers := map[string]int{}
	for _, path := range phaseAProductionFiles(t, "..") {
		_, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "home" && sel.Sel.Name == "SystemOps" {
				systemOpsCallers[path]++
				if path != "../platform/channelhost/channelhost.go" {
					t.Errorf("%s bridges home.SystemOps outside the ChannelHost assembly point", path)
				}
			}
			return true
		})
	}
	if systemOpsCallers["../platform/channelhost/channelhost.go"] != 1 {
		t.Errorf("home.SystemOps assembly-point call sites in channelhost=%d, want 1", systemOpsCallers["../platform/channelhost/channelhost.go"])
	}
}

// W3: retired Home ports and world-injected executors/planners stay absent.
func TestChannelRealmW3DeadPortsAndInjectionClosed(t *testing.T) {
	retiredMethods := map[string]bool{
		"Restart": true, "PresenceSweptCount": true, "CancelRequest": true,
		"EditDeclaration": true, "ApplyDeclaration": true,
		"EnsureSubjectSlot": true, "RemoveSubjectSlot": true,
	}
	for _, path := range phaseAProductionFiles(t, "../platform/home") {
		_, f := phaseAParse(t, path)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv != nil && retiredMethods[fn.Name.Name] {
				t.Errorf("%s retains retired Home method %s", path, fn.Name)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if ok && id.Name == "OperateExecutor" && path != "../platform/home/opentry.go" {
				t.Errorf("%s reaches sysactor OperateExecutor", path)
			}
			return true
		})
	}
	for _, target := range []struct{ dir, typ string }{{"../platform/home", "Config"}, {"../platform/channelhost", "HomeDeps"}} {
		for _, path := range phaseAProductionFiles(t, target.dir) {
			fset, f := phaseAParse(t, path)
			ast.Inspect(f, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok || ts.Name.Name != target.typ {
					return true
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					return false
				}
				for _, field := range st.Fields.List {
					shape := strings.ToLower(nodeText(fset, field.Type))
					for _, name := range field.Names {
						shape += " " + strings.ToLower(name.Name)
					}
					for _, forbidden := range []string{"executor", "planprovider", "daemonauthority", "operateexecutor"} {
						if strings.Contains(shape, forbidden) {
							t.Errorf("%s %s injects forbidden %s via %s", path, target.typ, forbidden, shape)
						}
					}
				}
				return false
			})
		}
	}
	for _, path := range phaseAProductionFiles(t, "../platform/internal/link") {
		fset, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "Config" {
				return true
			}
			if st, ok := ts.Type.(*ast.StructType); ok {
				for _, field := range st.Fields.List {
					shape := nodeText(fset, field)
					if strings.Contains(shape, "PlanProvider") || strings.Contains(shape, "DaemonAuthority") {
						t.Errorf("%s link.Config retains %s", path, shape)
					}
				}
			}
			return false
		})
	}
}

// W4: published lifecycle is tombstone-only; physical deletion is provision cleanup only.
func TestChannelRealmW4NoPhysicalDestroy(t *testing.T) {
	removeCalls := 0
	for _, path := range phaseAProductionFiles(t, "../platform/channelhost") {
		fset, f := phaseAParse(t, path)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && nodeText(fset, sel.X) == "os" && sel.Sel.Name == "Remove" {
					removeCalls++
					if fn.Name.Name != "Provision" {
						t.Errorf("%s %s physically deletes channel bytes", path, fn.Name)
					}
				}
				return true
			})
		}
	}
	if removeCalls != 1 {
		t.Fatalf("channelhost os.Remove calls=%d, want sole unpublished Provision cleanup", removeCalls)
	}
	body, err := os.ReadFile("../platform/channelhost/channelhost.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "renameNoReplace(") {
		t.Fatal("channelhost tombstone path does not go through renameNoReplace")
	}
	// The no-replace guarantee lives in per-platform files: each platform in the
	// closed set must implement renameNoReplace with its exclusive-rename flag.
	for _, platform := range []struct{ file, flag string }{
		{"rename_linux.go", "unix.RENAME_NOREPLACE"},
		{"rename_darwin.go", "unix.RENAME_EXCL"},
	} {
		impl, err := os.ReadFile("../platform/channelhost/" + platform.file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(impl), platform.flag) {
			t.Fatalf("%s tombstone rename is not no-replace (%s missing)", platform.file, platform.flag)
		}
	}
	for _, path := range phaseAProductionFiles(t, "../app") {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"home.Open(", "db_path", "map[channel.ID]*home.Home", "map[channel.ID] *home.Home"} {
			if strings.Contains(string(body), forbidden) {
				t.Errorf("%s retains physical Home ownership token %q", path, forbidden)
			}
		}
	}
}

// W5: lifecycle verbs have one production owner; ChannelHost construction stays at cmd.
func TestChannelRealmW5LifecycleCallsites(t *testing.T) {
	for _, path := range phaseAProductionFiles(t, "../app") {
		fset, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.SelectorExpr)
			if ok && x.Sel.Name == "host" {
				switch sel.Sel.Name {
				case "Provision", "Destroy", "Open", "Census":
					if path != "../app/channel_lifecycle.go" {
						t.Errorf("%s calls host.%s outside lifecycle owner", path, sel.Sel.Name)
					}
				}
			}
			_ = fset
			return true
		})
	}
	for _, path := range phaseAProductionFiles(t, "..") {
		_, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "channelhost" && sel.Sel.Name == "New" && path != "../cmd/server/main.go" {
					t.Errorf("%s constructs ChannelHost outside cmd/server", path)
				}
			}
			return true
		})
	}
}

// W6: each lifecycle runner has exactly one implementation shared by inline and worker paths.
func TestChannelRealmW6SingleJobRunners(t *testing.T) {
	want := map[string]int{"runProvisionJob": 0, "runDestroyJob": 0}
	for _, path := range phaseAProductionFiles(t, "../app") {
		_, f := phaseAParse(t, path)
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				if _, tracked := want[fn.Name.Name]; tracked {
					want[fn.Name.Name]++
				}
			}
		}
	}
	for name, count := range want {
		if count != 1 {
			t.Errorf("%s definitions=%d, want 1", name, count)
		}
	}

	// Tracked governance ledger: every realm runner names both its implementation
	// owner and durable anchors. Keeping this in archtest makes a clean clone carry
	// the wall even though the project-management corpus itself is workspace-local.
	runners := []struct {
		name    string
		source  string
		typ     string
		anchors []string
	}{
		{"channel lifecycle worker", "../app/channel_lifecycle.go", "type lifecycleWorker struct", []string{"channel_provision_jobs", "channel_destroy_jobs"}},
		{"admission service", "../app/admission.go", "type admissionService struct", []string{"channel_admission_operations"}},
	}
	schema, err := os.ReadFile("../app/store.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, runner := range runners {
		body, err := os.ReadFile(runner.source)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), runner.typ) {
			t.Errorf("registered %s implementation %q is absent", runner.name, runner.typ)
		}
		for _, anchor := range runner.anchors {
			if !strings.Contains(string(schema), `"table", "`+anchor+`"`) {
				t.Errorf("registered %s durable anchor %q is absent from strict schema", runner.name, anchor)
			}
		}
	}
}

// W7: Bundle.SysOp is consumed only by admissionService. Declaration
// convergence is Home-owned and uses a private store port.
func TestChannelRealmW7SysOpConsumptionClosed(t *testing.T) {
	allowed := map[string]bool{"../app/admission.go": true}
	for _, path := range phaseAProductionFiles(t, "../app") {
		fset, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "SysOp" && !allowed[path] {
				t.Errorf("%s consumes Bundle.SysOp outside admission", path)
			}
			_ = fset
			return true
		})
	}
}

// W8: the removed management-domain workspace vocabulary cannot return as code symbols.
func TestChannelRealmW8WorkspaceManagementAbsent(t *testing.T) {
	for _, root := range []string{"../app", "../drivers/gateway", "../cmd/server"} {
		for _, path := range phaseAProductionFiles(t, root) {
			_, f := phaseAParse(t, path)
			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if ok {
					name := strings.ToLower(id.Name)
					if strings.Contains(name, "workspaceid") || strings.Contains(name, "workspacemember") || name == "workspaces" {
						t.Errorf("%s retains workspace management identifier %s", path, id.Name)
					}
				}
				return true
			})
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, token := range []string{"workspace_id", "workspace_members", "/workspaces"} {
				if strings.Contains(string(body), token) {
					t.Errorf("%s retains workspace management token %q", path, token)
				}
			}
		}
	}
}

// W9: both realm and channel schemas retain the exact Phase-A accountability seats.
func TestChannelRealmW9StrictSchemaSeats(t *testing.T) {
	appSchema, err := os.ReadFile("../app/store.go")
	if err != nil {
		t.Fatal(err)
	}
	appText := string(appSchema)
	for _, required := range []string{
		"CREATE TABLE channels", "parent_id TEXT", "compensation_job_id INTEGER",
		"CREATE TABLE channel_admission_operations", "attempt INTEGER", "next_attempt_at INTEGER",
		"CREATE TABLE channel_decl_overlays", "config_json TEXT",
		"CREATE TABLE principal_channels",
	} {
		if !strings.Contains(appText, required) {
			t.Errorf("app schema missing %q", required)
		}
	}
	for _, forbidden := range []string{"CREATE TABLE workspaces", "CREATE TABLE workspace_members", "targets_json",
		"CREATE TABLE decl_fanout_jobs", "CREATE TABLE daemon_revoke_jobs", "CREATE TABLE decl_fanout_deliveries",
		"CREATE TABLE channel_finalize_deliveries", "CREATE TABLE decl_render_state", "pending_config_json", "pending_ref"} {
		if strings.Contains(appText, forbidden) {
			t.Errorf("app schema retains %q", forbidden)
		}
	}
	localSchema, err := os.ReadFile("../runtime/internal/store/schema.go")
	if err != nil {
		t.Fatal(err)
	}
	localText := string(localSchema)
	for _, required := range []string{
		"channel_genesis", "channel_daemon_bindings", "ux_sysop_completed_correlation",
		"source_channel_id", "source_resource_id",
	} {
		if !strings.Contains(localText, required) {
			t.Errorf("channel schema missing %q", required)
		}
	}
	for _, forbidden := range []string{"restart_applied", "restart_attempts", "render_seq"} {
		if strings.Contains(localText, forbidden) {
			t.Errorf("channel schema retains %q", forbidden)
		}
	}
}

// W10: requirements remain typed and neutral; Home cannot point back into app.
func TestChannelRealmW10RequirementNeutrality(t *testing.T) {
	for _, path := range []string{"../platform/home/requirements.go", "../platform/home/composition.go", "../platform/home/home.go"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(body))
		for _, forbidden := range []string{"channel_admission_operations", "actor_decls", "principal_channels", "database/sql", "app."} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("%s requirement surface leaks realm storage token %q", path, forbidden)
			}
		}
	}
	for _, path := range phaseAProductionFiles(t, "../platform/home") {
		_, f := phaseAParse(t, path)
		if phaseAImports(f, "github.com/wanpengxie/atoll/app") {
			t.Errorf("%s imports app", path)
		}
	}
}

// W11: observer Reader minting is centralized in canObserve; member Readers are out of scope.
func TestChannelRealmW11ObserverReadClosure(t *testing.T) {
	constructors := 0
	for _, path := range phaseAProductionFiles(t, "..") {
		_, f := phaseAParse(t, path)
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			ast.Inspect(lit, func(child ast.Node) bool {
				sel, ok := child.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "ReaderObserver" {
					return true
				}
				constructors++
				if path != "../app/channel_read.go" {
					t.Errorf("%s constructs observer Reader outside canObserve", path)
				}
				return true
			})
			return true
		})
	}
	if constructors != 1 {
		t.Fatalf("observer Reader construction sites=%d, want 1", constructors)
	}
	body, err := os.ReadFile("../app/channel_read.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "func (a *App) canObserve") || !strings.Contains(string(body), "DeclaredBySource") {
		t.Fatal("canObserve is not the realm-tool-backed observer gate")
	}
}
