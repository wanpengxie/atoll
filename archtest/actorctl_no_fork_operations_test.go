package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "actorctl 无 ForkOperations/fork receipt map".
//
// Fork is a Controller COMMAND, not a subsystem. It is the one command whose
// child id is freshly minted, so truth itself cannot answer "did I already do
// this"; the Controller therefore keeps one private in-process replay table
// (`forks map[forkKey]forkEntry`, runtime/actorctl/fork.go) consulted inside the
// same ledger lock as the command it protects. That table is a mechanism, not a
// concept: it has no name in the model, no reader outside the command, and no
// projection anyone may hold.
//
// Two regression forms, both of which the spec names:
//
//   - `ForkOperations` — hoisting fork out of the Controller into its own
//     handle/interface. Whoever holds that handle can fork without going through
//     the Controller's single ledger lock, so the "one command, one complete
//     ledger change" property stops being structural: it becomes a convention
//     two objects have to agree on.
//   - an exported fork receipt map — handing out the replay table (or a
//     projection of it) as `ForkReceipts()` / a `Receipts map[...]` field. The
//     table is only correct read under the ledger lock at the instant of the
//     retry decision; a copy escaping the lock is a stale answer to "did this
//     fork already happen", and acting on a stale answer births a second child.
//
// The existing neighbouring wall pins the private table's EXISTENCE. Nothing
// looks at whether an exported surface has grown around it, and nothing forbids
// the `ForkOperations` token anywhere in the tree.
//
// 防回归 wall: the tree is clean today — `ForkOperations` has zero hits and the
// only exported fork vocabulary is the command verb plus its two DTOs.

const actorctlPkg = "../runtime/actorctl"

// actorctlForkTableField is the private replay table the wall is built around.
const actorctlForkTableField = "forks"

// actorctlForkTableReaders is the closed set of functions that may touch the
// replay table: the constructor that creates it and the command it exists for.
// Anything else touching it is the beginning of an accessor.
var actorctlForkTableReaders = map[string]bool{"New": true, "Fork": true}

// actorctlExportedForkTypes is the closed exported fork vocabulary: the request
// and result DTOs of the command. A third exported fork noun is a fork
// subsystem announcing itself.
var actorctlExportedForkTypes = map[string]bool{"ForkRequest": true, "ForkResult": true}

// actorctlExportedForkFuncs is the closed exported fork verb set: the command.
var actorctlExportedForkFuncs = map[string]bool{"Fork": true}

// actorctlForkOperationsToken reports every appearance of the retired
// `ForkOperations` name anywhere in production — declaration or use, in any
// package. Scanning identifiers rather than raw text means prose about the
// retired shape (including this file's own rationale) does not count.
func actorctlForkOperationsToken(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok || !strings.Contains(ident.Name, "ForkOperations") {
				return true
			}
			v = append(v, fmt.Sprintf(
				"%s:%d names %s — fork is a Controller command; hoisting it into its own handle takes the ledger lock out of the guarantee",
				path, fset.Position(ident.Pos()).Line, ident.Name))
			return true
		})
	}
	return v
}

// actorctlForkSubsystemSurface reports exported fork vocabulary outside the
// closed sets, and any exported interface that declares a fork verb. The
// interface half is what catches a `ForkOperations` renamed to something
// innocuous: a sub-handle is recognisable by its METHOD, whatever the type is
// called.
func actorctlForkSubsystemSurface(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if !node.Name.IsExported() || !strings.Contains(node.Name.Name, "Fork") {
					continue
				}
				if actorctlExportedForkFuncs[node.Name.Name] {
					continue
				}
				v = append(v, fmt.Sprintf(
					"%s:%d exports fork verb %s — the exported fork surface is the single command Fork",
					path, fset.Position(node.Pos()).Line, node.Name.Name))
			case *ast.GenDecl:
				if node.Tok != token.TYPE {
					continue
				}
				for _, spec := range node.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || !typeSpec.Name.IsExported() {
						continue
					}
					line := fset.Position(typeSpec.Pos()).Line
					if strings.Contains(typeSpec.Name.Name, "Fork") && !actorctlExportedForkTypes[typeSpec.Name.Name] {
						v = append(v, fmt.Sprintf(
							"%s:%d exports fork noun %s — the exported fork vocabulary is ForkRequest/ForkResult, the command's own DTOs",
							path, line, typeSpec.Name.Name))
					}
					iface, ok := typeSpec.Type.(*ast.InterfaceType)
					if !ok {
						continue
					}
					for _, method := range iface.Methods.List {
						for _, name := range method.Names {
							if strings.Contains(name.Name, "Fork") {
								v = append(v, fmt.Sprintf(
									"%s:%d exported interface %s declares %s — a fork port is a fork subsystem under another name",
									path, fset.Position(method.Pos()).Line, typeSpec.Name.Name, name.Name))
							}
						}
					}
				}
			}
		}
	}
	return v
}

// actorctlExportedMapSurface reports every exported signature or struct field in
// actorctl carrying a map. The Controller's tables — the member ledger and the
// fork replay table — are correct only under its one lock; a map crossing the
// exported boundary is either that table or a snapshot of it, and both invite
// callers to make decisions on state the lock no longer covers. The Controller
// hands out values and slices, never tables.
func actorctlExportedMapSurface(files map[string]*ast.File, fset *token.FileSet) []string {
	hasMap := func(expr ast.Expr) (string, bool) {
		var text string
		ast.Inspect(expr, func(node ast.Node) bool {
			if text != "" {
				return false
			}
			if mapType, ok := node.(*ast.MapType); ok {
				text = expressionText(fset, mapType)
				return false
			}
			return true
		})
		return text, text != ""
	}
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if !node.Name.IsExported() {
					continue
				}
				fields := []*ast.Field{}
				if node.Type.Params != nil {
					fields = append(fields, node.Type.Params.List...)
				}
				if node.Type.Results != nil {
					fields = append(fields, node.Type.Results.List...)
				}
				for _, field := range fields {
					if text, bad := hasMap(field.Type); bad {
						v = append(v, fmt.Sprintf(
							"%s:%d exported %s carries %s in its signature — Controller tables do not cross the ledger lock",
							path, fset.Position(node.Pos()).Line, node.Name.Name, text))
					}
				}
			case *ast.GenDecl:
				if node.Tok != token.TYPE {
					continue
				}
				for _, spec := range node.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					structure, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range structure.Fields.List {
						for _, name := range field.Names {
							if !name.IsExported() {
								continue
							}
							if text, bad := hasMap(field.Type); bad {
								v = append(v, fmt.Sprintf(
									"%s:%d %s.%s is an exported %s — a receipt/member table must not be a field anyone can hold",
									path, fset.Position(field.Pos()).Line, typeSpec.Name.Name, name.Name, text))
							}
						}
					}
				}
			}
		}
	}
	return v
}

// actorctlForkTableConfinement reports every function other than the
// constructor and the Fork command that touches the replay table. An accessor
// starts life as a reader: `func (c *Controller) ForkedChild(...)` reads
// `c.forks` and looks harmless, and once a second reader exists the table has
// become a queryable projection with two callers' worth of expectations on it.
func actorctlForkTableConfinement(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	touches := 0
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != actorctlForkTableField {
					return true
				}
				touches++
				if actorctlForkTableReaders[fn.Name.Name] {
					return true
				}
				v = append(v, fmt.Sprintf(
					"%s:%d %s touches the private fork replay table — it is consulted by the Fork command alone, inside the ledger lock",
					path, fset.Position(selector.Pos()).Line, fn.Name.Name))
				return true
			})
		}
	}
	if touches == 0 {
		v = append(v, fmt.Sprintf(
			"no function touches %q — the fork replay table moved or vanished; retune this wall",
			actorctlForkTableField))
	}
	return v
}

func TestActorCtlHasNoForkSubsystemOrReceiptMap(t *testing.T) {
	files, fset := loadArchWallPackage(t, actorctlPkg)

	// Footing: the private table must still be exactly that — private, and
	// keyed/valued by private types. An exported key or value type is already
	// half a receipt API even before an accessor exists.
	source, err := readFile(actorctlPkg + "/controller.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "forks  map[forkKey]forkEntry") {
		t.Fatal("Controller no longer holds `forks map[forkKey]forkEntry` — retune the fork-surface wall")
	}

	failViolations(t, "actorctl exports no fork subsystem", actorctlForkSubsystemSurface(files, fset))
	failViolations(t, "actorctl exports no map surface (no receipt table escapes the ledger lock)",
		actorctlExportedMapSurface(files, fset))
	failViolations(t, "the fork replay table is reachable only from the Fork command",
		actorctlForkTableConfinement(files, fset))

	repo, repoFset := loadArchWallPackage(t, "..")
	failViolations(t, "ForkOperations exists nowhere in production", actorctlForkOperationsToken(repo, repoFset))
}

// TestActorCtlForkSurfaceWallTripsOnEachBreakForm is the trip proof; the
// accessor and receipt-map forms are applied to the REAL fork.go / controller.go.
func TestActorCtlForkSurfaceWallTripsOnEachBreakForm(t *testing.T) {
	const forkPath = actorctlPkg + "/fork.go"
	const controllerPath = actorctlPkg + "/controller.go"

	t.Run("receipt accessor on production source", func(t *testing.T) {
		const anchor = "// Fork births one entry-table record."
		const withAccessor = `// ForkReceipts exposes the replay table "for diagnostics".
func (c *Controller) ForkReceipts() map[actor.ActorID]actor.ActorID {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	out := map[actor.ActorID]actor.ActorID{}
	for key, entry := range c.forks {
		out[key.caller] = entry.child
	}
	return out
}

// Fork births one entry-table record.`
		files, fset := patchArchWallSource(t, forkPath, archWallPatch{old: anchor, new: withAccessor})
		if got := actorctlForkSubsystemSurface(files, fset); len(got) == 0 {
			t.Fatal("fork surface wall stayed green on an exported ForkReceipts accessor")
		}
		if got := actorctlExportedMapSurface(files, fset); len(got) == 0 {
			t.Fatal("map surface wall stayed green on an exported map[...]... receipt projection")
		}
		if got := actorctlForkTableConfinement(files, fset); len(got) == 0 {
			t.Fatal("table confinement wall stayed green on a second reader of c.forks")
		}
	})

	t.Run("fork port hoisted out of the Controller on production source", func(t *testing.T) {
		const anchor = "// Controller is the sole owner of the managed actor value ledger."
		const withPort = `// ForkOperations groups the fork verbs for callers that only fork.
type ForkOperations interface {
	Fork(ctx context.Context, request ForkRequest) (Transition[ForkResult], error)
}

// Controller is the sole owner of the managed actor value ledger.`
		files, fset := patchArchWallSource(t, controllerPath, archWallPatch{old: anchor, new: withPort})
		if got := actorctlForkOperationsToken(files, fset); len(got) == 0 {
			t.Fatal("ForkOperations token wall stayed green on a declared ForkOperations interface")
		}
		if got := actorctlForkSubsystemSurface(files, fset); len(got) == 0 {
			t.Fatal("fork surface wall stayed green on an exported fork port")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "receipt table as an exported field",
			src: `package actorctl
type ForkLedger struct {
	Receipts map[forkKey]forkEntry
}`,
			check: actorctlExportedMapSurface,
		},
		{
			name: "renamed fork port",
			src: `package actorctl
type LifecyclePort interface {
	ForkChild(request ForkRequest) (ForkResult, error)
}`,
			check: actorctlForkSubsystemSurface,
		},
		{
			name: "fork token in a foreign package",
			src: `package actorhost
type hostForkOperationsShim struct{}`,
			check: actorctlForkOperationsToken,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "actorctl_fork_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestActorCtlForkSurfaceWallLeavesTheCommandAlone pins the negative half: the
// command, its DTOs and the private table must all stay green, or the wall
// would be pushing the legitimate design out.
func TestActorCtlForkSurfaceWallLeavesTheCommandAlone(t *testing.T) {
	const src = `package actorctl
type forkKey struct {
	caller  actor.ActorID
	request message.ID
}
type forkEntry struct {
	child  actor.ActorID
	digest string
}
type ForkRequest struct{ CallerActorID actor.ActorID }
type ForkResult struct{ Child actor.ActorID }
type Controller struct {
	actors map[actor.ActorID]managedActor
	forks  map[forkKey]forkEntry
}
func New(store Store, nowMs func() int64) (*Controller, error) {
	return &Controller{forks: make(map[forkKey]forkEntry)}, nil
}
func (c *Controller) Fork(ctx context.Context, request ForkRequest) (Transition[ForkResult], error) {
	key := forkKey{caller: request.CallerActorID}
	if entry, found := c.forks[key]; found {
		_ = entry
	}
	c.forks[key] = forkEntry{}
	return Transition[ForkResult]{}, nil
}
func normalizeFork(spec actorcaps.ForkSpec) actorcaps.ForkSpec { return spec }`
	files, fset := parseArchWallFixtureSource(t, "actorctl_legit_fixture.go", src)
	for rule, got := range map[string][]string{
		"fork subsystem surface": actorctlForkSubsystemSurface(files, fset),
		"exported map surface":   actorctlExportedMapSurface(files, fset),
		"table confinement":      actorctlForkTableConfinement(files, fset),
		"ForkOperations token":   actorctlForkOperationsToken(files, fset),
	} {
		if len(got) != 0 {
			t.Errorf("%s fired on the legitimate fork command shape: %s", rule, strings.Join(got, "; "))
		}
	}
}
