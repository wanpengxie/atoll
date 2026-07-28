package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "actorrt 无保存多个 Unit/Incarnation/Actor 的 mutable collection" and
// "actorrt exported API 无 ActorID current/query/mutation、Seal、aggregate Close".
//
// actorrt is the exact-incarnation leaf. A Unit is reached by holding THAT
// Unit — never by naming an ActorID and asking the package to find it. The
// whole managed-lifecycle model rests on that: the Host owns "which incarnation
// is current for this ActorID", so an ActorID→Unit lookup living one layer below
// the Host would let any holder of a bare id deliver to, stop, or observe an
// incarnation the Host has already retired, behind the Host's back and outside
// its span lock.
//
// The regression form is a package-level registry: one `map[actor.ActorID]*Unit`
// plus an exported `Current(id)` is all it takes, and it reads like a
// convenience ("callers keep threading the Unit around; let the package hold
// it"). Nothing in the existing walls looks at actorrt's package-level state or
// at the parameter lists of its exported API — the closest wall
// (TestHarden03BActorRTIsAnExactUnitLeaf) inspects the fields of the `Unit`
// struct only, so a registry declared as a package var, or held by any OTHER
// type in the package, passes it untouched.
//
// This wall is a防回归 wall: actorrt is clean today. It nails the shape, not the
// name, so `unitsByID` / `registry` / `orphanTracker` are all the same
// violation.

const actorrtLeafPkg = "../runtime/actorrt"

// actorrtIncarnationNouns are the per-incarnation nouns of the leaf. A
// collection whose key or element mentions any of them holds MANY incarnations,
// which is the thing actorrt must not be able to do.
var actorrtIncarnationNouns = map[string]bool{
	"Unit":         true,
	"Incarnation":  true,
	"Actor":        true,
	"ActorContext": true,
	"ActorID":      true,
}

// actorrtMentionsIncarnationNoun reports whether a type expression names any
// per-incarnation noun anywhere inside it.
func actorrtMentionsIncarnationNoun(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if ok && actorrtIncarnationNouns[ident.Name] {
			found = true
		}
		return !found
	})
	return found
}

// actorrtIncarnationCollection returns the first map/slice/array/chan inside a
// type expression whose key or element reaches an incarnation noun. Any of the
// four is a "holds several of them" container: `map[actor.ActorID]*Unit` is the
// textbook registry, `[]*Unit` is the same table with linear scan, and
// `chan Incarnation` is the same table smeared over time.
func actorrtIncarnationCollection(fset *token.FileSet, expr ast.Expr) (string, bool) {
	var text string
	ast.Inspect(expr, func(node ast.Node) bool {
		if text != "" {
			return false
		}
		switch node.(type) {
		case *ast.MapType, *ast.ArrayType, *ast.ChanType:
		default:
			return true
		}
		container := node.(ast.Expr)
		if actorrtMentionsIncarnationNoun(container) {
			text = expressionText(fset, container)
			return false
		}
		return true
	})
	return text, text != ""
}

// actorrtIsTypeErasedContainer reports the `sync.Map` / `sync.Pool` form: a
// container that holds anything at all, so no key/element inspection can ever
// tell you whether Units are inside. In a package that is supposed to own zero
// tables, its presence is the violation regardless of what it currently stores.
func actorrtIsTypeErasedContainer(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if ok && pkg.Name == "sync" && (selector.Sel.Name == "Map" || selector.Sel.Name == "Pool") {
			found = true
		}
		return !found
	})
	return found
}

// actorrtPackageLevelRegistries reports package-scoped state that can hold more
// than one incarnation. Package scope is the aggravating factor: such a table
// outlives every Unit in it and is reachable from any function in the package
// without anyone passing a handle.
func actorrtPackageLevelRegistries(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				names := make([]string, 0, len(value.Names))
				for _, name := range value.Names {
					names = append(names, name.Name)
				}
				exprs := make([]ast.Expr, 0, len(value.Values)+1)
				if value.Type != nil {
					exprs = append(exprs, value.Type)
				}
				exprs = append(exprs, value.Values...)
				for _, expr := range exprs {
					if text, bad := actorrtIncarnationCollection(fset, expr); bad {
						v = append(v, fmt.Sprintf(
							"%s:%d package-level var %s holds %s — actorrt keeps no cross-Unit table; a Unit is reached by holding that Unit",
							path, fset.Position(value.Pos()).Line, strings.Join(names, ","), text))
						break
					}
					if actorrtIsTypeErasedContainer(expr) {
						v = append(v, fmt.Sprintf(
							"%s:%d package-level var %s is a sync.Map/sync.Pool — a type-erased table in the exact-incarnation leaf",
							path, fset.Position(value.Pos()).Line, strings.Join(names, ",")))
						break
					}
				}
			}
		}
	}
	return v
}

// actorrtTypeFieldRegistries reports any type in the package — not just `Unit` —
// carrying a field that holds several incarnations. Moving the table off `Unit`
// and onto a new `unitTable`/`orphanTracker` struct is the cheapest way around a
// Unit-struct-only check, and changes nothing about the defect.
func actorrtTypeFieldRegistries(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range structure.Fields.List {
				names := []string{"<embedded>"}
				if len(field.Names) > 0 {
					names = names[:0]
					for _, name := range field.Names {
						names = append(names, name.Name)
					}
				}
				if text, bad := actorrtIncarnationCollection(fset, field.Type); bad {
					v = append(v, fmt.Sprintf(
						"%s:%d %s.%s holds %s — no actorrt type owns a table of incarnations",
						path, fset.Position(field.Pos()).Line, spec.Name.Name, strings.Join(names, ","), text))
				}
				if actorrtIsTypeErasedContainer(field.Type) {
					v = append(v, fmt.Sprintf(
						"%s:%d %s.%s is a sync.Map/sync.Pool — a type-erased table in the exact-incarnation leaf",
						path, fset.Position(field.Pos()).Line, spec.Name.Name, strings.Join(names, ",")))
				}
			}
			return true
		})
	}
	return v
}

// actorrtExportedDecl reports whether a FuncDecl is part of the package's
// exported API: an exported free function, or an exported method on an exported
// type. A method on an unexported type is reachable only through an exported
// interface, which this wall checks separately.
func actorrtExportedDecl(fn *ast.FuncDecl) bool {
	if !fn.Name.IsExported() {
		return false
	}
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return true
	}
	base := fn.Recv.List[0].Type
	if star, ok := base.(*ast.StarExpr); ok {
		base = star.X
	}
	ident, ok := base.(*ast.Ident)
	return ok && ident.IsExported()
}

// actorrtExportedByIDSurface reports every exported entry point that takes an
// ActorID as an argument. That parameter is the whole violation: a function that
// is HANDED an id has to resolve it against something, and the only thing it
// could resolve against is a cross-Unit table. Returning an ActorID
// (`Incarnation.ID`, `ActorContext.Self`) is the opposite direction and stays
// legal — the leaf labels the incarnation it already holds.
func actorrtExportedByIDSurface(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	mentionsActorID := func(expr ast.Expr) bool {
		found := false
		ast.Inspect(expr, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if ok && ident.Name == "ActorID" {
				found = true
			}
			return !found
		})
		return found
	}
	for path, file := range files {
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if !actorrtExportedDecl(node) || node.Type.Params == nil {
					continue
				}
				for _, param := range node.Type.Params.List {
					if mentionsActorID(param.Type) {
						v = append(v, fmt.Sprintf(
							"%s:%d exported %s takes an ActorID (%s) — actorrt resolves nothing by id; the caller holds the exact Unit",
							path, fset.Position(node.Pos()).Line, node.Name.Name, expressionText(fset, param.Type)))
					}
				}
			case *ast.GenDecl:
				if node.Tok != token.TYPE {
					continue
				}
				for _, spec := range node.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || !typeSpec.Name.IsExported() {
						continue
					}
					iface, ok := typeSpec.Type.(*ast.InterfaceType)
					if !ok {
						continue
					}
					for _, method := range iface.Methods.List {
						fnType, ok := method.Type.(*ast.FuncType)
						if !ok || fnType.Params == nil || len(method.Names) == 0 {
							continue
						}
						for _, param := range fnType.Params.List {
							if mentionsActorID(param.Type) {
								v = append(v, fmt.Sprintf(
									"%s:%d exported interface %s.%s takes an ActorID (%s) — the leaf's contracts address one exact incarnation, never a name",
									path, fset.Position(method.Pos()).Line, typeSpec.Name.Name,
									method.Names[0].Name, expressionText(fset, param.Type)))
							}
						}
					}
				}
			}
		}
	}
	return v
}

// actorrtAggregateSignatures reports any signature — exported or not — that
// passes or returns several incarnations at once. `StopAll([]*Unit)` and
// `Seal(map[actor.ActorID]*Unit)` are aggregate verbs even when no field
// anywhere stores the slice: the leaf has no vocabulary for "all of them".
func actorrtAggregateSignatures(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			fields := []*ast.Field{}
			if fn.Type.Params != nil {
				fields = append(fields, fn.Type.Params.List...)
			}
			if fn.Type.Results != nil {
				fields = append(fields, fn.Type.Results.List...)
			}
			for _, field := range fields {
				if text, bad := actorrtIncarnationCollection(fset, field.Type); bad {
					v = append(v, fmt.Sprintf(
						"%s:%d %s has %s in its signature — actorrt has no aggregate verbs, only exact-Unit ones",
						path, fset.Position(fn.Pos()).Line, fn.Name.Name, text))
				}
			}
		}
	}
	return v
}

// actorrtSealAndAggregateClose reports the two named verbs the spec calls out.
// `Seal` is a population verb (freeze admission for everything); a package-level
// `Close`/`CloseAll`/`StopAll`/`Shutdown` is the same idea spelled as teardown.
// Both presuppose a population, and actorrt has none. Per-Unit teardown is
// `(*Unit).Stop`, and it stays.
func actorrtSealAndAggregateClose(files map[string]*ast.File, fset *token.FileSet) []string {
	aggregateFreeFuncs := map[string]bool{
		"Close": true, "CloseAll": true, "StopAll": true, "Shutdown": true, "SealAll": true,
	}
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			line := fset.Position(fn.Pos()).Line
			if strings.HasPrefix(fn.Name.Name, "Seal") {
				v = append(v, fmt.Sprintf(
					"%s:%d declares %s — Seal is a population verb; actorrt owns no population",
					path, line, fn.Name.Name))
				continue
			}
			if fn.Recv == nil && aggregateFreeFuncs[fn.Name.Name] {
				v = append(v, fmt.Sprintf(
					"%s:%d declares package-level %s — an aggregate close presupposes a table of Units; per-Unit teardown is (*Unit).Stop",
					path, line, fn.Name.Name))
			}
		}
	}
	return v
}

func TestActorRTHasNoCrossUnitRegistryOrByIDAPI(t *testing.T) {
	files, fset := loadArchWallPackage(t, actorrtLeafPkg)

	// Footing: the nouns the wall keys on must still be the package's nouns,
	// and its exported API must be non-trivial. Either failing means the wall
	// is scanning something other than the leaf it was written for.
	var hasUnit, hasIncarnation bool
	exported := 0
	for _, file := range files {
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if actorrtExportedDecl(node) {
					exported++
				}
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					switch typeSpec.Name.Name {
					case "Unit":
						hasUnit = true
					case "Incarnation":
						hasIncarnation = true
					}
				}
			}
		}
	}
	if !hasUnit || !hasIncarnation {
		t.Fatalf("actorrt no longer declares Unit(%v)/Incarnation(%v) — the exact-incarnation wall lost its subject",
			hasUnit, hasIncarnation)
	}
	if exported < 5 {
		t.Fatalf("actorrt exports %d entry points — too few for the exported-API wall to be scanning the real leaf", exported)
	}

	failViolations(t, "actorrt owns no package-level cross-Unit registry",
		actorrtPackageLevelRegistries(files, fset))
	failViolations(t, "no actorrt type owns a collection of incarnations",
		actorrtTypeFieldRegistries(files, fset))
	failViolations(t, "actorrt exported API takes no ActorID (no current/query/mutation by id)",
		actorrtExportedByIDSurface(files, fset))
	failViolations(t, "actorrt has no aggregate signatures",
		actorrtAggregateSignatures(files, fset))
	failViolations(t, "actorrt has no Seal and no aggregate Close",
		actorrtSealAndAggregateClose(files, fset))
}

// TestActorRTRegistryWallTripsOnEachBreakForm is the trip proof. The registry
// and by-id-query forms are applied to the REAL unit.go, so the wall is shown to
// fire on production source rather than only on miniatures.
func TestActorRTRegistryWallTripsOnEachBreakForm(t *testing.T) {
	const unitPath = actorrtLeafPkg + "/unit.go"

	t.Run("package-level registry on production source", func(t *testing.T) {
		const anchor = "const cancelSetCap = 256"
		const withRegistry = `// unitsByID lets callers stop threading Units around.
var unitsByID = map[actor.ActorID]*Unit{}

const cancelSetCap = 256`
		files, fset := patchArchWallSource(t, unitPath, archWallPatch{old: anchor, new: withRegistry})
		if got := actorrtPackageLevelRegistries(files, fset); len(got) == 0 {
			t.Fatal("registry wall stayed green on a package-level map[actor.ActorID]*Unit")
		}
	})

	t.Run("exported by-id query on production source", func(t *testing.T) {
		const anchor = "func (u *Unit) Stat() UnitStat {"
		const withQuery = `// Current resolves the live incarnation for an id.
func Current(id actor.ActorID) (Incarnation, bool) {
	return Incarnation{}, false
}

func (u *Unit) Stat() UnitStat {`
		files, fset := patchArchWallSource(t, unitPath, archWallPatch{old: anchor, new: withQuery})
		if got := actorrtExportedByIDSurface(files, fset); len(got) == 0 {
			t.Fatal("by-id wall stayed green on an exported Current(actor.ActorID)")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "registry moved onto a second type",
			src: `package actorrt
type orphanTracker struct {
	mu    sync.Mutex
	byID  map[actor.ActorID]*Unit
}`,
			check: actorrtTypeFieldRegistries,
		},
		{
			name: "type-erased package table",
			src: `package actorrt
var live sync.Map`,
			check: actorrtPackageLevelRegistries,
		},
		{
			name: "aggregate verb over a slice of Units",
			src: `package actorrt
func StopEvery(units []*Unit) {}`,
			check: actorrtAggregateSignatures,
		},
		{
			name: "exported mutation by id on an exported interface",
			src: `package actorrt
type UnitDirectory interface {
	Deliver(id actor.ActorID, env *message.Envelope) error
}`,
			check: actorrtExportedByIDSurface,
		},
		{
			name: "population Seal",
			src: `package actorrt
func Seal() {}`,
			check: actorrtSealAndAggregateClose,
		},
		{
			name: "package-level aggregate Close",
			src: `package actorrt
func Close() error { return nil }`,
			check: actorrtSealAndAggregateClose,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "actorrt_registry_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestActorRTRegistryWallLeavesLegitimateShapesAlone pins the negative half:
// the wall must not fire on the shapes actorrt legitimately has today, or the
// next person will weaken it to get their build green.
func TestActorRTRegistryWallLeavesLegitimateShapesAlone(t *testing.T) {
	const src = `package actorrt
type Unit struct {
	id      actor.ActorID
	inbox   chan *message.Envelope
	cancelQ chan message.ID
	self    Incarnation
	impl    Actor
}
type BuildFailure struct {
	Stack []byte
}
var ErrUnitStopped = errors.New("actorrt: unit stopped")
func (i Incarnation) ID() actor.ActorID { return i.id }
func Prepare(cfg UnitConfig, build func(Incarnation) Actor, sink UnitEventSink) (*Unit, error) {
	return nil, nil
}
func (u *Unit) Stop() {}`
	files, fset := parseArchWallFixtureSource(t, "actorrt_legit_fixture.go", src)
	for rule, got := range map[string][]string{
		"package-level registries": actorrtPackageLevelRegistries(files, fset),
		"type field registries":    actorrtTypeFieldRegistries(files, fset),
		"exported by-id surface":   actorrtExportedByIDSurface(files, fset),
		"aggregate signatures":     actorrtAggregateSignatures(files, fset),
		"Seal / aggregate Close":   actorrtSealAndAggregateClose(files, fset),
	} {
		if len(got) != 0 {
			t.Errorf("%s fired on a legitimate exact-incarnation shape: %s", rule, strings.Join(got, "; "))
		}
	}
}
