package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "`Stat` 只接 exact Unit".
//
// `Stat` is the substrate's read-only observation of one incarnation:
// "when did THIS body start, and what kind is it". Today the clause is held by
// Go's own method-receiver syntax — the single declaration is
// `func (u *Unit) Stat() UnitStat` on `../runtime/actorrt/unit.go`, zero
// parameters, so the only way to ask is to already hold the exact Unit.
//
// The regression form is the free-function version: `Stat(id)` — the package
// takes a name and answers about whatever body it can find under that name.
// That is a lie at the level of the model, not a convenience: an ActorID has no
// single body over time, so `Stat(id)` silently answers about SOME incarnation,
// and callers who compare its StartedAt to make a liveness decision will
// eventually be comparing a successor's start against a predecessor's death.
// Layers above actorrt legitimately expose `Stat(id)` (`View.Stat`,
// `actorSystem.Stat`) — they are methods on the owners that hold the
// ActorID→current-incarnation truth and can resolve it correctly. The leaf may
// not, because it does not hold that truth.
//
// The wall keys on the SHAPE, not on the name: any actorrt declaration that
// produces a `UnitStat` must be a zero-parameter method on `*Unit`. So
// `StatOf(id)`, `StatFor(id)`, `Lookup(id) UnitStat` and a `Stat` hung on some
// new table type are all the same violation, and renaming does not escape it.
//
// 防回归 wall: actorrt is clean today.

// actorrtStatMethodName is the one verb this wall is about.
const actorrtStatMethodName = "Stat"

// actorrtStatResultType is the type whose production is confined to the exact
// method. Keying on the produced TYPE is what makes the wall rename-proof.
const actorrtStatResultType = "UnitStat"

// actorrtStatReceiverIsExactUnit reports whether a receiver is exactly
// `*Unit` — the exact incarnation, not a table, a manager, or a value copy.
func actorrtStatReceiverIsExactUnit(recv *ast.FieldList) bool {
	if recv == nil || len(recv.List) != 1 {
		return false
	}
	star, ok := recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	return ok && ident.Name == "Unit"
}

// actorrtStatProducers reports every actorrt declaration producing a UnitStat
// that is not a zero-parameter method on the exact Unit. A parameter is the
// tell: a stat that needs an argument is resolving something, and resolution is
// exactly what the leaf must be unable to do.
func actorrtStatProducers(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	produces := func(results *ast.FieldList) bool {
		if results == nil {
			return false
		}
		found := false
		for _, result := range results.List {
			ast.Inspect(result.Type, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if ok && ident.Name == actorrtStatResultType {
					found = true
				}
				return !found
			})
		}
		return found
	}
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !produces(fn.Type.Results) {
				continue
			}
			line := fset.Position(fn.Pos()).Line
			if fn.Recv == nil {
				v = append(v, fmt.Sprintf(
					"%s:%d free function %s produces a %s — a stat that is not reached through the exact Unit must be resolving an id",
					path, line, fn.Name.Name, actorrtStatResultType))
				continue
			}
			if !actorrtStatReceiverIsExactUnit(fn.Recv) {
				v = append(v, fmt.Sprintf(
					"%s:%d %s produces a %s on receiver %s — only *Unit may report its own stat",
					path, line, fn.Name.Name, actorrtStatResultType, expressionText(fset, fn.Recv.List[0].Type)))
				continue
			}
			if fn.Type.Params != nil && len(fn.Type.Params.List) != 0 {
				v = append(v, fmt.Sprintf(
					"%s:%d (*Unit).%s takes %d parameter(s) — the exact Unit is the whole question; there is nothing left to pass in",
					path, line, fn.Name.Name, len(fn.Type.Params.List)))
			}
		}
	}
	return v
}

// actorrtStatNameHolders reports every actorrt declaration that claims the
// name `Stat` — function, method, or type. The shape rule above is the primary
// wall; this one keeps the name itself from being reused for something that is
// not a UnitStat (e.g. `type Stat struct{...}` plus `func Stat(id) Stat`, which
// would sidestep a type-keyed rule).
func actorrtStatNameHolders(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if node.Name.Name != actorrtStatMethodName {
					continue
				}
				line := fset.Position(node.Pos()).Line
				if !actorrtStatReceiverIsExactUnit(node.Recv) {
					v = append(v, fmt.Sprintf(
						"%s:%d Stat is declared without an exact *Unit receiver — the free-function form is the regression this wall exists for",
						path, line))
					continue
				}
				if node.Type.Params != nil && len(node.Type.Params.List) != 0 {
					v = append(v, fmt.Sprintf(
						"%s:%d (*Unit).Stat grew parameters — Stat answers about the receiver and nothing else", path, line))
				}
			case *ast.GenDecl:
				if node.Tok != token.TYPE {
					continue
				}
				for _, spec := range node.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if ok && typeSpec.Name.Name == actorrtStatMethodName {
						v = append(v, fmt.Sprintf(
							"%s:%d declares type Stat — the name belongs to the exact-Unit method; UnitStat is the shape it returns",
							path, fset.Position(typeSpec.Pos()).Line))
					}
				}
			}
		}
	}
	return v
}

// actorrtStatFreeFunctionEscapes reports free functions ANYWHERE in production
// that produce an `actorrt.UnitStat`. Confining the leaf alone is not enough:
// a helper package can host `func StatOf(id actor.ActorID) actorrt.UnitStat`
// and reintroduce exactly the by-name stat the leaf refuses to have, with the
// same "some incarnation" defect. Methods are untouched — the owners that hold
// ActorID→current truth (View, actorSystem, Kernel) legitimately report stats.
func actorrtStatFreeFunctionEscapes(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		if strings.Contains(path, "/runtime/actorrt/") {
			continue // the leaf itself is covered by the exact-shape rule above
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Type.Results == nil {
				continue
			}
			for _, result := range fn.Type.Results.List {
				found := false
				ast.Inspect(result.Type, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkg, ok := selector.X.(*ast.Ident)
					if ok && pkg.Name == "actorrt" && selector.Sel.Name == actorrtStatResultType {
						found = true
					}
					return !found
				})
				if found {
					v = append(v, fmt.Sprintf(
						"%s:%d free function %s returns actorrt.UnitStat — a stat with no owning holder is a by-name stat wearing a different package",
						path, fset.Position(fn.Pos()).Line, fn.Name.Name))
					break
				}
			}
		}
	}
	return v
}

func TestActorRTStatIsReachableOnlyThroughTheExactUnit(t *testing.T) {
	files, fset := loadArchWallPackage(t, actorrtLeafPkg)

	// Footing: the one declaration must be there in the exact shape the wall
	// claims to protect. If it moves or changes, the wall reports that rather
	// than passing on an empty scan.
	source, err := readFile(actorrtLeafPkg + "/unit.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "func (u *Unit) Stat() UnitStat {") {
		t.Fatal("unit.go no longer declares `func (u *Unit) Stat() UnitStat` — retune the exact-Unit stat wall")
	}

	failViolations(t, "every actorrt UnitStat is produced by the exact Unit itself",
		actorrtStatProducers(files, fset))
	failViolations(t, "the name Stat in actorrt belongs to the exact-Unit method",
		actorrtStatNameHolders(files, fset))

	repo, repoFset := loadArchWallPackage(t, "..")
	failViolations(t, "no production free function hands out an actorrt.UnitStat",
		actorrtStatFreeFunctionEscapes(repo, repoFset))
}

// TestActorRTStatWallTripsOnTheFreeFunctionForm is the trip proof. The two
// in-leaf forms are applied to the REAL unit.go.
func TestActorRTStatWallTripsOnTheFreeFunctionForm(t *testing.T) {
	const unitPath = actorrtLeafPkg + "/unit.go"

	t.Run("method rewritten as Stat(id) on production source", func(t *testing.T) {
		const anchor = "func (u *Unit) Stat() UnitStat {"
		const asFreeFunc = `func Stat(id actor.ActorID) UnitStat {
	u := unitsByID[id]`
		files, fset := patchArchWallSource(t, unitPath, archWallPatch{old: anchor, new: asFreeFunc})
		if got := actorrtStatProducers(files, fset); len(got) == 0 {
			t.Fatal("stat wall stayed green on the free-function Stat(id) form")
		}
		if got := actorrtStatNameHolders(files, fset); len(got) == 0 {
			t.Fatal("stat name wall stayed green on the free-function Stat(id) form")
		}
	})

	t.Run("renamed by-id producer on production source", func(t *testing.T) {
		const anchor = "func (u *Unit) Stat() UnitStat {"
		const withStatFor = `// StatFor reports the stat of a peer incarnation.
func (u *Unit) StatFor(id actor.ActorID) UnitStat {
	return UnitStat{}
}

func (u *Unit) Stat() UnitStat {`
		files, fset := patchArchWallSource(t, unitPath, archWallPatch{old: anchor, new: withStatFor})
		if got := actorrtStatProducers(files, fset); len(got) == 0 {
			t.Fatal("stat wall stayed green on a renamed by-id UnitStat producer")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "stat hung on a new table type",
			src: `package actorrt
func (t *unitTable) Stat(id actor.ActorID) UnitStat { return UnitStat{} }`,
			check: actorrtStatProducers,
		},
		{
			name: "the name reused for a struct",
			src: `package actorrt
type Stat struct{ StartedAt time.Time }`,
			check: actorrtStatNameHolders,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "actorrt_stat_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}

	t.Run("by-id stat helper in another package", func(t *testing.T) {
		const src = `package home
func StatOf(id actor.ActorID) actorrt.UnitStat { return actorrt.UnitStat{} }`
		files, fset := parseArchWallFixtureSource(t, "../platform/home/stat_fixture.go", src)
		if got := actorrtStatFreeFunctionEscapes(files, fset); len(got) == 0 {
			t.Fatal("stat escape wall stayed green on a free StatOf(id) helper outside actorrt")
		}
	})
}

// TestActorRTStatWallLeavesOwnerMethodsAlone pins the negative half: the layers
// that DO hold ActorID→current truth must keep reporting stats by id, or the
// wall would be pushing a correct design out of the tree.
func TestActorRTStatWallLeavesOwnerMethodsAlone(t *testing.T) {
	const src = `package home
func (a *actorSystem) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	return actorrt.UnitStat{}, false
}
func (v View) Stat(id actor.ActorID) (startedAt time.Time, live bool) { return }
type presencePort interface {
	Stat(actor.ActorID) (actorrt.UnitStat, bool)
}`
	files, fset := parseArchWallFixtureSource(t, "../platform/home/owner_fixture.go", src)
	if got := actorrtStatFreeFunctionEscapes(files, fset); len(got) != 0 {
		t.Errorf("stat escape wall fired on legitimate owner methods: %s", strings.Join(got, "; "))
	}
}
