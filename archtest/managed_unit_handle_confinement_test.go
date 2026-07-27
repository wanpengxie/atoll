package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// spec §13.3 last line: "package 依赖符合 §3.2" — and inside §3.2, the clause
// that no wall has ever looked at: Home / channelkit / schedule / presence /
// link do not acquire a MUTATION HANDLE on a managed Unit.
//
// This is NOT the same statement as the import graph. All of these packages may
// legitimately name actorrt types: presence folds `actorrt.ObsKind` /
// `actorrt.Incarnation`, link forwards observations, and Home literally calls
// `actorrt.Prepare` to build the SystemActor body during assembly. An import
// wall would therefore be either red or useless. The real invariant is about
// POSSESSION and DRIVING:
//
//	a Unit may TRANSIT one of these packages (assembly hands the freshly
//	prepared system body straight to SystemKernel.Start), and one of them may
//	ASK a body a question through the diagnostic view Host publishes — but none
//	of them may KEEP a body, and none of them may DRIVE one.
//
// The clause the spec writes is "mutation handle", and that word is load
// bearing. `runtime/actorhost` deliberately exports `Inspect(id) (Snapshot,
// bool)` with `Snapshot.Unit *actorrt.Unit`; `platform/home`'s actorSystem
// answers Stat / Incarnation off exactly that view. Reading `Stat()` / `Self()`
// / `IsAlive()` through it is the sanctioned diagnostic face. Calling `Stop()`
// on the same pointer is the break: it retires a body behind the supervisor
// that owns its life, and it is ONE token away from the legal form.
//
// So the wall splits on the VERB, not on the dot:
//
//   - holding a Unit (a field, or storing one into a field) is banned outright —
//     the diagnostic view is a momentary answer, never a cached handle;
//   - calling a state-CHANGING method on a Unit is banned however the handle
//     was obtained: a bound identifier, or a bare `snapshot.Unit.Stop()` chain;
//   - calling a read-only method is allowed, because that is precisely the
//     surface Host chose to publish.
//
// Note on the spec's list: `channelkit` no longer exists (runtime/systemkernel
// is its legal successor and is a Unit OWNER, deliberately outside this list).
// The four packages below are its live members.

// managedUnitBystanders are the §3.2 packages that must never hold or drive a
// Unit. Ownership lives with runtime/actorhost (managed bodies) and
// runtime/systemkernel (the one system body); neither appears here.
var managedUnitBystanders = []string{
	"../platform/home",
	"../platform/internal/link",
	"../platform/internal/presence",
	"../runtime/schedule",
}

// unitMutationVerbs and unitReadOnlyVerbs together are the COMPLETE exported
// method set of *actorrt.Unit, split by whether the call changes the body's
// state. A bystander calling anything in the first set is driving a body it does
// not own; anything in the second set is a question, which is what
// actorhost.Snapshot exists to answer.
//
// TestUnitMethodSetStaysClassified below re-derives the real method set from
// runtime/actorrt and fails if a method appears in neither map, so a newly added
// verb cannot slip through unclassified.
var unitMutationVerbs = map[string]bool{
	"Start":            true, // spins the body up
	"Stop":             true, // retires the body behind its supervisor
	"Deliver":          true, // puts work in the mailbox
	"CancelRequest":    true, // reaches into in-flight work
	"InstallEventSink": true, // rewires where the body reports to
}

var unitReadOnlyVerbs = map[string]bool{
	"Done":    true,
	"IsAlive": true,
	"State":   true,
	"Self":    true,
	"Stat":    true,
}

// isUnitFieldRead reports whether an expression is a `<something>.Unit` field
// read — the way a bystander actually comes to touch a body today
// (`snapshot, _ := host.Inspect(id)` then `snapshot.Unit`).
//
// This is a PARSER-LEVEL APPROXIMATION on purpose: archtest has no type
// information, so it keys on the field name `Unit`. The repo has exactly one
// exported field with that name (actorhost.Snapshot.Unit) and it is the one the
// spec clause is about; a same-named field on an unrelated type would be a
// false positive, which is the correct failure direction for a tripwire.
func isUnitFieldRead(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Unit"
}

// unitBindingsInFunc returns the identifiers inside one function body that are
// bound to a *actorrt.Unit: parameters, `var` declarations, the result of
// actorrt.Prepare, and — the path production actually uses — a read of a `.Unit`
// field off a diagnostic snapshot.
func unitBindingsInFunc(fn *ast.FuncDecl) map[string]bool {
	bound := map[string]bool{}
	if fn.Type.Params != nil {
		for _, param := range fn.Type.Params.List {
			if !isUnitPointerType(param.Type) {
				continue
			}
			for _, name := range param.Names {
				bound[name.Name] = true
			}
		}
	}
	if fn.Body == nil {
		return bound
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.ValueSpec:
			if isUnitPointerType(value.Type) {
				for _, name := range value.Names {
					bound[name.Name] = true
				}
			}
		case *ast.AssignStmt:
			for i, rhs := range value.Rhs {
				// `unit := snapshot.Unit` — the field read off the Snapshot the
				// Host publishes. This is how a bystander gets a body pointer in
				// practice; `actorrt.Prepare` is only the assembly path.
				if isUnitFieldRead(rhs) && i < len(value.Lhs) {
					if ident, ok := value.Lhs[i].(*ast.Ident); ok {
						bound[ident.Name] = true
					}
					continue
				}
				// `unit, err := actorrt.Prepare(...)` — how assembly comes to
				// hold the freshly built system body at all.
				call, ok := rhs.(*ast.CallExpr)
				if !ok || len(value.Rhs) != 1 {
					continue
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Prepare" {
					continue
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || pkg.Name != "actorrt" {
					continue
				}
				if ident, ok := value.Lhs[0].(*ast.Ident); ok {
					bound[ident.Name] = true
				}
			}
		}
		return true
	})
	return bound
}

// managedUnitHandleViolations reports the three ways one of these packages can
// acquire a mutation handle: keep a Unit in a struct, store one into a field, or
// call a state-changing method on one (through a binding or straight off the
// `.Unit` field read).
func managedUnitHandleViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		// (1) Long-term possession: a Unit-typed field anywhere in the package.
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				return true
			}
			for _, field := range structType.Fields.List {
				if !typeMentionsUnitPointer(field.Type) {
					continue
				}
				v = append(v, fmt.Sprintf(
					"%s:%d %s keeps a Unit handle — this package may pass a body through or ask it a question, never hold one",
					path, fset.Position(field.Pos()).Line, spec.Name.Name))
			}
			return true
		})

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			bound := unitBindingsInFunc(fn)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.AssignStmt:
					// (2) Escape into state: `x.field = unit`, whether `unit` is
					// a binding or a fresh `snapshot.Unit` read. A diagnostic
					// answer has a lifetime of one statement.
					for i, rhs := range value.Rhs {
						if i >= len(value.Lhs) {
							continue
						}
						ident, isIdent := rhs.(*ast.Ident)
						fromBinding := isIdent && bound[ident.Name]
						if !fromBinding && !isUnitFieldRead(rhs) {
							continue
						}
						if _, isField := value.Lhs[i].(*ast.SelectorExpr); !isField {
							continue
						}
						v = append(v, fmt.Sprintf(
							"%s:%d %s stores a Unit into a field — transfer is one-way and inspection is momentary, the body is not retained here",
							path, fset.Position(value.Pos()).Line, fn.Name.Name))
					}
				case *ast.CallExpr:
					// (3) Driving the body: a state-changing verb, however the
					// handle was obtained.
					selector, ok := value.Fun.(*ast.SelectorExpr)
					if !ok || !unitMutationVerbs[selector.Sel.Name] {
						return true
					}
					switch receiver := selector.X.(type) {
					case *ast.Ident:
						if !bound[receiver.Name] {
							return true
						}
						v = append(v, fmt.Sprintf(
							"%s:%d %s calls %s.%s — a MUTATION handle on a managed body belongs to actorhost/systemkernel; only the read-only face (%s) may be used here",
							path, fset.Position(value.Pos()).Line, fn.Name.Name,
							receiver.Name, selector.Sel.Name, sortedVerbs(unitReadOnlyVerbs)))
					case *ast.SelectorExpr:
						if receiver.Sel.Name != "Unit" {
							return true
						}
						v = append(v, fmt.Sprintf(
							"%s:%d %s calls .Unit.%s off a diagnostic snapshot — Inspect publishes a body to be QUESTIONED (%s), never to be driven",
							path, fset.Position(value.Pos()).Line, fn.Name.Name,
							selector.Sel.Name, sortedVerbs(unitReadOnlyVerbs)))
					}
				}
				return true
			})
		}
	}
	return v
}

func sortedVerbs(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return strings.Join(out, "/")
}

// TestUnitMethodSetStaysClassified keeps the verb split from rotting. The two
// maps above are only meaningful if they cover the REAL method set, so the set
// is re-derived from runtime/actorrt and every exported method must be
// classified. Adding a method to Unit therefore forces whoever adds it to say
// whether it changes the body — which is the whole question §3.2 turns on.
func TestUnitMethodSetStaysClassified(t *testing.T) {
	files, _ := loadArchWallPackage(t, "../runtime/actorrt")
	declared := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || recvBaseTypeName(fn) != "Unit" || !fn.Name.IsExported() {
				continue
			}
			declared[fn.Name.Name] = true
		}
	}
	if len(declared) == 0 {
		t.Fatal("no exported *actorrt.Unit methods found — the §3.2 verb split lost its subject")
	}
	for name := range declared {
		if unitMutationVerbs[name] == unitReadOnlyVerbs[name] {
			t.Fatalf("Unit.%s is classified in neither (or both) of unitMutationVerbs/unitReadOnlyVerbs — "+
				"a new Unit verb must be declared state-changing or read-only before §3.2 can be enforced against it", name)
		}
	}
	for _, set := range []map[string]bool{unitMutationVerbs, unitReadOnlyVerbs} {
		for name := range set {
			if !declared[name] {
				t.Fatalf("Unit.%s is classified here but no longer exists on *actorrt.Unit — the verb split is stale", name)
			}
		}
	}
}

func TestUnitMutationHandleStaysOutOfHomeLinkPresenceSchedule(t *testing.T) {
	for _, root := range managedUnitBystanders {
		files, fset := loadArchWallPackage(t, root)
		failViolations(t, "§3.2: "+root+" never acquires a managed Unit mutation handle",
			managedUnitHandleViolations(files, fset))
	}

	// Footing: the wall means nothing unless Home really does come to hold a
	// body pointer. If assembly stopped transiting a Unit AND stopped reading
	// the diagnostic view, this check must be retuned rather than left silently
	// guarding an empty set.
	files, _ := loadArchWallPackage(t, "../platform/home")
	handles := 0
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			handles += len(unitBindingsInFunc(fn))
		}
	}
	if handles == 0 {
		t.Fatal("platform/home no longer touches any *actorrt.Unit — retune the §3.2 handle wall")
	}
}

// TestUnitMutationHandleWallTripsOnAssemblyCache is the trip proof. The cases
// that can be expressed against the real source are applied to it directly.
func TestUnitMutationHandleWallTripsOnAssemblyCache(t *testing.T) {
	const actorSystem = "../platform/home/actor_system.go"

	t.Run("assembly stores the transiting body for restart", func(t *testing.T) {
		const anchor = "	if err := a.home.systemKernel.Start(systemUnit); err != nil {"
		const patched = `	// "restart acceleration": keep the body we just built.
	a.lastSystemUnit = systemUnit
	if err := a.home.systemKernel.Start(systemUnit); err != nil {`
		files, fset := patchArchWallSource(t, actorSystem, archWallPatch{old: anchor, new: patched})
		if got := managedUnitHandleViolations(files, fset); len(got) == 0 {
			t.Fatal("§3.2 handle wall stayed green on assembly caching the transiting Unit")
		}
	})

	// The audit's break: the diagnostic snapshot is already in hand at these
	// call sites, so escalating from a question to a command is a one-line edit
	// inside an existing `if`. Both spellings are proved against the real file.
	t.Run("diagnostic snapshot escalated to a Stop, chained", func(t *testing.T) {
		const anchor = "	if snapshot.Actual == actorhost.ActualBody && snapshot.Unit != nil && snapshot.Unit.IsAlive() {"
		const patched = `	if snapshot.Actual == actorhost.ActualBody && snapshot.Unit != nil && snapshot.Unit.IsAlive() {
		// "reap the body while we are here"
		snapshot.Unit.Stop()`
		files, fset := patchArchWallSource(t, actorSystem, archWallPatch{old: anchor, new: patched})
		if got := managedUnitHandleViolations(files, fset); len(got) == 0 {
			t.Fatal("§3.2 handle wall stayed green on snapshot.Unit.Stop() off actorhost.Inspect")
		}
	})

	t.Run("diagnostic snapshot escalated to a Stop, via a binding", func(t *testing.T) {
		const anchor = "	return snapshot.Unit.Self(), true"
		const patched = `	body := snapshot.Unit
	body.Deliver(nil)
	return snapshot.Unit.Self(), true`
		files, fset := patchArchWallSource(t, actorSystem, archWallPatch{old: anchor, new: patched})
		if got := managedUnitHandleViolations(files, fset); len(got) == 0 {
			t.Fatal("§3.2 handle wall stayed green on a Unit bound out of the snapshot and then driven")
		}
	})

	t.Run("diagnostic snapshot cached in a field", func(t *testing.T) {
		const anchor = "	return snapshot.Unit.Self(), true"
		const patched = `	a.lastBody = snapshot.Unit
	return snapshot.Unit.Self(), true`
		files, fset := patchArchWallSource(t, actorSystem, archWallPatch{old: anchor, new: patched})
		if got := managedUnitHandleViolations(files, fset); len(got) == 0 {
			t.Fatal("§3.2 handle wall stayed green on caching the snapshot's Unit in a field")
		}
	})

	fixtures := []struct {
		name string
		src  string
	}{
		{
			name: "Home keeps a Unit field",
			src: `package home
type Home struct {
	systemKernel *systemkernel.Kernel
	lastUnit     *actorrt.Unit
}`,
		},
		{
			name: "link drives a body directly",
			src: `package link
func (s *AuthenticatedLinkSession) deliverLocal(unit *actorrt.Unit, env *message.Envelope) error {
	return unit.Deliver(env)
}`,
		},
		{
			name: "presence rewires where a body reports",
			src: `package presence
func (f *Fold) attach(unit *actorrt.Unit, sink actorrt.UnitEventSink) error {
	return unit.InstallEventSink(sink)
}`,
		},
		{
			name: "schedule stops a body on timer expiry",
			src: `package schedule
func (e *Engine) expire() {
	var unit *actorrt.Unit
	unit.Stop()
}`,
		},
		{
			name: "schedule stops a body straight off an Inspect snapshot",
			src: `package schedule
func (e *Engine) expire(host *actorhost.Host, id actor.ActorID) {
	snapshot, ok := host.Inspect(id)
	if !ok {
		return
	}
	snapshot.Unit.Stop()
}`,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "bystander_fixture.go", tc.src)
			if got := managedUnitHandleViolations(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}

	// The negative half: the two SANCTIONED shapes must stay green, or the wall
	// is just an import ban wearing a costume.
	legal := []struct {
		name string
		src  string
	}{
		{
			name: "one-way transit",
			src: `package home
func (a *actorSystem) start(ctx context.Context, systemUnit *actorrt.Unit) error {
	return a.home.systemKernel.Start(systemUnit)
}`,
		},
		{
			name: "read-only diagnostic query through the Host snapshot",
			src: `package home
func (a *actorSystem) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	snapshot, ok := a.home.serverHost.Inspect(id)
	if !ok || snapshot.Unit == nil || !snapshot.Unit.IsAlive() {
		return actorrt.UnitStat{}, false
	}
	return snapshot.Unit.Stat(), true
}`,
		},
		{
			name: "read-only query through a bound snapshot field",
			src: `package presence
func (f *Fold) sample(snapshot actorhost.Snapshot) (actorrt.Incarnation, bool) {
	body := snapshot.Unit
	if body == nil || !body.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return body.Self(), true
}`,
		},
	}
	for _, tc := range legal {
		t.Run("allowed: "+tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "legal_fixture.go", tc.src)
			if got := managedUnitHandleViolations(files, fset); len(got) != 0 {
				t.Fatalf("§3.2 handle wall fired on the sanctioned shape %q: %v", tc.name, got)
			}
		})
	}
}
