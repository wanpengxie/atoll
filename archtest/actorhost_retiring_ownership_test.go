package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// spec §13.3:
//   - "Retiring 的唯一 insert helper 必须同时登记 exact Done-watcher ownership；
//     运行期 Done handler 必须 exact delete entry"  (audit #19)
//   - "Retiring 只能在对应 ActorID HostState row 内并由同一 hostSpan 保护；不得定义
//     Host-global unsynchronized Retiring map；actorrt 不拥有 zombie/leak registry"
//     (audit #10)
//
// Why these are AST walls and not text scans: the break form the audit named for
// #19 is to move `h.watcherWG.Add(1)` out of `retireLocked` and into
// `executeRetireTasks` ("we start the goroutine here anyway, account for it
// here too"). After that move host.go still contains BOTH strings, so the
// existing file-level `strings.Contains` wall stays green while the atomicity
// it claims to protect — one entry inserted into the row and one watcher debt
// booked in the same critical section — is gone. Only function-body ownership
// can see the difference.
//
// The break form for #10 is likewise invisible to a name scan: (a) a "fast
// path" that reads state.retiring without taking the per-ActorID span, and
// (b) a cross-Unit leak registry re-introduced under a different name
// (`orphanTracker` instead of `ZombieInfo`). So the wall is shaped on the
// STRUCTURE — which functions may touch the row, and whether any aggregate
// keyed by execution objects exists at all — not on the vocabulary.

// retiringFuncSet maps function name -> the files it was seen in.
type retiringFuncSet map[string][]string

func (s retiringFuncSet) add(name, path string) { s[name] = append(s[name], path) }

func (s retiringFuncSet) names() []string {
	out := make([]string, 0, len(s))
	for name := range s {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// retiringOwnershipViolations is the #19 wall: exactly one function inserts into
// the Retiring row, that same function books the Done-watcher debt, and nobody
// else books it.
func retiringOwnershipViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	inserts := retiringFuncSet{}
	watcherAdds := retiringFuncSet{}
	deletes := retiringFuncSet{}
	watcherDones := retiringFuncSet{}

	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.AssignStmt:
					for _, target := range value.Lhs {
						index, ok := target.(*ast.IndexExpr)
						if !ok {
							continue
						}
						selector, ok := index.X.(*ast.SelectorExpr)
						if ok && selector.Sel.Name == "retiring" {
							inserts.add(fn.Name.Name, path)
						}
					}
				case *ast.CallExpr:
					if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == "delete" &&
						len(value.Args) > 0 {
						if selector, ok := value.Args[0].(*ast.SelectorExpr); ok &&
							selector.Sel.Name == "retiring" {
							deletes.add(fn.Name.Name, path)
						}
					}
					selector, ok := value.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					inner, ok := selector.X.(*ast.SelectorExpr)
					if !ok || inner.Sel.Name != "watcherWG" {
						return true
					}
					switch selector.Sel.Name {
					case "Add":
						watcherAdds.add(fn.Name.Name, path)
					case "Done":
						watcherDones.add(fn.Name.Name, path)
					}
				}
				return true
			})
		}
	}

	var v []string
	insertNames := inserts.names()
	if len(insertNames) != 1 {
		v = append(v, fmt.Sprintf(
			"Retiring insert helpers = %v, want exactly one (the sole insert point is what makes watcher ownership provable)",
			insertNames))
	}
	addNames := watcherAdds.names()
	for _, name := range insertNames {
		if len(watcherAdds[name]) == 0 {
			v = append(v, fmt.Sprintf(
				"%s inserts a Retiring entry without booking the Done-watcher debt in the same body — insert and watcher ownership are one step",
				name))
		}
	}
	for _, name := range addNames {
		if len(inserts[name]) == 0 {
			v = append(v, fmt.Sprintf(
				"%s books a Done-watcher debt without inserting the Retiring entry — the debt moved out of the insert helper",
				name))
		}
	}
	deleteNames := deletes.names()
	if len(deleteNames) != 1 {
		v = append(v, fmt.Sprintf(
			"Retiring exact-delete sites = %v, want exactly one Done handler", deleteNames))
	}
	for _, name := range deleteNames {
		if len(watcherDones[name]) == 0 {
			v = append(v, fmt.Sprintf(
				"%s deletes the Retiring entry but is not the watcher that releases the debt", name))
		}
	}
	return v
}

// retiringSpanConfinement is the #10(a)/(b) wall: the Retiring set lives only in
// the per-ActorID row, and only the closed set of functions that hold (or are
// called under) that ActorID's span may touch it.
var retiringSpanAllowedFuncs = map[string]bool{
	// state predicate — pure, called under the row lock by its callers.
	"empty": true,
	// the sole insert helper: name ends in Locked, caller holds the span.
	"retireLocked": true,
	// the Done watcher: takes the span itself before touching the row.
	"watchRetiring": true,
	// shutdown accounting: takes the span per ActorID in its own body.
	"close": true,
}

func retiringSpanViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			touches, spanLocks := 0, 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.SelectorExpr:
					if value.Sel.Name == "retiring" {
						touches++
					}
				case *ast.CallExpr:
					selector, ok := value.Fun.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "lock" {
						return true
					}
					if inner, ok := selector.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "spans" {
						spanLocks++
					}
				}
				return true
			})
			if touches == 0 {
				continue
			}
			if !retiringSpanAllowedFuncs[fn.Name.Name] {
				v = append(v, fmt.Sprintf(
					"%s:%s touches the Retiring row outside the closed set %v — a lock-free fast path over Retiring is exactly the regression this pins",
					path, fn.Name.Name, sortedAllowedRetiringFuncs()))
				continue
			}
			// Anything that is not a *Locked helper or a pure predicate must
			// take the ActorID span in its own body.
			if fn.Name.Name == "empty" || strings.HasSuffix(fn.Name.Name, "Locked") {
				continue
			}
			if spanLocks == 0 {
				v = append(v, fmt.Sprintf(
					"%s:%s touches the Retiring row without taking the ActorID span", path, fn.Name.Name))
			}
		}
	}
	return v
}

func sortedAllowedRetiringFuncs() []string {
	out := make([]string, 0, len(retiringSpanAllowedFuncs))
	for name := range retiringSpanAllowedFuncs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// hostGlobalAggregateViolations is the rest of #10(a): HostSupervisor owns ONE
// aggregate — the per-ActorID row index. Any second Host-global collection is a
// cross-Unit registry no matter what it is called (`retiring`, `orphanTracker`,
// `leaked`, …).
const hostRowIndexField = "states"

func hostGlobalAggregateViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "HostSupervisor" {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return false
			}
			for _, field := range structure.Fields.List {
				switch field.Type.(type) {
				case *ast.MapType, *ast.ArrayType:
				default:
					continue
				}
				names := []string{""}
				if len(field.Names) > 0 {
					names = nil
					for _, name := range field.Names {
						names = append(names, name.Name)
					}
				}
				for _, name := range names {
					if name == hostRowIndexField {
						continue
					}
					v = append(v, fmt.Sprintf(
						"%s:%d HostSupervisor owns a second Host-global collection %q — per-ActorID rows are the only aggregate",
						path, fset.Position(field.Pos()).Line, name))
				}
			}
			return false
		})
	}
	return v
}

// actorrtExecutionRegistryViolations is #10(c) done structurally: actorrt may
// not own ANY collection keyed or valued by an execution object, and may not
// hold one in a package-level variable. Renaming ZombieInfo to orphanTracker
// changes nothing here.
func actorrtExecutionRegistryViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	executionish := func(text string) bool {
		for _, word := range []string{"Unit", "Incarnation", "Actor"} {
			if strings.Contains(text, word) {
				return true
			}
		}
		return false
	}
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeSpec:
				structure, ok := value.Type.(*ast.StructType)
				if !ok {
					return true
				}
				for _, field := range structure.Fields.List {
					text := expressionText(fset, field.Type)
					switch field.Type.(type) {
					case *ast.MapType, *ast.ArrayType:
					default:
						continue
					}
					if executionish(text) {
						v = append(v, fmt.Sprintf(
							"%s:%d type %s owns a collection of execution objects (%s) — actorrt is an exact Unit leaf with no zombie/leak registry",
							path, fset.Position(field.Pos()).Line, value.Name.Name, text))
					}
				}
			case *ast.GenDecl:
				if value.Tok != token.VAR {
					return true
				}
				for _, item := range value.Specs {
					spec, ok := item.(*ast.ValueSpec)
					if !ok || spec.Type == nil {
						continue
					}
					switch spec.Type.(type) {
					case *ast.MapType, *ast.ArrayType:
					default:
						continue
					}
					if executionish(expressionText(fset, spec.Type)) {
						v = append(v, fmt.Sprintf(
							"%s:%d package-level collection of execution objects", path,
							fset.Position(spec.Pos()).Line))
					}
				}
			}
			return true
		})
	}
	return v
}

func TestActorHostRetiringInsertOwnsItsDoneWatcher(t *testing.T) {
	files, fset := loadArchWallPackage(t, "../runtime/actorhost")
	failViolations(t, "Retiring insert ⇔ Done-watcher ownership (spec §13.3)",
		retiringOwnershipViolations(files, fset))
	failViolations(t, "Retiring stays inside the ActorID row under its own span",
		retiringSpanViolations(files, fset))
	failViolations(t, "no Host-global Retiring/leak aggregate",
		hostGlobalAggregateViolations(files, fset))

	rtFiles, rtFset := loadArchWallPackage(t, "../runtime/actorrt")
	failViolations(t, "actorrt owns no zombie/leak registry",
		actorrtExecutionRegistryViolations(rtFiles, rtFset))
}

func TestActorHostRetiringWallTripsOnOwnershipDrift(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			// The audit's #19 break: same file, same two strings, different
			// function bodies.
			name: "watcher debt moved out of the insert helper",
			src: `package actorhost
func (h *HostSupervisor) retireLocked(id actor.ActorID, state *hostState, unit *actorrt.Unit) *retireTask {
	if state.retiring == nil {
		state.retiring = make(map[*actorrt.Unit]*retireEntry)
	}
	entry := &retireEntry{unit: unit}
	state.retiring[unit] = entry
	return &retireTask{id: id, entry: entry}
}
func (h *HostSupervisor) executeRetireTasks(tasks []retireTask) {
	for i := range tasks {
		task := tasks[i]
		h.watcherWG.Add(1)
		go h.watchRetiring(task)
		task.entry.unit.Stop()
	}
}
func (h *HostSupervisor) watchRetiring(task retireTask) {
	defer h.watcherWG.Done()
	unlock := h.spans.lock(task.id)
	state := h.states[task.id]
	delete(state.retiring, task.entry.unit)
	unlock()
}`,
			check: retiringOwnershipViolations,
		},
		{
			name: "second Retiring insert point",
			src: `package actorhost
func (h *HostSupervisor) retireLocked(id actor.ActorID, state *hostState, unit *actorrt.Unit) *retireTask {
	entry := &retireEntry{unit: unit}
	state.retiring[unit] = entry
	h.watcherWG.Add(1)
	return &retireTask{id: id, entry: entry}
}
func (h *HostSupervisor) adoptRetiringLocked(state *hostState, unit *actorrt.Unit) {
	state.retiring[unit] = &retireEntry{unit: unit}
}
func (h *HostSupervisor) watchRetiring(task retireTask) {
	defer h.watcherWG.Done()
	unlock := h.spans.lock(task.id)
	state := h.states[task.id]
	delete(state.retiring, task.entry.unit)
	unlock()
}`,
			check: retiringOwnershipViolations,
		},
		{
			// The audit's #10(a) break: a "performance" fast path that reads the
			// Retiring row without the per-ActorID span.
			name: "lock-free fast path over the Retiring row",
			src: `package actorhost
func (h *HostSupervisor) retiringFast(id actor.ActorID) bool {
	state := h.states[id]
	return state != nil && len(state.retiring) > 0
}`,
			check: retiringSpanViolations,
		},
		{
			name: "allowlisted function loses its span",
			src: `package actorhost
func (h *HostSupervisor) watchRetiring(task retireTask) {
	state := h.states[task.id]
	delete(state.retiring, task.entry.unit)
}`,
			check: retiringSpanViolations,
		},
		{
			// The audit's #10 break: Retiring escapes the row into Host-global
			// state.
			name: "Host-global Retiring map",
			src: `package actorhost
type HostSupervisor struct {
	states   map[actor.ActorID]*hostState
	retiring map[*actorrt.Unit]*retireEntry
}`,
			check: hostGlobalAggregateViolations,
		},
		{
			// The audit's #10(b) break: the zombie registry returns under a new
			// name, so a forbidden-word wall never fires.
			name: "renamed cross-Unit leak registry in actorrt",
			src: `package actorrt
type orphanTracker struct {
	mu    sync.Mutex
	units map[*Unit]Incarnation
}`,
			check: actorrtExecutionRegistryViolations,
		},
		{
			name: "package-level Unit registry in actorrt",
			src: `package actorrt
var leakedUnits []*Unit`,
			check: actorrtExecutionRegistryViolations,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "actorhost_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestActorHostRetiringWallTripsOnPatchedProductionSource performs the audit's
// #19 refactor on the REAL host.go — move the watcher debt from the insert
// helper to the goroutine launcher — and requires the wall to catch it. The
// file-level string wall this replaces stays green on exactly this patch,
// because both strings are still in the file.
func TestActorHostRetiringWallTripsOnPatchedProductionSource(t *testing.T) {
	const path = "../runtime/actorhost/host.go"
	files, fset := patchArchWallSource(t, path,
		archWallPatch{
			old: "\tstate.retiring[unit] = entry\n\th.watcherWG.Add(1)\n",
			new: "\tstate.retiring[unit] = entry\n",
		},
		archWallPatch{
			old: "\t\tgo h.watchRetiring(task)\n",
			new: "\t\th.watcherWG.Add(1)\n\t\tgo h.watchRetiring(task)\n",
		},
	)
	if got := retiringOwnershipViolations(files, fset); len(got) == 0 {
		t.Fatal("insert⇔watcher-ownership wall stayed green after the debt moved out of retireLocked")
	}

	// #10's fast path, added to the real file.
	files, fset = patchArchWallSource(t, path, archWallPatch{
		old: "func (h *HostSupervisor) retireLocked(",
		new: `func (h *HostSupervisor) retiringFastPath(id actor.ActorID) bool {
	state := h.states[id]
	return state != nil && len(state.retiring) > 0
}

func (h *HostSupervisor) retireLocked(`,
	})
	if got := retiringSpanViolations(files, fset); len(got) == 0 {
		t.Fatal("span-confinement wall stayed green on a lock-free Retiring fast path")
	}

	// #10's Host-global registry, added to the real HostSupervisor.
	files, fset = patchArchWallSource(t, path, archWallPatch{
		old: "\tstates   map[actor.ActorID]*hostState\n",
		new: "\tstates   map[actor.ActorID]*hostState\n\torphans  map[*actorrt.Unit]*retireEntry\n",
	})
	if got := hostGlobalAggregateViolations(files, fset); len(got) == 0 {
		t.Fatal("Host-global aggregate wall stayed green on a renamed cross-Unit registry")
	}
}
