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
//   - "RouteActual 必须持 non-nil exact Binding，且不得持 Unit/Incarnation"  (audit #7)
//   - "CarrierDesired 无 Binding 时 Host Actual 必须为 nil；不得定义 CarrierActual /
//     detached carrier row"                                                (audit #8)
//   - "HostState 必须 sparse；Desired/BuildClaim/Actual/Retiring/worker metadata
//     全空的 row 不得保留"                                                  (audit #9)
//
// The existing wall checks the DECLARED SHAPE of routeActual and the literal
// text of the `empty()` call site. Both break forms the audit named slip
// underneath that:
//
//	#7: add a third construction point — `state.route = &routeActual{key: key}`
//	    ("reconnecting, the Binding is filled in a moment later"). The struct
//	    shape is untouched; only the constructed VALUE is now a route actual
//	    with a zero Binding, i.e. a route that claims a peer it cannot reach.
//	#8: on the "CarrierDesired lost its Binding" branch, set Actual to an empty
//	    carrier value instead of literal nil. types.go's own comment warns about
//	    this, which means it has happened before.
//	#9: delete `s.retryAt.IsZero()` from `empty()`. The call site string the
//	    current wall pins is unchanged; a row carrying a pending retry now
//	    reports empty and is reclaimed early, so the retry is lost.
//
// So all three are checked on constructed values and on field coverage, not on
// declarations or call-site text.

// routeActualConstructionViolations is #7 plus the "absence is nil, never a
// zero-resource value" half of #8.
func routeActualConstructionViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// Every Binding identifier this function proves valid.
			validated := map[string]bool{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Valid" {
					return true
				}
				if root := archWallRootIdent(selector.X); root != "" {
					validated[root] = true
				}
				return true
			})

			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.CompositeLit:
					name, ok := value.Type.(*ast.Ident)
					if !ok || name.Name != "routeActual" {
						return true
					}
					v = append(v, routeActualLiteralViolations(
						path, fn.Name.Name, value, fset, validated)...)
				case *ast.AssignStmt:
					for _, target := range value.Lhs {
						selector, ok := target.(*ast.SelectorExpr)
						if !ok || selector.Sel.Name != "route" {
							continue
						}
						for _, rhs := range value.Rhs {
							if !routeAssignmentIsNilOrExactRoute(rhs) {
								v = append(v, fmt.Sprintf(
									"%s:%s:%d assigns the route actual from %q — route absence is literal nil and route presence is an inline &routeActual{…}; nothing else may reach the field",
									path, fn.Name.Name, fset.Position(rhs.Pos()).Line,
									expressionText(fset, rhs)))
							}
						}
					}
				}
				return true
			})
		}
	}
	return v
}

func routeAssignmentIsNilOrExactRoute(rhs ast.Expr) bool {
	if ident, ok := rhs.(*ast.Ident); ok {
		return ident.Name == "nil"
	}
	unary, ok := rhs.(*ast.UnaryExpr)
	if !ok || unary.Op != token.AND {
		return false
	}
	lit, ok := unary.X.(*ast.CompositeLit)
	if !ok {
		return false
	}
	name, ok := lit.Type.(*ast.Ident)
	return ok && name.Name == "routeActual"
}

func routeActualLiteralViolations(
	path, fn string,
	lit *ast.CompositeLit,
	fset *token.FileSet,
	validated map[string]bool,
) []string {
	var v []string
	where := fmt.Sprintf("%s:%s:%d", path, fn, fset.Position(lit.Pos()).Line)
	fields := map[string]ast.Expr{}
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			v = append(v, fmt.Sprintf("%s builds a route actual positionally", where))
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		fields[key.Name] = kv.Value
	}
	for _, required := range []string{"key", "binding"} {
		if _, ok := fields[required]; !ok {
			v = append(v, fmt.Sprintf(
				"%s builds a route actual without %q — a route actual with a zero Binding is a route that names a peer it cannot reach",
				where, required))
		}
	}
	binding, ok := fields["binding"]
	if !ok {
		return v
	}
	switch value := binding.(type) {
	case *ast.Ident:
		if value.Name == "nil" {
			v = append(v, fmt.Sprintf("%s builds a route actual with a nil Binding", where))
			return v
		}
	case *ast.SelectorExpr:
	default:
		v = append(v, fmt.Sprintf(
			"%s builds the Binding inline (%s) instead of installing an already-authorized exact Binding",
			where, expressionText(fset, binding)))
		return v
	}
	root := archWallRootIdent(binding)
	if root != "" && !validated[root] {
		v = append(v, fmt.Sprintf(
			"%s installs Binding %q that this function never proves valid — non-nil-ness of the exact Binding is the route actual's whole contract",
			where, root))
	}
	return v
}

// carrierActualTypeViolations is the rest of #8: there is no representation for
// "route-less carrier actual" to construct in the first place.
func carrierActualTypeViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			lower := strings.ToLower(spec.Name.Name)
			carrierActual := strings.Contains(lower, "carrier") && strings.Contains(lower, "actual")
			detachedRoute := strings.Contains(lower, "detached") &&
				(strings.Contains(lower, "route") || strings.Contains(lower, "carrier"))
			if carrierActual || detachedRoute {
				v = append(v, fmt.Sprintf(
					"%s:%d declares %s — a route-less carrier actual is spelled nil, never as a zero-resource value",
					path, fset.Position(spec.Pos()).Line, spec.Name.Name))
			}
			if spec.Name.Name != "hostState" {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return false
			}
			for _, field := range structure.Fields.List {
				for _, name := range field.Names {
					if name.Name != "route" {
						continue
					}
					if _, pointer := field.Type.(*ast.StarExpr); !pointer {
						v = append(v, fmt.Sprintf(
							"%s:%d hostState.route is %s, not a pointer — absence must be representable as nil",
							path, fset.Position(field.Pos()).Line, expressionText(fset, field.Type)))
					}
				}
			}
			return false
		})
	}
	return v
}

// hostStateSparseViolations is #9: `empty()` must consult EVERY field of the
// row. Derived from the struct declaration, so both deleting a check and adding
// an unchecked field trip it.
func hostStateSparseViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var fields []string
	var emptyBody *ast.BlockStmt
	var emptyPath string
	deleters := map[string]bool{}
	emptyCallers := map[string]bool{}

	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "hostState" {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return false
			}
			for _, field := range structure.Fields.List {
				for _, name := range field.Names {
					fields = append(fields, name.Name)
				}
			}
			return false
		})
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "empty" && fn.Recv != nil {
				emptyBody, emptyPath = fn.Body, path
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "delete" &&
					len(call.Args) > 0 {
					if selector, ok := call.Args[0].(*ast.SelectorExpr); ok &&
						selector.Sel.Name == "states" {
						deleters[fn.Name.Name] = true
					}
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "empty" {
					emptyCallers[fn.Name.Name] = true
				}
				return true
			})
		}
	}

	var v []string
	if len(fields) == 0 {
		return []string{"hostState declaration not found — the sparse-row wall has nothing to hold"}
	}
	if emptyBody == nil {
		return []string{"hostState.empty() not found — row reclamation has no single predicate"}
	}
	consulted := map[string]bool{}
	ast.Inspect(emptyBody, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			consulted[selector.Sel.Name] = true
		}
		return true
	})
	var missing []string
	for _, field := range fields {
		if !consulted[field] {
			missing = append(missing, field)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		v = append(v, fmt.Sprintf(
			"%s hostState.empty() ignores row field(s) %v — a row that still carries them is not empty, and reclaiming it drops live state",
			emptyPath, missing))
	}
	for name := range deleters {
		if !emptyCallers[name] {
			v = append(v, fmt.Sprintf(
				"%s deletes a HostState row without consulting empty()", name))
		}
	}
	return v
}

func TestActorHostRouteActualAndSparseRow(t *testing.T) {
	files, fset := loadArchWallPackage(t, "../runtime/actorhost")
	failViolations(t, "route actual holds a non-nil exact Binding (spec §13.3)",
		routeActualConstructionViolations(files, fset))
	failViolations(t, "no route-less carrier actual representation",
		carrierActualTypeViolations(files, fset))
	failViolations(t, "HostState row is sparse on every field",
		hostStateSparseViolations(files, fset))
}

func TestActorHostRouteActualWallTripsOnZeroBindingAndFatRow(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			// #7's break: a third construction point that leaves Binding zero.
			name: "route actual constructed without a Binding",
			src: `package actorhost
func (h *HostSupervisor) reserveRoute(state *hostState, key AttemptKey) {
	state.route = &routeActual{key: key}
}`,
			check: routeActualConstructionViolations,
		},
		{
			// #8's break: "empty carrier value" instead of literal nil.
			name: "route actual constructed with a zero Binding value",
			src: `package actorhost
func (h *HostSupervisor) clearRoute(state *hostState, key AttemptKey) {
	state.route = &routeActual{key: key, binding: Binding{}, started: time.Now()}
}`,
			check: routeActualConstructionViolations,
		},
		{
			name: "route actual installs an unvalidated Binding",
			src: `package actorhost
func (h *HostSupervisor) adopt(state *hostState, key AttemptKey, binding Binding) {
	state.route = &routeActual{key: key, binding: binding, started: time.Now()}
}`,
			check: routeActualConstructionViolations,
		},
		{
			name: "route field assigned from something other than nil or an exact literal",
			src: `package actorhost
func (h *HostSupervisor) inherit(state *hostState, other *hostState) {
	state.route = other.route
}`,
			check: routeActualConstructionViolations,
		},
		{
			name: "route-less carrier actual type reintroduced",
			src: `package actorhost
type carrierActual struct {
	key AttemptKey
}`,
			check: carrierActualTypeViolations,
		},
		{
			name: "hostState.route stops being nil-representable",
			src: `package actorhost
type hostState struct {
	desired  *desiredValue
	route    routeActual
	retiring map[*actorrt.Unit]*retireEntry
}`,
			check: carrierActualTypeViolations,
		},
		{
			// #9's break: drop one field's check from empty().
			name: "empty() stops consulting the pending retry",
			src: `package actorhost
type hostState struct {
	desired  *desiredValue
	build    *buildJob
	body     *bodyActual
	route    *routeActual
	retiring map[*actorrt.Unit]*retireEntry
	retryAt  time.Time
}
func (s *hostState) empty() bool {
	return s != nil &&
		s.desired == nil &&
		s.build == nil &&
		s.body == nil &&
		s.route == nil &&
		len(s.retiring) == 0
}
func (h *HostSupervisor) deleteIfEmptyLocked(id actor.ActorID, state *hostState) {
	if state != nil && state.empty() {
		delete(h.states, id)
	}
}`,
			check: hostStateSparseViolations,
		},
		{
			name: "row reclaimed without consulting empty()",
			src: `package actorhost
type hostState struct {
	desired *desiredValue
}
func (s *hostState) empty() bool { return s != nil && s.desired == nil }
func (h *HostSupervisor) evict(id actor.ActorID) {
	delete(h.states, id)
}`,
			check: hostStateSparseViolations,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "route_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestActorHostRouteActualWallTripsOnPatchedProductionSource applies #7/#8/#9's
// break forms to the real host.go.
func TestActorHostRouteActualWallTripsOnPatchedProductionSource(t *testing.T) {
	const path = "../runtime/actorhost/host.go"

	// #7: a "the Binding arrives a moment later" placeholder construction.
	files, fset := patchArchWallSource(t, path, archWallPatch{
		old: "\tstate.route = &routeActual{key: key, binding: binding, started: started}\n",
		new: "\tstate.route = &routeActual{key: key, started: started}\n",
	})
	if got := routeActualConstructionViolations(files, fset); len(got) == 0 {
		t.Fatal("route-actual wall stayed green on a placeholder route with a zero Binding")
	}

	// #8: route absence written as a zero-resource value instead of nil.
	files, fset = patchArchWallSource(t, path, archWallPatch{
		old: "\t\t\tif state.route != nil {\n\t\t\t\tbindings = append(bindings, state.route.binding)\n\t\t\t\tstate.route = nil\n",
		new: "\t\t\tif state.route != nil {\n\t\t\t\tbindings = append(bindings, state.route.binding)\n\t\t\t\tstate.route = &routeActual{key: state.route.key, binding: Binding{}}\n",
	})
	if got := routeActualConstructionViolations(files, fset); len(got) == 0 {
		t.Fatal("route-actual wall stayed green on an empty-carrier actual replacing literal nil")
	}

	// #9: drop one field from the sparse-row predicate.
	files, fset = patchArchWallSource(t, path, archWallPatch{
		old: "\t\tlen(s.retiring) == 0 &&\n\t\ts.retryAt.IsZero()\n",
		new: "\t\tlen(s.retiring) == 0\n",
	})
	if got := hostStateSparseViolations(files, fset); len(got) == 0 {
		t.Fatal("sparse-row wall stayed green after empty() stopped consulting a live field")
	}
}
