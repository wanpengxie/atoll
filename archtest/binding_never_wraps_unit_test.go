package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// spec §13.3: "Binding/port 实现 ActorEndpoint，但不实现或包装 actorrt Unit".
//
// A route and a body are two different things that happen to answer the same
// two verbs. `ActorEndpoint` (Deliver + CancelRequest) is the shared delivery
// face and BOTH sides legitimately have it — `*actorrt.Unit` satisfies it today.
// The separation lives one level up: `BindingResource` is what Host will accept
// as a ROUTE, and a Unit must never be able to be one. If it could, a body
// could be installed as its own route: Host would then hold a "remote route"
// whose teardown is a local body's teardown, and the whole Actual/RouteActual
// split (§13.3's "RouteActual 必须持 non-nil exact Binding，且不得持
// Unit/Incarnation") would be enforced by nothing but good intentions.
//
// Today that isolation is REAL but THIN, and it is not architectural — it is an
// accident of two method-set details: `BindingResource` needs `Close() error`
// and `Done() <-chan struct{}`, while `Unit` offers `Stop()` (no error) and
// `Done()`. Adding `Close() error` to Unit is an utterly natural evolution for a
// resource-holding type, and the day it lands the isolation silently evaporates
// with nothing red. This wall makes that one method the wall's explicit subject.
//
// The second half is the WRAPPING ban: nothing may hold a Unit and present it
// as a route. It is a TREE-LEVEL rule, and it is enforced two ways, because a
// literal `*actorrt.Unit` field is only the shallowest spelling:
//
//	(1) every production package outside the body owners is scanned — not a
//	    hand-kept root list, which turns a prohibition into an enumeration and
//	    leaves whatever was written after the list was last edited unguarded;
//
//	(2) the ban follows the pointer TRANSITIVELY. `actorhost.Snapshot` carries an
//	    exported `Unit *actorrt.Unit`; a field of type `actorhost.Snapshot`
//	    therefore holds a body handle just as surely as a field of type
//	    `*actorrt.Unit` does, while mentioning neither `actorrt` nor `Unit`. The
//	    taint set below is closed under that reachability.
//
// Taint propagates through EXPORTED fields only, and that is the whole point of
// the distinction: `*actorhost.Host` also reaches Units internally, but it
// reaches them through unexported fields, so a holder gets a supervisor handle
// and nothing else. `actorhost.Snapshot` publishes the pointer. Reachability, not
// mention, is what makes a field a wrapping.
//
// Momentary use stays legal: `snapshot, ok := host.Inspect(id)` in a local
// variable is how platform/home answers Stat, and this wall only looks at STRUCT
// FIELDS — the difference between borrowing an answer and keeping a handle.

// bindingResourceType / actorEndpointType are the two route-facing contracts.
var (
	bindingResourceType = reflect.TypeOf((*actorhost.BindingResource)(nil)).Elem()
	actorEndpointType   = reflect.TypeOf((*actorhost.ActorEndpoint)(nil)).Elem()
)

// concreteMethodMatches compares a concrete method against an interface method.
// A concrete method's reflect.Type carries the receiver as its first input, so
// the comparison drops it before matching arguments and results.
func concreteMethodMatches(got, want reflect.Method) bool {
	gotType, wantType := got.Type, want.Type
	if gotType.NumIn()-1 != wantType.NumIn() || gotType.NumOut() != wantType.NumOut() ||
		gotType.IsVariadic() != wantType.IsVariadic() {
		return false
	}
	for i := 0; i < wantType.NumIn(); i++ {
		if gotType.In(i+1) != wantType.In(i) {
			return false
		}
	}
	for i := 0; i < wantType.NumOut(); i++ {
		if gotType.Out(i) != wantType.Out(i) {
			return false
		}
	}
	return true
}

// missingRouteResourceMethods returns the BindingResource methods a type does
// NOT provide. Empty means the type can be installed as a Host route, which
// reflect confirms independently via Implements.
func missingRouteResourceMethods(candidate reflect.Type) []string {
	var missing []string
	for i := 0; i < bindingResourceType.NumMethod(); i++ {
		want := bindingResourceType.Method(i)
		got, ok := candidate.MethodByName(want.Name)
		if !ok || !concreteMethodMatches(got, want) {
			missing = append(missing, want.Name+strings.TrimPrefix(want.Type.String(), "func"))
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 && !candidate.Implements(bindingResourceType) {
		missing = append(missing, "<method-set mismatch reflect.Implements rejects>")
	}
	if len(missing) != 0 && candidate.Implements(bindingResourceType) {
		// Never let the reporting path disagree with the verdict.
		return nil
	}
	return missing
}

// TestActorRTUnitCannotBeInstalledAsARoute is the method-set half: a Unit must
// never satisfy BindingResource, so `actorhost.NewBinding(unit)` can never
// compile.
func TestActorRTUnitCannotBeInstalledAsARoute(t *testing.T) {
	unitType := reflect.TypeOf((*actorrt.Unit)(nil))

	missing := missingRouteResourceMethods(unitType)
	if len(missing) == 0 {
		t.Fatalf("*actorrt.Unit now satisfies actorhost.BindingResource — a body can be installed as its own route; "+
			"the body/route split must be restored (Unit keeps Stop(), routes keep %s)",
			bindingResourceType.String())
	}
	t.Logf("body/route isolation rests on Unit lacking: %s", strings.Join(missing, ", "))

	// The shared delivery face is expected on both sides; it is NOT what keeps
	// them apart. Recording it here keeps the wall honest about what it proves.
	if !unitType.Implements(actorEndpointType) {
		t.Logf("note: *actorrt.Unit no longer satisfies actorhost.ActorEndpoint")
	}
}

// unitWithCloseFixture is the trip proof for the method-set half: it is a Unit
// that grew exactly one method — `Close() error`, the single most natural
// addition to a type that owns a goroutine and a mailbox. It must be reported
// as route-installable, which is what proves the check above is live rather
// than vacuously green.
type unitWithCloseFixture struct{ *actorrt.Unit }

func (unitWithCloseFixture) Close() error { return nil }

func TestActorRTUnitRouteWallTripsWhenUnitGrowsClose(t *testing.T) {
	if missing := missingRouteResourceMethods(reflect.TypeOf(unitWithCloseFixture{})); len(missing) != 0 {
		t.Fatalf("trip fixture does not satisfy BindingResource (missing %v) — the method-set wall proves nothing", missing)
	}
}

// unitWrapScanRoots is every production tree in the repo. The wrapping ban is a
// statement about the whole tree; scanning a curated subset would silently
// exempt whatever package is created next.
var unitWrapScanRoots = []string{
	"../app", "../cmd", "../drivers", "../lib",
	"../platform", "../protocol", "../registry", "../runtime",
}

// unitHandleOwners are the packages whose whole job is owning a body: the type
// that DEFINES it, the Host that supervises managed Units, and the Kernel that
// custodies the one system Unit. Everywhere else, holding a Unit — directly or
// through a type that publishes one — IS the wrapping the spec forbids.
var unitHandleOwners = map[string]bool{
	"../runtime/actorrt":      true,
	"../runtime/actorhost":    true,
	"../runtime/systemkernel": true,
}

// isUnitHandleOwnerPath reports whether a production file belongs to a body
// owner package (the package directory itself, not its subtrees — a child
// package is a separate package and owns nothing).
func isUnitHandleOwnerPath(path string) bool {
	return unitHandleOwners[filepath.ToSlash(filepath.Dir(path))]
}

// typeMentionsUnitPointer reports whether a field type expression contains
// `*actorrt.Unit` anywhere — directly, embedded, behind a map/slice/channel, or
// inside a func signature.
func typeMentionsUnitPointer(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "actorrt" && selector.Sel.Name == "Unit" {
			found = true
		}
		return true
	})
	return found
}

// unitTypeRefs returns every named type a field type expression refers to,
// qualified as "pkg.Type". A bare identifier resolves against the declaring
// file's own package. Element types of maps, slices, arrays, channels, func
// signatures and generic instantiations are all followed, so a registry spelled
// `map[actor.ActorID]actorhost.Snapshot` refers to Snapshot exactly as a plain
// field of that type does.
//
// Import ALIASES are the one thing this cannot see (archtest has no type
// information); an aliased import is the same review-layer evasion the package
// doc already declares out of scope.
func unitTypeRefs(pkgName string, expr ast.Expr) []string {
	var out []string
	var walk func(ast.Expr)
	walk = func(node ast.Expr) {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			// `pkg.Type` — the qualifier is a package name, not a type, so the
			// walk stops here rather than also emitting a bare `Type` ref.
			if pkg, ok := value.X.(*ast.Ident); ok {
				out = append(out, pkg.Name+"."+value.Sel.Name)
			}
		case *ast.Ident:
			out = append(out, pkgName+"."+value.Name)
		case *ast.StarExpr:
			walk(value.X)
		case *ast.ParenExpr:
			walk(value.X)
		case *ast.Ellipsis:
			walk(value.Elt)
		case *ast.ArrayType:
			walk(value.Elt)
		case *ast.MapType:
			walk(value.Key)
			walk(value.Value)
		case *ast.ChanType:
			walk(value.Value)
		case *ast.IndexExpr:
			walk(value.X)
			walk(value.Index)
		case *ast.IndexListExpr:
			walk(value.X)
			for _, index := range value.Indices {
				walk(index)
			}
		case *ast.StructType:
			if value.Fields != nil {
				for _, field := range value.Fields.List {
					walk(field.Type)
				}
			}
		case *ast.InterfaceType:
			if value.Methods != nil {
				for _, method := range value.Methods.List {
					walk(method.Type)
				}
			}
		case *ast.FuncType:
			for _, list := range []*ast.FieldList{value.Params, value.Results} {
				if list == nil {
					continue
				}
				for _, field := range list.List {
					walk(field.Type)
				}
			}
		}
	}
	walk(expr)
	return out
}

// unitFieldIsExported reports whether a struct field is reachable from outside
// its own package. Taint only travels along such fields: an unexported field
// that happens to reach a Unit (every supervisor has one) hands its holder
// nothing, while an exported one hands over the pointer.
func unitFieldIsExported(field *ast.Field) bool {
	if len(field.Names) > 0 {
		for _, name := range field.Names {
			if name.IsExported() {
				return true
			}
		}
		return false
	}
	// Embedded: the promoted name is the embedded type's own name.
	expr := field.Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	switch value := expr.(type) {
	case *ast.SelectorExpr:
		return value.Sel.IsExported()
	case *ast.Ident:
		return value.IsExported()
	}
	return false
}

// unitCarrierStruct is one named struct type seen during the tree scan.
type unitCarrierStruct struct {
	pkg    string
	name   string
	fields []*ast.Field
}

// buildUnitTaintSet returns every named struct type from which a
// `*actorrt.Unit` is reachable through exported fields, closed to a fixpoint.
// The seed is a literal Unit pointer field; each round adds the types that carry
// an already-tainted type. `actorhost.Snapshot` is the live member: holding one
// is holding a body handle under another name.
func buildUnitTaintSet(files map[string]*ast.File) map[string]bool {
	var structs []unitCarrierStruct
	for _, file := range files {
		pkg := file.Name.Name
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				return true
			}
			structs = append(structs, unitCarrierStruct{pkg: pkg, name: spec.Name.Name, fields: structType.Fields.List})
			return true
		})
	}
	taint := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, carrier := range structs {
			key := carrier.pkg + "." + carrier.name
			if taint[key] {
				continue
			}
			for _, field := range carrier.fields {
				if !unitFieldIsExported(field) {
					continue
				}
				if !typeMentionsUnitPointer(field.Type) && len(unitRefsTainted(carrier.pkg, field.Type, taint)) == 0 {
					continue
				}
				taint[key] = true
				changed = true
				break
			}
		}
	}
	return taint
}

func unitRefsTainted(pkgName string, expr ast.Expr, taint map[string]bool) []string {
	var hits []string
	for _, ref := range unitTypeRefs(pkgName, expr) {
		if taint[ref] {
			hits = append(hits, ref)
		}
	}
	sort.Strings(hits)
	return hits
}

// unitWrappingFields reports every struct field in a non-owner package whose
// type reaches a Unit — literally, or through a Unit-carrying type. Named fields
// are the "fast path cache"; embedded ones are worse — embedding promotes
// Deliver/CancelRequest/Done straight onto the wrapper, so a Binding-shaped type
// acquires a body's face for free.
func unitWrappingFields(files map[string]*ast.File, fset *token.FileSet, taint map[string]bool) []string {
	var v []string
	for path, file := range files {
		if isUnitHandleOwnerPath(path) {
			continue
		}
		pkg := file.Name.Name
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
				name := "<embedded>"
				if len(field.Names) > 0 {
					name = field.Names[0].Name
				}
				switch {
				case typeMentionsUnitPointer(field.Type):
					v = append(v, fmt.Sprintf(
						"%s:%d type %s holds a Unit in field %s — a route/port never wraps a body (Deliver/Cancel/Stop would bypass Host)",
						path, fset.Position(field.Pos()).Line, spec.Name.Name, name))
				default:
					hits := unitRefsTainted(pkg, field.Type, taint)
					if len(hits) == 0 {
						continue
					}
					v = append(v, fmt.Sprintf(
						"%s:%d type %s holds %s in field %s — that type publishes a *actorrt.Unit, so the field is a body handle in disguise (inspect momentarily, never keep)",
						path, fset.Position(field.Pos()).Line, spec.Name.Name, strings.Join(hits, "/"), name))
				}
			}
			return true
		})
	}
	return v
}

// loadUnitWrapTree parses every production file in the repo under one FileSet.
func loadUnitWrapTree(t *testing.T) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, root := range unitWrapScanRoots {
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			files[filepath.ToSlash(path)] = file
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	if len(files) == 0 {
		t.Fatal("no production files found — the no-wrapping wall lost its subject")
	}
	return files, fset
}

// linkRouteOwnerPkg is the package that actually implements BindingResource;
// it is the one the named break targets and it already imports actorrt.
const linkRouteOwnerPkg = "../platform/internal/link"

func TestNoTypeOutsideBodyOwnersWrapsAUnit(t *testing.T) {
	files, fset := loadUnitWrapTree(t)
	taint := buildUnitTaintSet(files)

	// Footing 1: the transitive half is only meaningful if the taint closure
	// actually found the Unit-publishing type the audit named. If Snapshot stops
	// exposing the pointer, this half must be retuned rather than left green.
	if !taint["actorhost.Snapshot"] {
		names := make([]string, 0, len(taint))
		for name := range taint {
			names = append(names, name)
		}
		sort.Strings(names)
		t.Fatalf("actorhost.Snapshot is no longer a Unit carrier (taint set: %s) — "+
			"retune the transitive half of the no-wrapping wall", strings.Join(names, ", "))
	}

	// Footing 2: the wall is pointed at the package that really does implement
	// BindingResource. If link stopped doing so, this wall is aimed at nothing.
	source, err := readFile(linkRouteOwnerPkg + "/physical.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "_ actorhost.BindingResource = (*Binding)(nil)") {
		t.Fatal("platform/internal/link no longer asserts *Binding is a BindingResource — retune the no-wrapping wall")
	}

	failViolations(t, "no type outside the body owners wraps an actorrt Unit",
		unitWrappingFields(files, fset, taint))
}

// TestNoUnitWrappingWallTripsOnBindingFastPath is the trip proof for the
// wrapping half. The taint set is always derived from the REAL tree, so the
// synthetic fixtures are checked against the same closure production is.
func TestNoUnitWrappingWallTripsOnBindingFastPath(t *testing.T) {
	treeFiles, _ := loadUnitWrapTree(t)
	taint := buildUnitTaintSet(treeFiles)

	t.Run("named fast-path field on the real Binding", func(t *testing.T) {
		const path = linkRouteOwnerPkg + "/physical.go"
		const anchor = `type Binding struct {
	session  *AuthenticatedLinkSession
	endpoint actorhost.ActorEndpoint`
		const patched = `type Binding struct {
	session  *AuthenticatedLinkSession
	endpoint actorhost.ActorEndpoint
	// "local delivery fast path": skip the wire when the body is right here.
	unit     *actorrt.Unit`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := unitWrappingFields(files, fset, taint); len(got) == 0 {
			t.Fatal("no-wrapping wall stayed green on a Binding that caches a Unit")
		}
	})

	// The audit's transitive break, on real source: platform/home already calls
	// Inspect and already holds long-lived state, so caching the whole Snapshot
	// "to avoid a second Inspect" is one field — and it smuggles the body pointer
	// in without the words `actorrt` or `Unit` appearing anywhere.
	t.Run("assembly caches a whole actorhost.Snapshot", func(t *testing.T) {
		const path = "../platform/home/actor_system.go"
		const anchor = `type actorSystem struct {
	home         *Home`
		const patched = `type actorSystem struct {
	home         *Home
	// "avoid a second Inspect on the status path"
	lastSnapshot actorhost.Snapshot`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := unitWrappingFields(files, fset, taint); len(got) == 0 {
			t.Fatal("no-wrapping wall stayed green on a cached actorhost.Snapshot — the transitive half is not live")
		}
	})

	fixtures := []struct {
		name string
		path string
		src  string
	}{
		{
			name: "embedded Unit promotes a body face onto a route",
			path: "../platform/internal/link/link_fixture.go",
			src: `package link
type Binding struct {
	*actorrt.Unit
	session *AuthenticatedLinkSession
}`,
		},
		{
			name: "indirect containers still count",
			path: "../platform/compute/compute_fixture.go",
			src: `package compute
type slotIndex struct {
	bodies map[string]*actorrt.Unit
}`,
		},
		{
			name: "a registry keyed to Unit-carrying values",
			path: "../drivers/registry_fixture.go",
			src: `package drivers
type inspectCache struct {
	rows map[actor.ActorID]actorhost.Snapshot
}`,
		},
		{
			name: "a slice of carriers",
			path: "../app/history_fixture.go",
			src: `package app
type history struct {
	seen []actorhost.Snapshot
}`,
		},
		{
			name: "a package the old root list never scanned",
			path: "../runtime/harness/fixture.go",
			src: `package harness
type localFastPath struct {
	body *actorrt.Unit
}`,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, tc.path, tc.src)
			if got := unitWrappingFields(files, fset, taint); len(got) == 0 {
				t.Fatalf("no-wrapping wall stayed green on the break form %q", tc.name)
			}
		})
	}

	legal := []struct {
		name string
		path string
		src  string
	}{
		{
			name: "body owners stay allowed",
			path: "../runtime/actorhost/actorhost_fixture.go",
			src: `package actorhost
type bodyActual struct {
	key  AttemptKey
	unit *actorrt.Unit
}`,
		},
		{
			name: "the kernel custodies the system body",
			path: "../runtime/systemkernel/kernel_fixture.go",
			src: `package systemkernel
type Kernel struct {
	unit *actorrt.Unit
}`,
		},
		{
			name: "a momentary Snapshot in a local and a parameter",
			path: "../platform/home/momentary_fixture.go",
			src: `package home
type actorSystem struct {
	home *Home
}

func (a *actorSystem) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	snapshot, ok := a.home.serverHost.Inspect(id)
	if !ok {
		return actorrt.UnitStat{}, false
	}
	return describe(snapshot)
}

func describe(snapshot actorhost.Snapshot) (actorrt.UnitStat, bool) {
	return snapshot.Unit.Stat(), true
}`,
		},
		{
			name: "holding the supervisor is not holding a body",
			path: "../platform/home/supervisor_fixture.go",
			src: `package home
type Home struct {
	serverHost *actorhost.Host
}`,
		},
	}
	for _, tc := range legal {
		t.Run("allowed: "+tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, tc.path, tc.src)
			if got := unitWrappingFields(files, fset, taint); len(got) != 0 {
				t.Fatalf("no-wrapping wall fired on the sanctioned shape %q: %v", tc.name, got)
			}
		})
	}
}
