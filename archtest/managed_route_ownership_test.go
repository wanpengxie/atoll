package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "managed ActorID Deliver/Cancel/current route 只在 actorhost".
//
// "Which body receives this envelope right now" is one decision, and it belongs
// to exactly one owner. HostSupervisor makes it under its own span lock, against
// its own Actual/Route state, with the retire/publish sequencing that makes the
// answer honest across a generation switch. Every other layer either hands an
// ActorID to that owner or holds an already-resolved handle — nobody re-derives
// the answer.
//
// The dangerous shape here is unusually cheap to write. `platform/compute`
// LEGITIMATELY holds `actorhost.ActualCurrent` — the currency probe is right
// there, in the same file as the five capability facades. Adding
//
//	if !p.slot.current.IsCurrent() { …deliver some other way… }
//
// inside `outboundPen.Write` looks like defensive hardening, compiles, and
// leaves every existing wall green. What it actually creates is a second router:
// a facade that decides, per call, which path a message takes — using a probe
// that answers about the PHYSICAL incarnation while the arms bundle it just
// loaded belongs to a specific generation. The two can disagree, and the failure
// is a message quietly taking the wrong path rather than an error.
//
// The wall has three parts, matching the three ways the decision can move:
//
//	A. the set of by-ActorID delivery/cancel ENTRY POINTS is closed, and every
//	   one outside actorhost is a pure delegation that consults no currency;
//	B. no capability facade consults currency at all — the load helpers own that
//	   verdict, and a facade gets one already-decided bundle;
//	C. nobody outside actorhost indexes endpoints by ActorID — an ActorID→route
//	   table IS the routing decision, wherever it lives.

const managedRouteOwnerPkg = "../runtime/actorhost"

// managedRouteDelegates are the by-ActorID delivery/cancel entry points that may
// exist outside the owner. Each is a pass-through:
//
//	actor_system.go — ChannelActors, whose ONE extra branch is the spec's
//	                  SystemActorID→SystemKernel case;
//	control.go      — Home's cancel reach and its upstream-cancel authorization,
//	                  both of which end in the ChannelActors delegate above.
var managedRouteDelegates = map[string]bool{
	"../platform/home/actor_system.go:Deliver":         true,
	"../platform/home/actor_system.go:CancelRequest":   true,
	"../platform/home/control.go:cancelRequest":        true,
	"../platform/home/control.go:handleCancelUpstream": true,
}

// currencyProbeVerbs / currencyProbeFields are how a routing decision is
// spelled: ask whether some coordinate is still the live one.
var (
	currencyProbeVerbs  = map[string]bool{"IsCurrent": true, "CurrentAttempt": true}
	currencyProbeFields = map[string]bool{"current": true, "identity": true, "attempt": true}
)

// routeCarrierTypeNames are the things an ActorID→X table would have to hold to
// be a routing table.
var routeCarrierTypeNames = []string{"ActorEndpoint", "Binding", "ActorStream", "actorrt.Unit"}

// flattenedParamTypes renders one signature's parameter types, one entry per
// declared name, so a shape can be matched positionally.
func flattenedParamTypes(fset *token.FileSet, params *ast.FieldList) []string {
	if params == nil {
		return nil
	}
	var out []string
	for _, field := range params.List {
		text := expressionText(fset, field.Type)
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			out = append(out, text)
		}
	}
	return out
}

// isByActorIDDispatchShape recognises the two managed dispatch signatures by
// TYPE, not by name: (ActorID, envelope) and (ActorID, request id). A renamed
// `Route`/`Send`/`Post` has the same shape and is caught identically.
func isByActorIDDispatchShape(types []string) bool {
	if len(types) != 2 || types[0] != "actor.ActorID" {
		return false
	}
	return types[1] == "*message.Envelope" || types[1] == "message.ID"
}

// managedRouteEntryPointKeys returns every by-ActorID dispatch entry point
// declared outside the owner, whitelisted or not. It is what both the wall and
// the wall's own footing check are built on.
func managedRouteEntryPointKeys(files map[string]*ast.File, fset *token.FileSet) []string {
	var keys []string
	for path, file := range files {
		if strings.Contains(path, strings.TrimPrefix(managedRouteOwnerPkg, "../")) {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if isByActorIDDispatchShape(flattenedParamTypes(fset, fn.Type.Params)) {
				keys = append(keys, path+":"+fn.Name.Name)
			}
		}
	}
	return keys
}

// managedRouteEntryPointViolations enforces part A.
func managedRouteEntryPointViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		if strings.Contains(path, strings.TrimPrefix(managedRouteOwnerPkg, "../")) {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if !isByActorIDDispatchShape(flattenedParamTypes(fset, fn.Type.Params)) {
				continue
			}
			key := path + ":" + fn.Name.Name
			if !managedRouteDelegates[key] {
				v = append(v, fmt.Sprintf(
					"%s is a by-ActorID dispatch entry point outside actorhost — resolving which body receives a managed ActorID is the Host's decision alone",
					key))
				continue
			}
			// A sanctioned delegate may pass the ActorID on; it may never
			// decide anything with it.
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !currencyProbeVerbs[selector.Sel.Name] {
					return true
				}
				v = append(v, fmt.Sprintf(
					"%s:%d consults %s inside a dispatch delegate — a delegate forwards, it does not re-decide the current route",
					key, fset.Position(call.Pos()).Line, selector.Sel.Name)) //nolint:gocritic
				return true
			})
		}
	}
	return v
}

// outboundLoadHelpers names the bundle-load helpers this wall keys on. The
// one-load/one-call wall used to share this list and no longer does — it derives
// its loader set from shape (returns *OutboundArmsBundle off an arms.Load()).
// Here a name list is still adequate: this wall asks "does a facade re-open the
// currency question", and a renamed helper simply drops that facade from the
// currency check without letting the retry shape through anywhere.
var outboundLoadHelpers = map[string]bool{
	"loadConnected": true,
	"loadIdentity":  true,
	"loadAttempt":   true,
	"loadPhysical":  true,
}

// managedRouteFacadeCurrencyViolations enforces part B: a capability facade
// receives an already-decided bundle and must not re-open the currency question.
func managedRouteFacadeCurrencyViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	facades := 0
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || outboundLoadHelpers[fn.Name.Name] {
				continue
			}
			loads := false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok &&
					outboundLoadHelpers[selector.Sel.Name] {
					loads = true
				}
				return true
			})
			if !loads {
				continue
			}
			facades++
			where := fmt.Sprintf("%s:%s", path, fn.Name.Name)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch {
				case currencyProbeVerbs[selector.Sel.Name]:
					v = append(v, fmt.Sprintf(
						"%s:%d asks %s — the load helper already answered currency; a facade that asks again is deciding a route",
						where, fset.Position(selector.Pos()).Line, selector.Sel.Name))
				case currencyProbeFields[selector.Sel.Name] &&
					archWallRootIdent(selector.X) != "":
					v = append(v, fmt.Sprintf(
						"%s:%d reaches the %q currency probe — probes belong to the load helpers, facades get one decided bundle",
						where, fset.Position(selector.Pos()).Line, selector.Sel.Name))
				}
				return true
			})
		}
	}
	if facades < 5 {
		v = append(v, fmt.Sprintf("capability facades found=%d, want at least the five arms — the facade currency wall lost its subject", facades))
	}
	return v
}

// managedRouteIndexViolations enforces part C.
func managedRouteIndexViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		if strings.Contains(path, strings.TrimPrefix(managedRouteOwnerPkg, "../")) {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			mapType, ok := node.(*ast.MapType)
			if !ok || expressionText(fset, mapType.Key) != "actor.ActorID" {
				return true
			}
			value := expressionText(fset, mapType.Value)
			for _, carrier := range routeCarrierTypeNames {
				if strings.Contains(value, carrier) {
					v = append(v, fmt.Sprintf(
						"%s:%d indexes %s by ActorID — an ActorID→endpoint table is a routing decision, and that decision lives in actorhost",
						path, fset.Position(mapType.Pos()).Line, value))
					break
				}
			}
			return true
		})
	}
	return v
}

func TestManagedRouteDecisionLivesOnlyInActorHost(t *testing.T) {
	for _, root := range []string{"../app", "../cmd", "../drivers", "../lib", "../platform", "../registry", "../runtime"} {
		files, fset := loadArchWallPackage(t, root)
		failViolations(t, "by-ActorID dispatch entry points are closed and delegate-only",
			managedRouteEntryPointViolations(files, fset))
		failViolations(t, "no ActorID→endpoint index outside actorhost",
			managedRouteIndexViolations(files, fset))
	}

	files, fset := loadArchWallPackage(t, outboundFacadePkg)
	failViolations(t, "capability facades never re-decide currency",
		managedRouteFacadeCurrencyViolations(files, fset))

	// Footing: the owner must still own it.
	ownerFiles, ownerFset := loadArchWallPackage(t, managedRouteOwnerPkg)
	owners := 0
	for _, file := range ownerFiles {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if isByActorIDDispatchShape(flattenedParamTypes(ownerFset, fn.Type.Params)) {
				owners++
			}
		}
	}
	if owners < 2 {
		t.Fatalf("actorhost declares %d by-ActorID dispatch entry points, want Deliver and CancelRequest — retune the managed route wall", owners)
	}

	// Footing: the whitelist must describe real code. A stale entry would let
	// the wall look tight while guarding a set it can no longer see.
	homeFiles, homeFset := loadArchWallPackage(t, "../platform/home")
	seen := map[string]bool{}
	for _, key := range managedRouteEntryPointKeys(homeFiles, homeFset) {
		seen[key] = true
	}
	for delegate := range managedRouteDelegates {
		if !seen[delegate] {
			t.Errorf("whitelisted dispatch delegate %s no longer exists — the entry-point wall is guarding a stale set", delegate)
		}
	}
}

// TestManagedRouteWallTripsOnFacadeSideRouting is the trip proof. The facade
// case is the audit's named break, applied verbatim to the real outbound facade.
func TestManagedRouteWallTripsOnFacadeSideRouting(t *testing.T) {
	t.Run("facade re-decides currency and picks another path", func(t *testing.T) {
		const path = outboundFacadePkg + "/outbound.go"
		const anchor = `func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	bundle, err := p.slot.loadAttempt()`
		const patched = `func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if !p.slot.current.IsCurrent() {
		return p.slot.writeViaFallback(ctx, env)
	}
	bundle, err := p.slot.loadAttempt()`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := managedRouteFacadeCurrencyViolations(files, fset); len(got) == 0 {
			t.Fatal("facade currency wall stayed green on an inline route decision inside outboundPen.Write")
		}
	})

	t.Run("a second by-ActorID dispatch entry point", func(t *testing.T) {
		const path = outboundFacadePkg + "/outbound.go"
		const anchor = "// Wake coalesces a level-convergence hint."
		const patched = `// Deliver routes an envelope to whichever slot currently holds this actor.
func (d *DaemonOutbound) Deliver(id actor.ActorID, env *message.Envelope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for slot := range d.slots {
		if slot.id == id {
			return slot.arms.Load().Pen.WriteEnvelope(env)
		}
	}
	return ErrOutboundDisconnected
}

// Wake coalesces a level-convergence hint.`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := managedRouteEntryPointViolations(files, fset); len(got) == 0 {
			t.Fatal("entry-point wall stayed green on a new by-ActorID dispatch outside actorhost")
		}
	})

	t.Run("a sanctioned delegate starts deciding", func(t *testing.T) {
		const path = "../platform/home/actor_system.go"
		const anchor = `func (a *actorSystem) Deliver(id actor.ActorID, env *message.Envelope) error {
	if id == actor.SystemActorID {`
		const patched = `func (a *actorSystem) Deliver(id actor.ActorID, env *message.Envelope) error {
	if !a.home.serverHost.IsCurrent(id) {
		return errors.New("not current")
	}
	if id == actor.SystemActorID {`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := managedRouteEntryPointViolations(files, fset); len(got) == 0 {
			t.Fatal("entry-point wall stayed green on a delegate that re-decides the current route")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "renamed dispatch with the same shape",
			src: `package link
func (s *AuthenticatedLinkSession) Route(id actor.ActorID, env *message.Envelope) error {
	return nil
}`,
			check: managedRouteEntryPointViolations,
		},
		{
			name: "renamed cancel dispatch with the same shape",
			src: `package presence
func (f *Fold) Abandon(id actor.ActorID, requestID message.ID) {
}`,
			check: managedRouteEntryPointViolations,
		},
		{
			name: "an ActorID keyed endpoint table",
			src: `package home
type routes struct {
	byActor map[actor.ActorID]actorhost.ActorEndpoint
}`,
			check: managedRouteIndexViolations,
		},
		{
			name: "an ActorID keyed binding table",
			src: `package compute
type routes struct {
	byActor map[actor.ActorID]*link.Binding
}`,
			check: managedRouteIndexViolations,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "managed_route_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}
