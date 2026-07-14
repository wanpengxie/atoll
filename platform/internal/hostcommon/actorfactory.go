package hostcommon

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// ActorFactory is the "def" every out-generation entry point speaks (spec
// actorbase-v1 §4 S3: _, _, _ = Home.SpawnIfAbsent(ctx,id,kind,def) / CapsFactoryBuilder /
// compute.Builder / ActorDecl all resolve to this ONE shape). It replaces the
// old bare `func(actorcaps.Caps) actorrt.Actor` — no downstream package ever
// names actorcaps.Caps to produce one; the caps→actor weld happens INSIDE
// hostcommon's Build (below), the single seam this period.
//
// Proc is the single production representation. fullCaps is platform's own
// test seam (see CapsFactory); it is not exposed as a production constructor.
type ActorFactory struct {
	Proc actorbase.Def

	// fullCaps is the platform tree's OWN internal test seam: the platform
	// tree is the one place actorcaps.Caps is a legitimate, freely-nameable
	// type (spec's confinement allowlist), so its tests may exercise a
	// factory over the WHOLE bundle (e.g. asserting which caps a fork child
	// was welded) without inventing a second production-facing form. Set
	// only via CapsFactory, only from platform-tree _test.go files (today:
	// platform/home's internal and external tests).
	fullCaps func(actorcaps.Caps) actorrt.Actor
}

// CapsFactory builds an ActorFactory from a raw full-Caps constructor — for
// the platform tree's own tests (see ActorFactory.fullCaps' doc). Exported so
// platform-tree test packages (today: platform/home's internal and external
// tests) can reach it; production code never calls this.
func CapsFactory(f func(actorcaps.Caps) actorrt.Actor) ActorFactory {
	return ActorFactory{fullCaps: f}
}

// Build constructs the actorrt.Actor one ActorFactory declares, over one
// incarnation's already-welded caps bundle. hooks configures the actorbase
// engine (spec §3's out-generation matrix: Home wires its own CancelRequest,
// a daemon host wires computeRing's cellCancelForwarder).
func Build(caps actorcaps.Caps, hooks actorbase.Hooks, f ActorFactory) actorrt.Actor {
	if f.fullCaps != nil {
		return f.fullCaps(caps)
	}
	return actorbase.New(caps, hooks, f.Proc)
}
