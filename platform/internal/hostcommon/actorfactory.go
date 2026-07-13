package hostcommon

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// ActorFactory is the "def" every out-generation entry point speaks (spec
// actorbase-v1 §4 S3: _, _, _ = Home.SpawnIfAbsent(ctx,id,kind,def) / CapsFactoryBuilder /
// ComputeBuilder / ActorDecl all resolve to this ONE shape). It replaces the
// old bare `func(actorcaps.Caps) actorrt.Actor` — no downstream package ever
// names actorcaps.Caps to produce one; the caps→actor weld happens INSIDE
// platform's build (below), the single seam this period.
//
// Exactly one of Proc / Legacy is set (fullCaps is platform's own test seam,
// see CapsFactory):
//
//   - Proc is the actorbase-migrated shape (actorbase spec §1.6): a Proc
//     speaks only Sys, never Caps.
//   - Legacy is the pre-actorbase shape every current actor implementation
//     still wears (echo/xhs/device/kimi/agent providers/sysactor — the S5/S5b
//     migration queue): a raw actorrt.Actor constructor over the one
//     capability every current implementation actually reaches for
//     (harness.Pen — no consumer today reaches Access/State/Schedule/Spawn
//     from a factory closure). This field shrinks to zero as each consumer
//     migrates to Proc; it never grows.
type ActorFactory struct {
	Proc   actorbase.Def
	Legacy func(pen harness.Pen) actorrt.Actor

	// fullCaps is platform's OWN internal test seam: platform is the one
	// place actorcaps.Caps is a legitimate, freely-nameable type (spec's
	// confinement allowlist), so its own tests may exercise a factory over
	// the WHOLE bundle (e.g. asserting which caps a fork child was welded)
	// without inventing a second production-facing form. Never set outside
	// this package's _test.go files.
	fullCaps func(actorcaps.Caps) actorrt.Actor
}

// CapsFactory builds an ActorFactory from a raw full-Caps constructor — for
// platform's own tests (see ActorFactory.fullCaps' doc). Exported so
// platform_test (external test package) files can reach it; production code
// never calls this.
func CapsFactory(f func(actorcaps.Caps) actorrt.Actor) ActorFactory {
	return ActorFactory{fullCaps: f}
}

// Build constructs the actorrt.Actor one ActorFactory declares, over one
// incarnation's already-welded caps bundle. hooks configures the actorbase
// engine when Proc is set (spec §3's out-generation matrix: Home wires its
// own CancelRequest, a daemon host wires none — the known cancel-upstream
// gap); Legacy/fullCaps ignore it (a raw actor owns whatever cancel wiring it
// has today, unchanged).
func Build(caps actorcaps.Caps, hooks actorbase.Hooks, f ActorFactory) actorrt.Actor {
	switch {
	case f.fullCaps != nil:
		return f.fullCaps(caps)
	case f.Legacy != nil:
		return f.Legacy(caps.Pen)
	default:
		return actorbase.New(caps, hooks, f.Proc)
	}
}
