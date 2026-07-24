package hostcommon

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// ActorFactory is the "def" every out-generation entry point speaks (spec
// actorbase-v1 §4 S3: every construction entry point resolves this one definition shape.
// Home, compute, and declarations all resolve to this ONE shape. Downstream
// packages never receive the raw caps-to-actor constructor representation; the caps→actor weld happens INSIDE
// hostcommon's Build (below), the single seam this period.
type ActorFactory struct {
	Proc actorbase.Def
}

// Build constructs the actorrt.Actor one ActorFactory declares, over one
// incarnation's already-welded caps bundle. hooks configures the actorbase
// engine (spec §3's out-generation matrix: Home wires its own CancelRequest,
// while a daemon host wires its current DaemonOutbound capability bundle).
func Build(caps actorcaps.Caps, hooks actorbase.Hooks, f ActorFactory, options actorbase.Options) actorrt.Actor {
	return actorbase.NewWithOptions(caps, hooks, f.Proc, options)
}
