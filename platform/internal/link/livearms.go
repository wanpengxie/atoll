package link

import (
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// NewLiveArms bundles a RebindableArms' four wire-flap facades into a gated
// Caps set, welded to inc and gated on host — the daemon (attached-compute)
// counterpart of platform/home.go's buildCaps. It weaves the SAME four
// membranes buildCaps weaves (livePen / liveResourceAccess (Access) /
// liveAccess (State) / liveSchedule), just over the daemon's own
// relay-backed arms instead of home's directly-minted ones, and gated by the
// DAEMON's own actorrt.Runtime (the local host of the cell being born)
// rather than home's.
//
// This closes G12 (§10.13 推导7①, sealed-pen prior art extended to the port
// path): "a factory must not write" is a structural rule on the cell path —
// rt.SpawnIfAbsent's build closure runs BEFORE go-live, so a livePen constructed
// inside it always fences (IsLive(inc)==false until the closure returns).
// Before this membrane, compute.Run's build closure handed the factory the
// RAW RebindableArms facades directly — ungated, already "live" the instant
// the wire stream was open — so a daemon-hosted actor's construction-time
// write escaped un-fenced, a softened half of the invariant relative to a
// cell born at home. Wiring NewLiveArms into the build closure closes that
// parity gap: every arm refuses every call until inc goes live, exactly like
// home's buildCaps.
//
// Spawn is deliberately left zero — the fork/despawn arm does not cross the
// wire this period (期6 拍); compute.Run's factory is only ever handed the
// four wire-flap arms.
func NewLiveArms(rb *RebindableArms, inc actorrt.Incarnation, host *actorrt.Runtime) actorcaps.Caps {
	return actorcaps.Caps{
		Pen:      NewLivePen(rb.Pen(), inc, host),
		Access:   NewLiveResourceAccess(rb.Access(), inc, host),
		State:    NewLiveAccess(rb.State(), inc, host),
		Schedule: NewLiveSchedule(rb.Schedule(), inc, host),
	}
}
