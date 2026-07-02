package link

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/protocol/resource"
	"github.com/wanpengxie/ActOS/runtime/accessdoor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// ErrAccessNotLive is the WHEN-validity rejection on the access plane: a
// liveAccess whose welded incarnation is no longer the live embodiment
// (despawned / dead / replaced) refuses to invoke. It is the plane-2 twin of
// ErrWriterNotLive — a capability captured by a goroutine that outlived its
// incarnation cannot act on resources on its behalf.
var ErrAccessNotLive = errors.New("link: access capability no longer the live incarnation")

// liveAccess is the liveCap (WHEN-validity membrane) over a raw
// accessdoor.AccessHandle: a thin wrapper that, per invoke, first checks the
// host that the welded incarnation is STILL live (by POINTER, ABA-safe;
// lock-free) and only then forwards to the raw handle. It is the plane-2
// (access/state) twin of livePen — the substrate (actorrt) owns liveness, the
// door owns the caller weld + decision tree, and this wrapper composes the two
// with no change to either (bi-layer: accessdoor never imports actorrt-liveness;
// liveAccess lives here in link, beside livePen, so the port path can construct
// it too).
//
// HONEST SCOPE: it fences "a leaked cap used long after death" and "ABA across
// an incarnation replacement". The sub-microsecond window between the IsLive
// check passing and raw.Invoke committing is the accepted in-flight seam (a
// current incarnation's best-effort last gasp). liveAccess is a lease, not
// strict fencing.
type liveAccess struct {
	raw  accessdoor.AccessHandle
	inc  actorrt.Incarnation
	host *actorrt.Runtime
}

// NewLiveAccess wraps raw in the WHEN-validity membrane welded to inc, gated on
// host. Participant cells (agent/tool/human) are born with this over both their
// channel-scoped (Mint) and actor-scoped (MintState) handles; substrate anchors
// (system/sysactor) deliberately use the raw handle — no incarnation gate — for
// the same reason livePen skips anchors (see NewLivePen).
func NewLiveAccess(raw accessdoor.AccessHandle, inc actorrt.Incarnation, host *actorrt.Runtime) accessdoor.AccessHandle {
	return liveAccess{raw: raw, inc: inc, host: host}
}

// Invoke implements accessdoor.AccessHandle: fence on the welded incarnation's
// liveness, then forward to the raw handle (which resolves under the welded
// caller). The incarnation NEVER rides any wire — the caller is stamped by the
// raw handle exactly as before.
func (a liveAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.Outcome{}, ErrAccessNotLive
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}
