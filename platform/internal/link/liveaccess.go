package link

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// ErrAccessNotLive is the WHEN-validity rejection on the access plane: a
// liveAccess whose welded incarnation is no longer the live embodiment
// (despawned / dead / replaced) refuses to invoke. It is the plane-2 twin of
// ErrWriterNotLive — a capability captured by a goroutine that outlived its
// incarnation cannot act on resources on its behalf.
var ErrAccessNotLive error = codedSentinel{code: "access_not_live", message: "link: access capability no longer the live incarnation"}

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

// liveResourceAccess is the RESOURCE-FACE twin of liveAccess (期11 spec
// §3.2's "膜层...各拆两型" — the membrane layer's resource/state split):
// same WHEN-validity fence, over accessdoor.ResourceAccessHandle's full
// method set (Invoke+Create+Stat+List) instead of Invoke alone. A single Go
// type wrapping BOTH scopes would either grow Create/Stat/List on a
// state-scoped instance (violating "状态面不实现空方法") or need
// ErrUnsupported branches per method (the explicit red-line-forbidden
// intermediate state, §8.7) — so liveAccess and liveResourceAccess stay two
// separate types sharing nothing but the IsLive check's shape.
type liveResourceAccess struct {
	raw  accessdoor.ResourceAccessHandle
	inc  actorrt.Incarnation
	host *actorrt.Runtime
}

// NewLiveResourceAccess wraps raw in the WHEN-validity membrane welded to
// inc, gated on host — the resource-face counterpart of NewLiveAccess.
// Participant cells are born with this over their channel-scoped (Mint)
// handle; substrate anchors (system/sysactor) deliberately use the raw
// handle — no incarnation gate — for the same reason NewLivePen/NewLiveAccess
// skip anchors.
//
// ResourceAccessHandle has one uniform file call face on every host. The raw
// home handle answers capability_unavailable; the daemon proxy forwards to its
// byte plane. This membrane always forwards that same face after its liveness
// check, so host placement never changes the method set visible to actorbase.
func NewLiveResourceAccess(raw accessdoor.ResourceAccessHandle, inc actorrt.Incarnation, host *actorrt.Runtime) accessdoor.ResourceAccessHandle {
	return liveResourceAccess{raw: raw, inc: inc, host: host}
}

func (a liveResourceAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.Outcome{}, ErrAccessNotLive
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}

func (a liveResourceAccess) Create(ctx context.Context, id resource.ResourceID, spec accessdoor.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.Outcome{}, ErrAccessNotLive
	}
	return a.raw.Create(ctx, id, spec, initial)
}

func (a liveResourceAccess) Stat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.StatResult{}, ErrAccessNotLive
	}
	return a.raw.Stat(ctx, id)
}

func (a liveResourceAccess) List(ctx context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.ListPage{}, ErrAccessNotLive
	}
	return a.raw.List(ctx, q)
}

func (a liveResourceAccess) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, ErrAccessNotLive
	}
	return a.raw.Open(ctx, id, mode)
}

func (a liveResourceAccess) Redeem(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.FileAccess{}, ErrAccessNotLive
	}
	return a.raw.Redeem(ctx, route)
}

var _ accessdoor.FileOpener = liveResourceAccess{}
