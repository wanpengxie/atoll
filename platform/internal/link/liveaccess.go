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
// FileOpener pass-through (found+fixed during 期11 S6's platform-level walk
// verification — a REAL bug, not a spec-authored requirement): raw is a
// boundHandle for a home-hosted caller (which does NOT implement
// accessdoor.FileOpener — Open/CreateFile correctly answer ErrUnsupported
// there, §5's own documented scope) but a remoteResourceHandle for a
// daemon-hosted caller (which DOES implement it — this is the ONE avatar
// §5 built Open/Redeem for in the first place, dial.go's own doc). This
// wrapper used to ALWAYS return the plain liveResourceAccess value — which
// has no Open/Redeem methods — so lib/actorbase's own type-assertion
// (`r.h.(accessdoor.FileOpener)`) failed for EVERY caller, daemon-hosted
// included, silently defeating §5's entire Open/CreateFile build (S5's own
// handoff claimed "ready for a consumer"; the first actual actorbase-level
// caller — this section's walk tests — is what surfaced it, since S4/S5's
// own tests drove remoteResourceHandle directly, never through this
// membrane). Fixed by returning a DIFFERENT concrete type when raw itself
// implements FileOpener — never by making liveResourceAccess unconditionally
// claim the interface (which would have to fabricate a NEW "unsupported"
// error identity for the home-hosted case, changing existing behavior
// instead of just completing it).
func NewLiveResourceAccess(raw accessdoor.ResourceAccessHandle, inc actorrt.Incarnation, host *actorrt.Runtime) accessdoor.ResourceAccessHandle {
	base := liveResourceAccess{raw: raw, inc: inc, host: host}
	if fo, ok := raw.(accessdoor.FileOpener); ok {
		return liveResourceAccessFileOpener{liveResourceAccess: base, fo: fo}
	}
	return base
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

// liveResourceAccessFileOpener is liveResourceAccess PLUS the
// accessdoor.FileOpener forwarding pair (Open/Redeem) — minted by
// NewLiveResourceAccess only when raw itself implements FileOpener (see its
// doc). Embedding liveResourceAccess gives it the same Invoke/Create/Stat/
// List + liveness-fence behavior for free; Open/Redeem apply the IDENTICAL
// fence before forwarding to fo (never to a.raw directly — fo IS a.raw,
// captured pre-asserted so this type never re-asserts per call).
type liveResourceAccessFileOpener struct {
	liveResourceAccess
	fo accessdoor.FileOpener
}

func (a liveResourceAccessFileOpener) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, ErrAccessNotLive
	}
	return a.fo.Open(ctx, id, mode)
}

func (a liveResourceAccessFileOpener) Redeem(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	if !a.host.IsLive(a.inc) {
		return accessdoor.FileAccess{}, ErrAccessNotLive
	}
	return a.fo.Redeem(ctx, route)
}

var _ accessdoor.FileOpener = liveResourceAccessFileOpener{}
