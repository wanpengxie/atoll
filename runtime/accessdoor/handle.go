package accessdoor

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// CreateSpec is a type alias re-exporting resourcespec.CreateSpec at the
// door's public face — the SAME type, not a copy (identical underlying type,
// wire-compatible, directly assignable both ways). archtest's
// TestResourcespecImportedOnlyWithinRuntime confines the resourcespec import
// itself to the runtime tree (only the kernel may implement a Driver /
// construct a raw Registry); downstream (lib/actorbase, platform/internal/
// link) needs CreateSpec's SHAPE to call Create/ResourceAccessHandle without
// ever importing resourcespec — this alias is that seam, the same pattern
// Outcome/StatMeta/etc. already establish (accessdoor is the door's whole
// public vocabulary; resourcespec's types ride out through it, never
// directly).
type CreateSpec = resourcespec.CreateSpec

// KindKV re-exports resourcespec.KindKV for the same reason CreateSpec is
// aliased above — the day-1 inline-value kind, the only one a domain Proc
// author's Create sugar (lib/actorbase's ResourceHandle.Create) drives.
const KindKV = resourcespec.KindKV

// KindFile re-exports resourcespec.KindFile — §5's CreateFile sugar
// (lib/actorbase's ResourceHandle.CreateFile) needs it to build a file-kind
// CreateSpec without importing resourcespec directly (same purity-wall
// reason as KindKV/CreateSpec above).
const KindFile = resourcespec.KindFile

// FormatFileAddress gives domain code the canonical name needed by the file
// verbs without exposing resourcespec, whose raw registry/driver vocabulary is
// deliberately confined to the runtime tree.
//
// Both segments are registry NAMES, not ids: a daemon:// address is the
// readable namespace, and ids stay the authority for bindings and routing
// (resourcespec.ParseFileAddress says the same from the other direction). The
// distinction is worth spelling out in the parameter names because the two are
// both plain strings, so passing an id compiles and then resolves to nothing.
func FormatFileAddress(deviceName, channelName, path string) (resource.ResourceID, error) {
	raw, err := resourcespec.FormatFileAddress(resourcespec.FileAddress{
		Scheme: resourcespec.DaemonScheme, Host: deviceName, Channel: channelName, Path: path,
	})
	return resource.ResourceID(raw), err
}

// AccessHandle is the STATE-FACE capability — the access-plane dual of
// harness.Pen, narrowed to the one verb the actor-scoped (collapsed) locus
// has any use for. It is welded to ONE caller/owner at construction and NEVER
// self-reports identity. It is an INTERFACE: the cell implementation
// (boundStateHandle) and the port implementation (remoteAccessHandle) are
// twins of one contract, both minted behind the same authority-welding seam.
type AccessHandle interface {
	Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error)
}

// ResourceAccessHandle is the RESOURCE-FACE capability (channel-scoped,
// Mint-minted) — 期11 spec §3.1's scope split: everything AccessHandle has,
// plus the three additional resource-locus verbs (Create/Stat/List) the
// actor-scoped locus structurally cannot answer (no kind routing, no R, no
// membership — the scope law itself, §3.1's "决策树保持一条,劈的是词表不是
// 执法"). Embedding AccessHandle rather than duplicating Invoke keeps the two
// faces' Invoke contract byte-for-byte identical and lets a ResourceAccessHandle
// value satisfy AccessHandle wherever only Invoke is needed (Caps.Access's
// wire path and the Invocation-arm dispatch both do exactly this).
//
// Caps.Access's DECLARED TYPE widens to this interface (§3.1: "Caps.Access
// 声明类型加宽为资源面接口，字段集合零变") — Caps.State stays AccessHandle,
// narrow, unchanged: the two fields are already two Mint faces (Access via
// Mint, State via MintState), only Access's Go type grows.
//
// THREE-AVATAR PARITY (红线, §3.2): boundHandle (here), the resource liveness
// membrane (link.liveResourceAccess), and the resource wire proxy
// (link.remoteResourceHandle) must all carry every method of this interface
// in lockstep — landing one without the other two is a half-wired vertical
// slice the red line forbids outright (no ErrUnsupported intermediate state).
type ResourceAccessHandle interface {
	AccessHandle
	FileOpener

	// Create is the SOLE create entry point (§3.1's "create 单入口"): the
	// resource face's Invoke no longer accepts a bare OpCreate at all (see
	// boundHandle.Invoke below) — CreateSpec never rides Invocation/Args
	// (§1's carrier red line), so create needed its own typed method the
	// moment CreateSpec existed.
	Create(ctx context.Context, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (Outcome, error)

	// Stat projects id's owner-root-or-any-grant-visible metadata + caller's effective ops
	// (§3.6). Never Operation-gated (Stat is a Query method, not a grantable
	// verb).
	Stat(ctx context.Context, id resource.ResourceID) (StatResult, error)

	// List enumerates channel-scoped resources this caller can see (owner root
	// or any-grant projection), paginated (§3.7). The actor-scoped locus has NO List — that
	// absence IS the scope law (no kind column, no cross-owner enumeration
	// makes sense there), so List belongs on this interface alone.
	List(ctx context.Context, q ListQuery) (ListPage, error)
}

// Open decides and redeems in one call, under this handle's welded caller. The
// decision half was always here; the redeeming half used to answer
// capability_unavailable, which meant an actor living in this process could be
// granted a file route and then have no way to turn it into bytes — even though
// this process is the one that opens every byte stream on this plane.
func (h boundHandle) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (FileAccess, Outcome, error) {
	if err := h.authorize(ctx); err != nil {
		return FileAccess{}, Outcome{RejectReason: access.OwnerInactive}, nil
	}
	// Open is the file byte verb, so it may be strict where invoke cannot: a
	// bare "a.pdf" is a legal opaque id for a kv resource, but as a file name
	// it has no base — the caller has a shell and moves its working directory.
	// Saying so beats letting it arrive as resource_not_found, which reads as
	// "that file is gone" and sends a model off rebuilding it.
	if raw := string(id); raw != "" && !isFileAddress(raw) && !looksAbsolute(raw) {
		return FileAccess{}, Outcome{}, &PathRelativeError{Path: raw}
	}
	out, err := h.door.invoke(ctx, h.caller, mode, id, nil)
	if err != nil || !out.Accepted() || out.Route == nil {
		return FileAccess{}, out, err
	}
	fa, err := h.redeem(ctx, *out.Route)
	return fa, out, err
}

// Redeem turns an ALREADY-decided route into bytes. It re-checks the welded
// caller rather than trusting the route: a route is a value, and a value can be
// carried to a handle that was never the one it was decided for.
func (h boundHandle) Redeem(ctx context.Context, route FileRoute) (FileAccess, error) {
	if err := h.authorize(ctx); err != nil {
		return FileAccess{}, ErrAuthorInactive
	}
	return h.redeem(ctx, route)
}

func (h boundHandle) redeem(ctx context.Context, route FileRoute) (FileAccess, error) {
	if h.door.deps.TransferRedeem == nil {
		return FileAccess{}, ErrFileCapabilityUnavailable
	}
	return h.door.deps.TransferRedeem.RedeemTransfer(ctx, h.caller, route)
}

// boundHandle is a ResourceAccessHandle welded to one caller (the cell
// implementation, channel-scoped). The caller is a struct field, not a wire
// field — structurally there is nowhere to self-report it.
type boundHandle struct {
	door      *door
	caller    actor.ActorID
	authority capauth.Authority
}

var ErrAuthorInactive = errors.New("accessdoor: author inactive or stale")

// authorize is the door's one complete verdict, run on every call. A handle
// without an authority is not a trusted handle — it is a broken one.
func (h boundHandle) authorize(ctx context.Context) error {
	if h.authority == nil {
		return ErrAuthorInactive
	}
	return h.authority.Admit()
}

// ErrCreateViaInvoke is the resource face's "create 单入口" enforcement
// (§3.1): a bare op=create reaching Invoke is a caller-protocol misuse — the
// SAME class of failure ErrMalformed already names (a structurally
// unacceptable shape rejected before resolve, never a verdict) — so it wraps
// ErrMalformed rather than minting a fourth error class. The actor-scoped
// (state) face is UNCHANGED and continues to accept op=create through
// Invoke (checkpoint's own birth verb, §3.2's "状态面 Invoke 路零改动") —
// this gate lives ONLY on boundHandle.Invoke, never in the shared ingress()
// cluster both faces run.
var ErrCreateViaInvoke = fmt.Errorf("%w: op=create must use the Create method, not Invoke (资源面 create 单入口)", ErrMalformed)

// Invoke runs ingress (structure → ErrMalformed), then the create单入口 gate,
// then the decision tree under the welded caller. The rejection layers stay
// distinct: a structural fault is a Go error before anything resolves; an
// authorization failure is a verdict.
func (h boundHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error) {
	if err := h.authorize(ctx); err != nil {
		return Outcome{RejectReason: access.OwnerInactive}, nil
	}
	if err := ingress(op, id, args); err != nil {
		return Outcome{}, err
	}
	if op == access.OpCreate {
		return Outcome{}, ErrCreateViaInvoke
	}
	return h.door.invoke(ctx, h.caller, op, id, args)
}

// Create runs the create-specific ingress (structure → ErrMalformed), then
// the create decision tree under the welded caller.
func (h boundHandle) Create(ctx context.Context, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (Outcome, error) {
	if err := h.authorize(ctx); err != nil {
		return Outcome{RejectReason: access.OwnerInactive}, nil
	}
	if err := ingressCreate(id, spec, initial); err != nil {
		return Outcome{}, err
	}
	return h.door.create(ctx, h.caller, id, spec, initial)
}

// Stat runs the read-face projection under the welded caller.
func (h boundHandle) Stat(ctx context.Context, id resource.ResourceID) (StatResult, error) {
	if err := h.authorize(ctx); err != nil {
		return StatResult{Reject: QueryNotFound}, nil
	}
	if err := checkResourceID(id); err != nil {
		return StatResult{}, err
	}
	return h.door.stat(ctx, h.caller, id)
}

// List runs the read-face pagination under the welded caller.
func (h boundHandle) List(ctx context.Context, q ListQuery) (ListPage, error) {
	if err := h.authorize(ctx); err != nil {
		return ListPage{}, ErrAuthorInactive
	}
	return h.door.list(ctx, h.caller, q)
}

// AccessMinter is the door's ONE outward face (mirroring harness.Minter's
// discipline: New hands out only a Minter, the bare door stays sealed). It has
// two mint faces, one per scope:
//   - MintAuthority welds a caller for the channel-scoped tree — the door is
//     already bound to its channel/Registry via Deps, and R authorization needs
//     no kind, so one parameter suffices. Its return type is the WIDE resource
//     face (ResourceAccessHandle, §3.1) — the channel-scoped locus is where
//     Create/Stat/List live;
//   - MintStateAuthority welds an owner for the actor-scoped (collapsed)
//     branch. Its return type stays the NARROW AccessHandle (Invoke only) — the
//     scope law itself: there is no kind/R/membership at this locus for
//     Create/Stat/List to mean anything, so the interface does not offer them
//     (§3.2's "不实现空方法" red line). The state ORGAN (memstate.go) is its
//     one caller: backing selection happens there, never at an injection point.
//
// The door mints against a LIVE authority and nothing else. The returned handle
// runs that authority's one complete verdict at the door on every call, which
// is what lets one shell serve a local body for its whole term and a remote
// ingress for one operation. There is no admitted-snapshot mint here: the door
// is the only place with a right to judge access, so it never accepts someone
// else's verdict as input.
type AccessMinter interface {
	MintAuthority(capauth.Authority) ResourceAccessHandle
	MintStateAuthority(capauth.Authority) AccessHandle
}

type minter struct{ door *door }

func (m *minter) MintAuthority(authority capauth.Authority) ResourceAccessHandle {
	if authority == nil || authority.ActorID() == "" {
		return rejectedResourceHandle{err: ErrAuthorInactive}
	}
	return boundHandle{
		door:      m.door,
		caller:    authority.ActorID(),
		authority: authority,
	}
}

func (m *minter) MintStateAuthority(authority capauth.Authority) AccessHandle {
	if authority == nil || authority.ActorID() == "" {
		return rejectedStateHandle{err: ErrAuthorInactive}
	}
	return boundStateHandle{
		door:      m.door,
		owner:     authority.ActorID(),
		authority: authority,
	}
}

type rejectedStateHandle struct{ err error }

func (h rejectedStateHandle) Invoke(
	context.Context,
	access.Operation,
	resource.ResourceID,
	[]byte,
) (Outcome, error) {
	return Outcome{}, h.err
}

type rejectedResourceHandle struct{ err error }

func (h rejectedResourceHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error) {
	return Outcome{}, h.err
}
func (h rejectedResourceHandle) Create(context.Context, resource.ResourceID, CreateSpec, []byte) (Outcome, error) {
	return Outcome{}, h.err
}
func (h rejectedResourceHandle) Stat(context.Context, resource.ResourceID) (StatResult, error) {
	return StatResult{}, h.err
}
func (h rejectedResourceHandle) List(context.Context, ListQuery) (ListPage, error) {
	return ListPage{}, h.err
}
func (h rejectedResourceHandle) Open(context.Context, resource.ResourceID, access.Operation) (FileAccess, Outcome, error) {
	return FileAccess{}, Outcome{}, h.err
}
func (h rejectedResourceHandle) Redeem(context.Context, FileRoute) (FileAccess, error) {
	return FileAccess{}, h.err
}

// New assembles the door from Deps and returns a Minter — never the bare door.
// It fail-fasts at assembly, not at first op: every Dep is required, and the
// day-1 KindKV driver must be present (op=create hardcodes KindKV, so a missing
// one would otherwise surface only when someone first creates).
func New(deps Deps) (AccessMinter, error) {
	return NewAssembly(deps)
}

func NewAssembly(deps Deps) (AccessMinter, error) {
	if deps.Registry == nil || deps.Drivers == nil || deps.Authority == nil || deps.State == nil {
		return nil, errors.New("accessdoor: Deps incomplete")
	}
	if deps.Drivers[resourcespec.KindKV] == nil {
		return nil, errors.New("accessdoor: KindKV driver missing")
	}
	d := &door{deps: deps}
	return &minter{door: d}, nil
}
