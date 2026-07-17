package link

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// RebindableArms is the daemon-side wire-flap membrane over one hosted actor's
// four capability arms (Pen, Access, State, Schedule): the cell's Caps are built
// from its facades ONCE, at first Spawn — never from the raw per-stream
// proxies directly — so a wire-session death and the later reconnect that
// replaces the proxies never touch the cell (§10.13 推导3: wire-session death ≠
// hosted-work death). Rebind swaps the underlying arm atomically (lock-free)
// each time a NEW stream opens for this actor — the initial OpenStream or a
// later reopen (reconnect, or a single stream dying while the link stays up,
// F6) — and the facades read the CURRENT pointer on every call.
//
// The disconnect window needs NO separate "dead arm": a torn-down link already
// closes every raw proxy (RemoteWriter.Close / relayClient.close, driven by
// streamReadLoop's deferred cleanup), and a closed proxy already returns the
// disconnected-window verdict each arm's contract promises — emit fails
// (errRemoteWriterClosed), access returns outcome_unknown (relayClient.close
// marks every pending/future round-trip transport-closed), schedule returns an
// error. Until Rebind runs again the facades simply keep pointing at that
// already-closed arm — fail-closed by construction (红线 12), never a stale
// arm that silently keeps answering as if still connected. Each arm publishes
// atomically. A mixed-generation observation fails closed and cannot report a
// false success. Rebind across the four capabilities is intentionally not a
// transaction: the four independent atomic Stores are a declared-benign
// single-point tolerance (a mid-Rebind read straddling old/new arms is safe
// precisely because every arm's stale side already fails closed — no
// transaction is required to prevent a false success).
type RebindableArms struct {
	pen       atomic.Pointer[harness.Pen]
	access    atomic.Pointer[accessdoor.ResourceAccessHandle]
	state     atomic.Pointer[accessdoor.AccessHandle]
	schedule  atomic.Pointer[schedule.ScheduleHandle]
	lifecycle atomic.Pointer[actorrt.LifecycleHandle]
}

// NewRebindableArms builds the membrane already bound to arms' four capability
// faces — every hosted actor is born with a live arm, so there is no arm-less
// construction state to guard against.
func NewRebindableArms(arms CellArms) *RebindableArms {
	r := &RebindableArms{}
	r.Rebind(arms)
	return r
}

// Rebind atomically swaps in arms' four capability faces. Call once per newly
// opened stream for this actor id (initial build, or a post-reconnect reopen).
// Down is deliberately NOT part of the membrane — it is re-registered directly
// on the caller's per-id down-handler map, which already keys it by actor id
// and needs no atomics of its own.
func (r *RebindableArms) Rebind(arms CellArms) {
	pen, access, state, sched, lifecycle := arms.Pen, arms.Access, arms.State, arms.Schedule, arms.Lifecycle
	r.pen.Store(&pen)
	r.access.Store(&access)
	r.state.Store(&state)
	r.schedule.Store(&sched)
	if lifecycle != nil {
		r.lifecycle.Store(&lifecycle)
	}
}

// Pen returns the stable facade to inject into the cell's Caps once at build
// time — it reads the CURRENT pen arm on every Write.
func (r *RebindableArms) Pen() harness.Pen { return rebindPen{r} }

// Access returns the stable channel-scoped (resource-face) facade
// (Caps.Access).
func (r *RebindableArms) Access() accessdoor.ResourceAccessHandle {
	return rebindResourceAccess{ptr: &r.access}
}

// State returns the stable actor-scoped facade (Caps.State) — Access and State
// are two faces of the same arm (§10.13 prior art), so they get independent
// atomic slots swapped together by one Rebind call. State stays the NARROW
// (Invoke-only) facade — the scope law itself (§3.1/§3.2).
func (r *RebindableArms) State() accessdoor.AccessHandle {
	return rebindAccess{ptr: &r.state}
}

// Schedule returns the stable facade to inject into the cell's Caps once at
// build time — it reads the CURRENT schedule arm on every call.
func (r *RebindableArms) Schedule() schedule.ScheduleHandle { return rebindSchedule{r} }

func (r *RebindableArms) Lifecycle() actorrt.LifecycleHandle { return rebindLifecycle{r: r} }

// rebindPen is the Caps-injected Pen facade: every Write reads the CURRENT arm
// off the membrane, so a reconnect never requires rebuilding the cell.
type rebindPen struct{ r *RebindableArms }

func (p rebindPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return (*p.r.pen.Load()).Write(ctx, env)
}

// rebindAccess is the Caps-injected AccessHandle facade — the actor-scoped
// State face's narrow (Invoke-only) facade.
type rebindAccess struct {
	ptr *atomic.Pointer[accessdoor.AccessHandle]
}

func (a rebindAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	return (*a.ptr.Load()).Invoke(ctx, op, id, args, grant)
}

// rebindResourceAccess is the Caps-injected ResourceAccessHandle facade —
// the channel-scoped Access face's WIDE facade (期11 spec §3.2's "wire 层
// 各拆两型": this is rebindAccess's resource-face twin, reading an
// independent atomic slot so a reconnect swaps both without touching cell
// code).
type rebindResourceAccess struct {
	ptr *atomic.Pointer[accessdoor.ResourceAccessHandle]
}

func (a rebindResourceAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	return (*a.ptr.Load()).Invoke(ctx, op, id, args, grant)
}

func (a rebindResourceAccess) Create(ctx context.Context, id resource.ResourceID, spec accessdoor.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	return (*a.ptr.Load()).Create(ctx, id, spec, initial)
}

func (a rebindResourceAccess) Stat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	return (*a.ptr.Load()).Stat(ctx, id)
}

func (a rebindResourceAccess) List(ctx context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return (*a.ptr.Load()).List(ctx, q)
}

// ErrResourceAccessNoFileOpener signals the CURRENTLY Rebind'd arm does not
// implement accessdoor.FileOpener (found+fixed during 期11 S6's platform-
// level walk verification, mirrors liveaccess.go's own fix note): day-1 this
// never actually fires — every rebindResourceAccess arm is a daemon-hosted
// remoteResourceHandle, which always implements FileOpener — but the facade
// re-reads the CURRENT pointer PER CALL (a reconnect could in principle swap
// in a different concrete arm), so Open/Redeem stay a real checked type
// assertion rather than an unchecked one.
var ErrResourceAccessNoFileOpener = errors.New("link: current resource access arm does not support file byte access")

// Open/Redeem implement accessdoor.FileOpener over the CURRENT arm — the
// missing half of this facade before this fix: rebindResourceAccess already
// satisfied ResourceAccessHandle's four pinned methods, but with no Open/
// Redeem, NewLiveResourceAccess's own FileOpener detection (liveaccess.go)
// could never see through this wrapper to the underlying
// *remoteResourceHandle, so a daemon-hosted actor's sys.Resource().Open/
// CreateFile always answered actorbase.ErrUnsupported despite §5 building
// the whole daemon-side mechanism these two calls need.
func (a rebindResourceAccess) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	fo, ok := (*a.ptr.Load()).(accessdoor.FileOpener)
	if !ok {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, ErrResourceAccessNoFileOpener
	}
	return fo.Open(ctx, id, mode)
}

func (a rebindResourceAccess) Redeem(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	fo, ok := (*a.ptr.Load()).(accessdoor.FileOpener)
	if !ok {
		return accessdoor.FileAccess{}, ErrResourceAccessNoFileOpener
	}
	return fo.Redeem(ctx, route)
}

// rebindSchedule is the Caps-injected ScheduleHandle facade.
type rebindSchedule struct{ r *RebindableArms }

type rebindLifecycle struct{ r *RebindableArms }

func (h rebindLifecycle) load() actorrt.LifecycleHandle {
	p := h.r.lifecycle.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (h rebindLifecycle) Fork(ctx context.Context, spec actorrt.ForkSpec) (child actor.ActorID, err error) {
	nonce := uuid.NewString()
	err = retryLifecycle(ctx, func(raw actorrt.LifecycleHandle) error {
		if arm, ok := raw.(interface {
			forkWithNonce(context.Context, actorrt.ForkSpec, string) (actor.ActorID, error)
		}); ok {
			child, err = arm.forkWithNonce(ctx, spec, nonce)
			return err
		}
		child, err = raw.Fork(ctx, spec)
		return err
	}, h.load)
	return child, err
}

func (h rebindLifecycle) DespawnChild(ctx context.Context, id actor.ActorID, reason string) error {
	return retryLifecycle(ctx, func(raw actorrt.LifecycleHandle) error {
		return raw.DespawnChild(ctx, id, reason)
	}, h.load)
}

func (h rebindLifecycle) EndSelf(ctx context.Context) error {
	return retryLifecycle(ctx, func(raw actorrt.LifecycleHandle) error { return raw.EndSelf(ctx) }, h.load)
}

func (s rebindSchedule) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	return (*s.r.schedule.Load()).Schedule(ctx, req)
}

func (s rebindSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	return (*s.r.schedule.Load()).Cancel(ctx, id)
}

func (s rebindSchedule) Ack(ctx context.Context, id schedule.TimerID) error {
	return (*s.r.schedule.Load()).Ack(ctx, id)
}

// Compile-time proof the facades satisfy the substrate capability contracts —
// a reconnect-surviving cell's caps are indistinguishable from a fixed one.
var (
	_ harness.Pen                     = rebindPen{}
	_ accessdoor.AccessHandle         = rebindAccess{}
	_ accessdoor.ResourceAccessHandle = rebindResourceAccess{}
	_ accessdoor.FileOpener           = rebindResourceAccess{}
	_ schedule.ScheduleHandle         = rebindSchedule{}
	_ actorrt.LifecycleHandle         = rebindLifecycle{}
)
