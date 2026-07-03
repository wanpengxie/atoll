package link

import (
	"context"
	"sync/atomic"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
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
// arm that silently keeps answering as if still connected.
type RebindableArms struct {
	pen      atomic.Pointer[harness.Pen]
	access   atomic.Pointer[accessdoor.AccessHandle]
	state    atomic.Pointer[accessdoor.AccessHandle]
	schedule atomic.Pointer[schedule.ScheduleHandle]
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
	pen, access, state, sched := arms.Pen, arms.Access, arms.State, arms.Schedule
	r.pen.Store(&pen)
	r.access.Store(&access)
	r.state.Store(&state)
	r.schedule.Store(&sched)
}

// Pen returns the stable facade to inject into the cell's Caps once at build
// time — it reads the CURRENT pen arm on every Write.
func (r *RebindableArms) Pen() harness.Pen { return rebindPen{r} }

// Access returns the stable channel-scoped facade (Caps.Access).
func (r *RebindableArms) Access() accessdoor.AccessHandle {
	return rebindAccess{ptr: &r.access}
}

// State returns the stable actor-scoped facade (Caps.State) — Access and State
// are two faces of the same arm (§10.13 prior art), so they get independent
// atomic slots swapped together by one Rebind call.
func (r *RebindableArms) State() accessdoor.AccessHandle {
	return rebindAccess{ptr: &r.state}
}

// Schedule returns the stable facade to inject into the cell's Caps once at
// build time — it reads the CURRENT schedule arm on every call.
func (r *RebindableArms) Schedule() schedule.ScheduleHandle { return rebindSchedule{r} }

// rebindPen is the Caps-injected Pen facade: every Write reads the CURRENT arm
// off the membrane, so a reconnect never requires rebuilding the cell.
type rebindPen struct{ r *RebindableArms }

func (p rebindPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return (*p.r.pen.Load()).Write(ctx, env)
}

// rebindAccess is the Caps-injected AccessHandle facade (backs both the
// channel-scoped Access face and the actor-scoped State face — ptr selects
// which of the membrane's two slots this facade reads).
type rebindAccess struct {
	ptr *atomic.Pointer[accessdoor.AccessHandle]
}

func (a rebindAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	return (*a.ptr.Load()).Invoke(ctx, op, id, args, grant)
}

// rebindSchedule is the Caps-injected ScheduleHandle facade.
type rebindSchedule struct{ r *RebindableArms }

func (s rebindSchedule) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	return (*s.r.schedule.Load()).Schedule(ctx, req)
}

func (s rebindSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	return (*s.r.schedule.Load()).Cancel(ctx, id)
}

// Compile-time proof the facades satisfy the substrate capability contracts —
// a reconnect-surviving cell's caps are indistinguishable from a fixed one.
var (
	_ harness.Pen             = rebindPen{}
	_ accessdoor.AccessHandle = rebindAccess{}
	_ schedule.ScheduleHandle = rebindSchedule{}
)
