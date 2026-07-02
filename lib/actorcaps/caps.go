package actorcaps

import (
	"github.com/wanpengxie/ActOS/runtime/accessdoor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/schedule"
)

// Caps is the five-capability bundle welded to one actor incarnation at birth.
// Every field is a substrate-minted, caller-welded capability: the actor never
// self-reports which id/author it is — that coordinate is welded into each
// handle at construction (non-ambient), exactly as harness.Pen welds the writer
// identity. The platform assembly root builds all five in ONE step (发 handle 与
// live 膜 wrap 同一步, 红线) so an actor never receives a bare, ungated handle.
type Caps struct {
	// Pen is the plane-1 truth-write capability (append messages AS this actor).
	// Wrapped in the death-after-write membrane (livePen) at assembly.
	Pen harness.Pen
	// Access is the plane-2 off-log capability over the channel-scoped resource
	// tree (the access-plane dual of Pen). Wrapped in the liveAccess membrane.
	Access accessdoor.AccessHandle
	// State is the actor-scoped (collapsed) branch of the same plane-2 door —
	// this actor's own private durable state, minted via MintState(owner) and
	// wrapped in the same liveAccess membrane.
	State accessdoor.AccessHandle
	// Schedule is the time-axis capability (self-targeted timers). Wrapped in
	// the liveSchedule membrane.
	Schedule schedule.ScheduleHandle
	// Spawn is the fork/despawn capability over THIS incarnation's own children
	// (§10.5 句柄不出 — Fork returns only a child's name, never a handle).
	Spawn actorrt.SpawnHandle
}
