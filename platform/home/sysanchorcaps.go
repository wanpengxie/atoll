package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// The system anchor's Schedule and Spawn caps arms are LATE-BOUND (S6 Q5). The
// system cell is born inside channelkit.New (Home.Open step 6), but the schedule
// engine (h.schedMinter, step 10) and the fork/despawn dependencies (h.builder /
// buildChildCaps / hooks, step 9) do not exist yet. substrate-本质:
// the ring0 anchor's authority is welded once at channel genesis, before the
// time axis and the class→factory table are assembled — so the two arms that
// depend on them are resolved lazily through the same h-variable capture
// Hooks.Canceller already uses, not built eagerly at birth. Both arms are RAW
// (no incarnation membrane): the authority itself sets no incarnation gate on
// itself, same posture as the system pen. The Access/State arms are eager (the
// access door is assembled before channelkit) and so are built inline.
//
// The system cell never arms a timer or forks during assembly (it only answers
// actor.list/describe/operate), so the resolve() derefs run long after Open
// returns, when h is fully wired.

// systemScheduleHandle is the anchor's late-bound Schedule arm: it mints a fresh
// per-author ScheduleHandle from the (later-assembled) schedule minter on each
// call, welded to SystemActorID. Raw — no liveSchedule membrane.
type systemScheduleHandle struct{ home func() *Home }

func (s systemScheduleHandle) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	return s.home().schedMinter.Mint(actor.SystemActorID).Schedule(ctx, req)
}

func (s systemScheduleHandle) Cancel(ctx context.Context, id schedule.TimerID) error {
	return s.home().schedMinter.Mint(actor.SystemActorID).Cancel(ctx, id)
}

// systemSpawnHandle is the anchor's late-bound Spawn arm: it resolves the shared
// platform spawn handle (the SAME newSpawnHandle every admission builds) welded
// to the system cell's own incarnation, on each Fork/Despawn call. Fork children
// of the anchor are ordinary incarnation-level citizens (buildChildCaps).
type systemSpawnHandle struct {
	inc  actorrt.Incarnation
	home func() *Home
}

func (s systemSpawnHandle) resolve() actorrt.SpawnHandle {
	h := s.home()
	return newSpawnHandle(s.inc, h.channel.Cells(), h.builder, h.buildChildCaps, h.hooks())
}

func (s systemSpawnHandle) Fork(spec actorrt.ForkSpec) (actor.ActorID, error) {
	return s.resolve().Fork(spec)
}

func (s systemSpawnHandle) Despawn(childID actor.ActorID) error {
	return s.resolve().Despawn(childID)
}
