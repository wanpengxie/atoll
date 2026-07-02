package link

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/schedule"
)

// ErrScheduleNotLive is the WHEN-validity rejection on the time plane: a
// liveSchedule whose welded incarnation is no longer the live embodiment
// (despawned / dead / replaced) refuses to schedule or cancel. It is the
// time-axis twin of ErrWriterNotLive — a capability captured by a goroutine
// that outlived its incarnation cannot arm timers on its behalf.
var ErrScheduleNotLive = errors.New("link: schedule capability no longer the live incarnation")

// liveSchedule is the liveCap (WHEN-validity membrane) over a raw
// schedule.ScheduleHandle: a thin wrapper that, per call, first checks the host
// that the welded incarnation is STILL live (by POINTER, ABA-safe; lock-free)
// and only then forwards to the raw handle. It is the time-axis twin of livePen
// — the substrate (actorrt) owns liveness, the engine owns the author weld +
// due-set, and this wrapper composes the two with no change to either (bi-layer:
// schedule never imports actorrt-liveness through this seam; liveSchedule lives
// here in link, beside livePen, so the port path can construct it too).
//
// HONEST SCOPE: it fences "a leaked cap used long after death" and "ABA across
// an incarnation replacement". The sub-microsecond window between the IsLive
// check passing and the raw call committing is the accepted in-flight seam (a
// current incarnation's best-effort last gasp). Note the engine independently
// drops an incarnation-bound entry whose embodiment is gone at FIRE time
// (LivenessProbe.IsLive) — this membrane fences the SCHEDULE/CANCEL call itself,
// the two guards are complementary, not redundant. liveSchedule is a lease, not
// strict fencing.
type liveSchedule struct {
	raw  schedule.ScheduleHandle
	inc  actorrt.Incarnation
	host *actorrt.Runtime
}

// NewLiveSchedule wraps raw in the WHEN-validity membrane welded to inc, gated
// on host. Participant cells are born with this; substrate anchors
// (system/sysactor) deliberately use the raw handle — no incarnation gate — for
// the same reason livePen skips anchors (see NewLivePen).
func NewLiveSchedule(raw schedule.ScheduleHandle, inc actorrt.Incarnation, host *actorrt.Runtime) schedule.ScheduleHandle {
	return liveSchedule{raw: raw, inc: inc, host: host}
}

// Schedule implements schedule.ScheduleHandle: fence on the welded incarnation's
// liveness, then forward to the raw handle (which welds the author).
func (s liveSchedule) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	if !s.host.IsLive(s.inc) {
		return schedule.TimerID(""), ErrScheduleNotLive
	}
	return s.raw.Schedule(ctx, req)
}

// Cancel implements schedule.ScheduleHandle: fence, then forward.
func (s liveSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	if !s.host.IsLive(s.inc) {
		return ErrScheduleNotLive
	}
	return s.raw.Cancel(ctx, id)
}
