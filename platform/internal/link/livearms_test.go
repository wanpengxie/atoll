package link_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// TestLiveArmsFencesConstructionTimeWrite is S7's birth-fence proof for the
// daemon (attached-compute) host: link.NewLiveArms welds ALL FOUR wire-flap
// arms to one incarnation, gated on the DAEMON's own rt.IsLive — mirroring
// TestLivePenFencesPostDeathWrite's construction-time case, generalised to the
// whole caps bundle. It proves G12 closed (§10.13 推导7①): a factory that
// calls any arm from INSIDE the Spawn build closure (before go-live) is
// refused on every arm, not just the pen.
func TestLiveArmsFencesConstructionTimeWrite(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	pen := &recordPen{}
	acc := &recordAccess{}
	st := &recordAccess{}
	sch := &recordSchedule{}
	rb := link.NewRebindableArms(link.CellArms{Pen: pen, Access: acc, State: st, Schedule: sch})

	var errs struct{ pen, access, state, schedule error }

	// The build closure runs inside Spawn, BEFORE go-live: IsLive(inc)==false,
	// so every arm a factory could reach during construction is fenced — the
	// "factory must not write" rule is structural on the port path too, not a
	// soft convention left for the daemon alone.
	inc := rt.Spawn("compute-w", actor.KindTool, func(i actorrt.Incarnation) actorrt.Actor {
		caps := link.NewLiveArms(rb, i, rt)
		_, errs.pen = caps.Pen.Write(context.Background(), &message.Envelope{ID: "during-ctor"})
		_, errs.access = caps.Access.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil)
		_, errs.state = caps.State.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil)
		_, errs.schedule = caps.Schedule.Schedule(context.Background(), schedule.ScheduleReq{})
		return noopLiveActor{}
	})

	if !errors.Is(errs.pen, link.ErrWriterNotLive) {
		t.Fatalf("construction-time pen write err = %v, want ErrWriterNotLive", errs.pen)
	}
	if !errors.Is(errs.access, link.ErrAccessNotLive) {
		t.Fatalf("construction-time access invoke err = %v, want ErrAccessNotLive", errs.access)
	}
	if !errors.Is(errs.state, link.ErrAccessNotLive) {
		t.Fatalf("construction-time state invoke err = %v, want ErrAccessNotLive", errs.state)
	}
	if !errors.Is(errs.schedule, link.ErrScheduleNotLive) {
		t.Fatalf("construction-time schedule err = %v, want ErrScheduleNotLive", errs.schedule)
	}
	if n := pen.count(); n != 0 {
		t.Fatalf("raw pen saw %d writes during construction, want 0 (fenced before go-live)", n)
	}
	if n := acc.count(); n != 0 {
		t.Fatalf("raw access saw %d invokes during construction, want 0 (fenced before go-live)", n)
	}
	if n := st.count(); n != 0 {
		t.Fatalf("raw state saw %d invokes during construction, want 0 (fenced before go-live)", n)
	}
	if n, _ := sch.counts(); n != 0 {
		t.Fatalf("raw schedule saw %d calls during construction, want 0 (fenced before go-live)", n)
	}

	// Now live: the same rebindable arms, gated by the same incarnation, pass
	// every call through to the raw arms.
	live := link.NewLiveArms(rb, inc, rt)
	if _, err := live.Pen.Write(context.Background(), &message.Envelope{ID: "while-live"}); err != nil {
		t.Fatalf("live pen write err = %v, want nil", err)
	}
	if _, err := live.Access.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil); err != nil {
		t.Fatalf("live access invoke err = %v, want nil", err)
	}
	if _, err := live.Schedule.Schedule(context.Background(), schedule.ScheduleReq{}); err != nil {
		t.Fatalf("live schedule err = %v, want nil", err)
	}
	if n := pen.count(); n != 1 {
		t.Fatalf("raw pen saw %d writes while live, want 1", n)
	}
	if n := acc.count(); n != 1 {
		t.Fatalf("raw access saw %d invokes while live, want 1", n)
	}
	if n, _ := sch.counts(); n != 1 {
		t.Fatalf("raw schedule saw %d calls while live, want 1", n)
	}

	// Despawn the incarnation: the welded bundle fences every arm again.
	rt.Despawn(inc)
	if _, err := live.Pen.Write(context.Background(), &message.Envelope{ID: "after-death"}); !errors.Is(err, link.ErrWriterNotLive) {
		t.Fatalf("post-death pen write err = %v, want ErrWriterNotLive", err)
	}
	if _, err := live.Access.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil); !errors.Is(err, link.ErrAccessNotLive) {
		t.Fatalf("post-death access invoke err = %v, want ErrAccessNotLive", err)
	}
	if n := pen.count(); n != 1 {
		t.Fatalf("raw pen saw %d writes total, want 1 (post-death write fenced before raw)", n)
	}
	if n := acc.count(); n != 1 {
		t.Fatalf("raw access saw %d invokes total, want 1 (post-death invoke fenced before raw)", n)
	}
}
