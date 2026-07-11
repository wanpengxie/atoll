package link_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// recordAccess is a minimal raw accessdoor.ResourceAccessHandle: it counts
// the invokes that reach it and always accepts. Wrapping it in a liveAccess/
// liveResourceAccess lets a test assert which invokes the incarnation gate
// let THROUGH versus fenced before it. It satisfies the WIDE resource face so
// the same double backs both the Access (resource-face) and State
// (narrow-face) slots — a wider double is harmless where only Invoke is ever
// called.
type recordAccess struct {
	mu sync.Mutex
	n  int
}

func (a *recordAccess) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
	a.mu.Lock()
	a.n++
	a.mu.Unlock()
	return accessdoor.Outcome{}, nil
}

func (a *recordAccess) Create(context.Context, resource.ResourceID, resourcespec.CreateSpec, []byte) (accessdoor.Outcome, error) {
	a.mu.Lock()
	a.n++
	a.mu.Unlock()
	return accessdoor.Outcome{}, nil
}

func (a *recordAccess) Stat(context.Context, resource.ResourceID) (accessdoor.StatResult, error) {
	a.mu.Lock()
	a.n++
	a.mu.Unlock()
	return accessdoor.StatResult{}, nil
}

func (a *recordAccess) List(context.Context, accessdoor.ListQuery) (accessdoor.ListPage, error) {
	a.mu.Lock()
	a.n++
	a.mu.Unlock()
	return accessdoor.ListPage{}, nil
}

func (a *recordAccess) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

// TestLiveAccessFencesPostDeathInvoke closes the WHEN-validity gap on the access
// plane: a liveAccess welded to an incarnation is fenced pre-go-live, passes
// while live, and fences with ErrAccessNotLive after despawn — the plane-2 twin
// of the livePen death-after-write test.
func TestLiveAccessFencesPostDeathInvoke(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	raw := &recordAccess{}
	var h accessdoor.AccessHandle
	var ctorErr error

	inc, _, _ := rt.SpawnIfAbsent("w", actor.KindTool, func(i actorrt.Incarnation) actorrt.Actor {
		h = link.NewLiveAccess(raw, i, rt)
		_, ctorErr = h.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil)
		return noopLiveActor{}
	})

	if !errors.Is(ctorErr, link.ErrAccessNotLive) {
		t.Fatalf("construction-time invoke err = %v, want ErrAccessNotLive (pre-go-live)", ctorErr)
	}
	if raw.count() != 0 {
		t.Fatalf("raw handle saw %d invokes during construction, want 0 (fenced before go-live)", raw.count())
	}

	if _, err := h.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil); err != nil {
		t.Fatalf("live invoke err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw handle saw %d invokes while live, want 1", raw.count())
	}

	rt.Despawn(inc)
	_, err := h.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil)
	if !errors.Is(err, link.ErrAccessNotLive) {
		t.Fatalf("post-death invoke err = %v, want ErrAccessNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw handle saw %d invokes total, want 1 (post-death invoke fenced before raw)", raw.count())
	}
}

// recordSchedule is a minimal raw schedule.ScheduleHandle counting Schedule and
// Cancel calls, always accepting.
type recordSchedule struct {
	mu       sync.Mutex
	schedule int
	cancel   int
}

func (s *recordSchedule) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	s.mu.Lock()
	s.schedule++
	s.mu.Unlock()
	return schedule.TimerID("t"), nil
}

func (s *recordSchedule) Cancel(context.Context, schedule.TimerID) error {
	s.mu.Lock()
	s.cancel++
	s.mu.Unlock()
	return nil
}

func (s *recordSchedule) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schedule, s.cancel
}

// TestLiveScheduleFencesPostDeath closes the WHEN-validity gap on the time plane: a
// liveSchedule welded to an incarnation is fenced pre-go-live, passes while
// live, and fences BOTH Schedule and Cancel with ErrScheduleNotLive after
// despawn — the time-axis twin of the livePen death-after-write test.
func TestLiveScheduleFencesPostDeath(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	raw := &recordSchedule{}
	var h schedule.ScheduleHandle
	var ctorErr error

	inc, _, _ := rt.SpawnIfAbsent("w", actor.KindTool, func(i actorrt.Incarnation) actorrt.Actor {
		h = link.NewLiveSchedule(raw, i, rt)
		_, ctorErr = h.Schedule(context.Background(), schedule.ScheduleReq{})
		return noopLiveActor{}
	})

	if !errors.Is(ctorErr, link.ErrScheduleNotLive) {
		t.Fatalf("construction-time schedule err = %v, want ErrScheduleNotLive (pre-go-live)", ctorErr)
	}
	if n, _ := raw.counts(); n != 0 {
		t.Fatalf("raw handle saw %d schedules during construction, want 0 (fenced before go-live)", n)
	}

	if _, err := h.Schedule(context.Background(), schedule.ScheduleReq{}); err != nil {
		t.Fatalf("live schedule err = %v, want nil", err)
	}
	if n, _ := raw.counts(); n != 1 {
		t.Fatalf("raw handle saw %d schedules while live, want 1", n)
	}

	rt.Despawn(inc)
	if _, err := h.Schedule(context.Background(), schedule.ScheduleReq{}); !errors.Is(err, link.ErrScheduleNotLive) {
		t.Fatalf("post-death schedule err = %v, want ErrScheduleNotLive", err)
	}
	if err := h.Cancel(context.Background(), schedule.TimerID("t")); !errors.Is(err, link.ErrScheduleNotLive) {
		t.Fatalf("post-death cancel err = %v, want ErrScheduleNotLive", err)
	}
	if n, c := raw.counts(); n != 1 || c != 0 {
		t.Fatalf("raw handle saw (schedule=%d cancel=%d) total, want (1, 0) (post-death fenced before raw)", n, c)
	}
}

// TestLiveAccessFencesPostDeath_PortPath is the plane-2 twin of the livePen
// port-path fence: a liveAccess welded to an out-of-process port's Incarnation
// invokes while that port is the live embodiment, then — once a same-id reattach
// REPLACES the port (predecessor stopped) — fences with ErrAccessNotLive. The gate
// is the shared pointer-identity IsLive check, here exercised over a real port
// incarnation rather than a local cell.
func TestLiveAccessFencesPostDeath_PortPath(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	const id = actor.ActorID("remote-access")
	inc1, remote1 := attachTestPort(t, rt, id)
	defer remote1.Close()

	raw := &recordAccess{}
	h := link.NewLiveAccess(raw, inc1, rt)

	if _, err := h.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil); err != nil {
		t.Fatalf("live invoke err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw handle saw %d invokes while live, want 1", raw.count())
	}

	// A second attach for the SAME id replaces the port: Attach stops inc1 before
	// returning, so the welded liveAccess is no longer the live incarnation.
	_, remote2 := attachTestPort(t, rt, id)
	defer remote2.Close()

	_, err := h.Invoke(context.Background(), access.Operation(""), resource.ResourceID(""), nil, nil)
	if !errors.Is(err, link.ErrAccessNotLive) {
		t.Fatalf("post-replace invoke err = %v, want ErrAccessNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw handle saw %d invokes total, want 1 (post-replace invoke fenced before raw)", raw.count())
	}
}

// TestLiveScheduleFencesPostDeath_PortPath is the time-axis twin of the livePen
// port-path fence: a liveSchedule welded to a port's Incarnation passes while the
// port is live, then fences BOTH Schedule and Cancel with ErrScheduleNotLive once
// a same-id reattach replaces the port.
func TestLiveScheduleFencesPostDeath_PortPath(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	const id = actor.ActorID("remote-schedule")
	inc1, remote1 := attachTestPort(t, rt, id)
	defer remote1.Close()

	raw := &recordSchedule{}
	h := link.NewLiveSchedule(raw, inc1, rt)

	if _, err := h.Schedule(context.Background(), schedule.ScheduleReq{}); err != nil {
		t.Fatalf("live schedule err = %v, want nil", err)
	}
	if n, _ := raw.counts(); n != 1 {
		t.Fatalf("raw handle saw %d schedules while live, want 1", n)
	}

	_, remote2 := attachTestPort(t, rt, id)
	defer remote2.Close()

	if _, err := h.Schedule(context.Background(), schedule.ScheduleReq{}); !errors.Is(err, link.ErrScheduleNotLive) {
		t.Fatalf("post-replace schedule err = %v, want ErrScheduleNotLive", err)
	}
	if err := h.Cancel(context.Background(), schedule.TimerID("t")); !errors.Is(err, link.ErrScheduleNotLive) {
		t.Fatalf("post-replace cancel err = %v, want ErrScheduleNotLive", err)
	}
	if n, c := raw.counts(); n != 1 || c != 0 {
		t.Fatalf("raw handle saw (schedule=%d cancel=%d) total, want (1, 0) (post-replace fenced before raw)", n, c)
	}
}
