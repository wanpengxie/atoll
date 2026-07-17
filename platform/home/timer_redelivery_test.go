package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type timerCrashResolver struct {
	attempts atomic.Int32
	acked    chan struct{}
	once     sync.Once
}

type inertTimerActor struct{}

func (inertTimerActor) Receive(context.Context, *message.Envelope) error { return nil }

func (r *timerCrashResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			for {
				msg, err := sys.Recv()
				if err != nil {
					return err
				}
				if msg.Type != "timer.crash-redeliver" {
					continue
				}
				if r.attempts.Add(1) == 1 {
					panic("crash before timer Ack")
				}
				r.once.Do(func() { close(r.acked) })
			}
		}, nil
	}}}, true
}

func TestFiredTimerSurvivesHandlerPanicAndRedeliversUntilAck(t *testing.T) {
	resolver := &timerCrashResolver{acked: make(chan struct{})}
	h, err := Open(Config{
		ChannelID: "timer-redelivery", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver, DaemonAuthority: allowTestDaemonAuthority{},
		ReconcileInterval: 10 * time.Millisecond, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	result, err := h.Declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:timer-crasher", Principal: "timer-crasher", Kind: actor.KindAgent,
		Class: "timer-crasher", Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(result.Row.ID)
		return live
	})
	handle := h.schedMinter.Mint(storespec.AuthorStamp{ID: result.Row.ID, BirthVersion: 1})
	timerID, err := handle.Schedule(context.Background(), schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: time.Now().Add(20 * time.Millisecond).UnixMilli(), Type: "timer.crash-redeliver",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-resolver.acked:
	case <-time.After(3 * time.Second):
		t.Fatalf("timer %s was not redelivered and Acked; attempts=%d", timerID, resolver.attempts.Load())
	}
	deadline := time.Now().Add(time.Second)
	for {
		page, err := h.cs.FiredTimers.ListFired(context.Background(), runtime.FiredTimerCursor{}, 16)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Rows) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Ack left fired rows: %+v", page.Rows)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if resolver.attempts.Load() < 2 {
		t.Fatalf("attempts=%d, want panic then redelivery", resolver.attempts.Load())
	}
}

func TestFiredTimerFullAttemptIsReleasedAndRetriedOnNextSweep(t *testing.T) {
	h, err := Open(Config{
		ChannelID: "timer-full-retry", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{},
		ReconcileInterval: time.Hour, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.reconcileStop()
	<-h.reconcileDone

	ctx := context.Background()
	result, err := h.Declare(ctx, DeclareRequest{
		SourceDeclID: "decl:timer-full", Principal: "timer-full", Kind: actor.KindAgent,
		Class: "timer-full", Placement: storespec.NewServerPlacement(),
		TIdle: int64(time.Hour / time.Millisecond), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, built, err := h.channel.Cells().SpawnIfAbsent(result.Row.ID, actor.KindAgent, func(actorrt.Incarnation) actorrt.Actor {
		return inertTimerActor{}
	}); err != nil || !built {
		t.Fatalf("spawn inert embodiment=(%v,%v)", built, err)
	}
	full := &testCarrier{err: actorrt.ErrMailboxFull}
	ticket, verdict := h.liveness.BeginEnsure(result.Row.ID, 1)
	if verdict != transitionApplied || h.liveness.PublishLocal(result.Row.ID, ticket, noInc, full) != transitionApplied {
		t.Fatalf("publish full carrier: ticket=%q verdict=%v", ticket, verdict)
	}
	handle := h.schedMinter.Mint(storespec.AuthorStamp{ID: result.Row.ID, BirthVersion: 1})
	timerID, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: time.Now().Add(10 * time.Millisecond).UnixMilli(), Type: "timer.full-retry",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		full.mu.Lock()
		attempted := len(full.envs) > 0
		full.mu.Unlock()
		page, readErr := h.cs.FiredTimers.ListFired(ctx, runtime.FiredTimerCursor{}, 16)
		return readErr == nil && attempted && len(page.Rows) == 1 && page.Rows[0].ID == timerID
	})
	full.mu.Lock()
	before := len(full.envs)
	full.mu.Unlock()
	h.sweepFired(ctx)
	full.mu.Lock()
	after := len(full.envs)
	full.mu.Unlock()
	if after != before+1 {
		t.Fatalf("next sweep attempts=%d, want %d; an in-memory suppression survived the full attempt", after, before+1)
	}

	if _, verdict := h.liveness.Retire(result.Row.ID, false); verdict != transitionApplied {
		t.Fatalf("retire full carrier=%v", verdict)
	}
	nextTicket, verdict := h.liveness.BeginEnsure(result.Row.ID, 1)
	next := &testCarrier{}
	if verdict != transitionApplied || h.liveness.PublishLocal(result.Row.ID, nextTicket, noInc, next) != transitionApplied {
		t.Fatalf("publish successor: ticket=%q verdict=%v", nextTicket, verdict)
	}
	h.sweepFired(ctx)
	next.mu.Lock()
	defer next.mu.Unlock()
	if len(next.envs) != 1 || next.envs[0].ID != message.ID("timer:"+string(timerID)) {
		t.Fatalf("successor fired deliveries=%+v", next.envs)
	}
}

// TestFiredSweepTransientTargetSetsDirtyOnceWithoutDoubleAccept is S8/DoD 4's
// "attached daemon fired 行: EnsureLive transient 时 dirty 仍已置、一拍内不
// 二次入队" case: a fired row whose target has NEVER been embodied (no
// SpawnIfAbsent, no carrier publish — the same "no local carrier yet" shape
// an attached daemon target has before its wire attaches) must, after
// exactly ONE sweepFired pass, (a) have its liveness wake-debt bit (dirty)
// set by AcceptFiredDelivery, and (b) still show exactly the one fired row,
// untouched (Accept never Acks) — proving the level-triggered loop attempted
// this row exactly once this pass rather than looping/duplicating it.
func TestFiredSweepTransientTargetSetsDirtyOnceWithoutDoubleAccept(t *testing.T) {
	h, err := Open(Config{
		ChannelID: "timer-transient-dirty", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{},
		ReconcileInterval: time.Hour, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.reconcileStop()
	<-h.reconcileDone

	ctx := context.Background()
	result, err := h.Declare(ctx, DeclareRequest{
		SourceDeclID: "decl:timer-transient", Principal: "timer-transient", Kind: actor.KindAgent,
		Class: "timer-transient", Placement: storespec.NewServerPlacement(),
		TIdle: int64(time.Hour / time.Millisecond), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := result.Row.ID

	// Declare alone admits a dormant liveness row (no embodiment, no carrier)
	// — the target stays transient throughout this test.
	if ws, ok := h.liveness.WakeStanding(id); !ok || ws.Dirty || ws.HasCarrier {
		t.Fatalf("wake standing before sweep = (%+v, %v), want dormant/no-carrier/dirty=false", ws, ok)
	}

	handle := h.schedMinter.Mint(storespec.AuthorStamp{ID: id, BirthVersion: 1})
	timerID, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: time.Now().Add(-time.Millisecond).UnixMilli(), Type: "timer.transient-dirty",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		page, readErr := h.cs.FiredTimers.ListFired(ctx, runtime.FiredTimerCursor{}, 16)
		return readErr == nil && len(page.Rows) == 1 && page.Rows[0].ID == timerID
	})

	// One reconcile-beat sweep, driven by hand (background reconcile is
	// stopped): sweepFired's own loop body must run AcceptFiredDelivery
	// exactly once for this row within this single call.
	h.sweepFired(ctx)

	ws, ok := h.liveness.WakeStanding(id)
	if !ok || !ws.Dirty {
		t.Fatalf("wake standing after one sweep = (%+v, %v), want dirty=true (fired row created wake debt)", ws, ok)
	}
	if ws.HasCarrier {
		t.Fatalf("wake standing after one sweep = %+v, want still no carrier (target never embodied)", ws)
	}

	// AcceptFiredDelivery never Acks — the row must still be there, exactly
	// once, not duplicated and not silently consumed by a buggy re-attempt
	// within the same pass.
	page, err := h.cs.FiredTimers.ListFired(ctx, runtime.FiredTimerCursor{}, 16)
	if err != nil {
		t.Fatalf("ListFired after sweep: %v", err)
	}
	if len(page.Rows) != 1 || page.Rows[0].ID != timerID {
		t.Fatalf("fired rows after one sweepFired pass = %+v, want exactly [%s] untouched", page.Rows, timerID)
	}
}
