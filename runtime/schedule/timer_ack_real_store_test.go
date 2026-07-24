package schedule

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// ackFailOnceStore wraps a REAL sqlite-backed timerspec.TimerStore (from
// runtime/internal/store — the only tree allowed to touch the raw store,
// substrate 反旁路墙) and fails the FIRST AckOwned call with a transient
// error, then delegates every later call (including subsequent AckOwned
// calls) to the real store untouched. This is DoD 4's "销账失败语义" case
// exercised against real durable storage rather than an in-memory fake: the
// engine's own ack 口 (schedule.Handle.Ack → Store.AckOwned) must see a real
// store/链路错误 the first time and a real success the second time — exactly
// the shape a transient write hiccup has in production, with the fired row's
// durability coming from an actual sqlite file, not a map.
type ackFailOnceStore struct {
	timerspec.TimerStore
	mu     sync.Mutex
	failed bool
}

var errInjectedAckFailure = errors.New("schedule: injected ack store failure")

func (s *ackFailOnceStore) AckOwned(ctx context.Context, id timerspec.TimerID, author actor.ActorID) (bool, error) {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		s.mu.Unlock()
		return false, errInjectedAckFailure
	}
	s.mu.Unlock()
	return s.TimerStore.AckOwned(ctx, id, author)
}

// openRealTimerFixture opens a real per-test sqlite channel db and admits one
// real declared actor row into it (timerStore.Insert enforces a genuine
// actor_registry FK — store-not-validate trusts its caller the way the
// engine's Schedule path does, so a real store needs a real registered
// author, unlike the in-memory fakeStore which has no such constraint). It
// returns the raw timerspec.TimerStore — the same construction path
// runtime.OpenChannel uses internally (store.OpenChannel().Timers()),
// reachable here only because runtime/schedule lives under the runtime/ tree
// that runtime/internal/store's Go "internal" visibility permits — and the
// admitted author's ActorID.
func openRealTimerFixture(t *testing.T) (timerspec.TimerStore, actor.ActorID) {
	t.Helper()
	ctx := context.Background()
	const chID channel.ID = "timer-ack-real-store"
	cs, err := store.OpenChannel(ctx, chID, filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("store.OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	result, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: actor.KindAgent, SourceDeclID: "timer-ack-author", Class: "timer-ack-author",
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("AdmitDeclared: %v", err)
	}
	return cs.Timers(), result.ID
}

// TestAckOwnedRealStoreFailureLeavesFiredRowForNextAttempt is the real-sqlite
// counterpart to lib/actorbase's TestAutomaticTimerAckFailureIsObservedAndLeftRetryable
// (which only exercises an in-memory fake ScheduleHandle). DoD 4: "销账失败→
// fired 行保持不变→显式 Ack 重试收敛" — a genuine store error on the FIRST AckOwned
// must leave the fired row durably in place (provable against real sqlite,
// not an in-memory map), and a SECOND attempt (standing in for the next
// redeliver pass) must both succeed and actually delete the row.
func TestAckOwnedRealStoreFailureLeavesFiredRowForNextAttempt(t *testing.T) {
	real, author := openRealTimerFixture(t)
	wrapped := &ackFailOnceStore{TimerStore: real}
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	minter, engine, err := New(Deps{
		Store:       wrapped,
		Fire:        sink,
		DurableFire: NewTimerFirePen(wrapped, allowScheduleAuthority{}, channel.ID("timer-ack-real-store")),
		Clock:       clock,
		Authority:   allowScheduleAuthority{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint(testStamp(author))
	id, err := handle.Schedule(context.Background(), ScheduleReq{
		Home: TimerHomeDurable, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.ack-real-store",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		page, err := real.ListFired(context.Background(), timerspec.FiredCursor{}, 16)
		if err != nil {
			return false
		}
		for _, row := range page.Rows {
			if row.ID == id {
				return true
			}
		}
		return false
	})

	// First Ack: the injected store failure must surface, and the fired row
	// must still be durably present afterward (real sqlite read, not a fake's
	// in-memory bookkeeping).
	if err := handle.Ack(context.Background(), id); !errors.Is(err, errInjectedAckFailure) {
		t.Fatalf("first Ack err = %v, want errInjectedAckFailure", err)
	}
	page, err := real.ListFired(context.Background(), timerspec.FiredCursor{}, 16)
	if err != nil {
		t.Fatalf("ListFired after failed ack: %v", err)
	}
	found := false
	for _, row := range page.Rows {
		if row.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("fired row %s vanished despite a failed Ack — 销账失败必须保持 fired truth", id)
	}

	// Second explicit Ack retry must both
	// succeed and actually clear the fired row from real durable storage.
	if err := handle.Ack(context.Background(), id); err != nil {
		t.Fatalf("second (retry) Ack: %v", err)
	}
	page, err = real.ListFired(context.Background(), timerspec.FiredCursor{}, 16)
	if err != nil {
		t.Fatalf("ListFired after successful ack: %v", err)
	}
	for _, row := range page.Rows {
		if row.ID == id {
			t.Fatalf("fired row %s still present after a successful retry Ack", id)
		}
	}
}
