package schedule

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// openDurableTimerRows opens a real per-test sqlite channel db and returns its
// raw timers face. Deliberately NO actor_registry seeding: the timers table
// carries no FK to it and Insert trusts the welded author (store-not-validate),
// so the fixture stays a pure durable-row substrate — the thing an Engine
// restart has to survive on.
//
// A real store, not fakeStore, is the whole point of the restart tests: "the
// row is still there after the process that scheduled it is gone" is a claim
// about sqlite, and an in-memory map that outlives the Engine only because the
// test holds a pointer to it would prove nothing.
func openDurableTimerRows(t *testing.T, chID channel.ID) timerspec.TimerStore {
	t.Helper()
	cs, err := store.OpenChannel(context.Background(), chID,
		filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("store.OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs.Timers()
}

const durableRestartAuthor actor.ActorID = "agent:durable-restart"

// TestDurableTimerSurvivesEngineRestartAndFiresLateExactlyOnce is the
// cross-restart half of the durable home's reason to exist. A memory-home timer
// legitimately dies with its Engine; a DURABLE one must not, and the row alone
// — not any Engine-local state — has to be enough to make it ring.
//
// The test closes the Engine that scheduled the timer, moves the clock hours
// PAST the deadline while nothing is running (the downtime window), then brings
// up a completely fresh Engine over the same store. Three things are then true
// or the durable home is broken:
//
//   - it rings at all (the row, not the process, owns the intent);
//   - it rings ONCE — a deadline missed by three hours is not three hours of
//     backlog to catch up on; one-shot means one fire no matter how late;
//   - it rings AS SCHEDULED — type / payload / correlation / audience are the
//     ones written down before the restart, with only TS reading the instant it
//     actually fired (the fire is honest about being late, it does not
//     back-date itself to the deadline it missed).
func TestDurableTimerSurvivesEngineRestartAndFiresLateExactlyOnce(t *testing.T) {
	ctx := context.Background()
	timers := openDurableTimerRows(t, "timer-durable-restart")

	const startMs int64 = 1_700_000_000_000
	const oneHourMs = int64(time.Hour / time.Millisecond)
	fireAt := startMs + oneHourMs
	clock := newFakeClock(time.UnixMilli(startMs))

	// ---- incarnation 1: schedule it, never reach the deadline, shut down.
	firstSink := &fakeFireSink{}
	firstMinter, firstEngine, err := New(Deps{Store: timers, Fire: firstSink, Clock: clock})
	if err != nil {
		t.Fatalf("New (incarnation 1): %v", err)
	}
	firstEngine.Start()

	id, err := firstMinter.MintAuthority(testAuthority{id: durableRestartAuthor}).Schedule(ctx, ScheduleReq{
		Home:          TimerHomeDurable,
		FireAt:        fireAt,
		Type:          "timer.restart",
		Payload:       []byte(`{"round":1}`),
		CorrelationID: "corr-across-restart",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	// The first run loop must have SEEN the row (it armed an alarm for the
	// future deadline) before we tear it down — otherwise "survived a restart"
	// would be indistinguishable from "the first engine never got to it".
	waitForArmedAtLeast(t, clock, 1)
	if got := firstSink.callCount(); got != 0 {
		t.Fatalf("timer fired %d times before its deadline", got)
	}
	firstEngine.Close()

	// The row is the only thing carrying the intent across the gap.
	next, ok, err := timers.NextFireAt(ctx)
	if err != nil || !ok || next != fireAt {
		t.Fatalf("NextFireAt after engine shutdown = (%d, %v, %v), want (%d, true, nil)", next, ok, err, fireAt)
	}

	// ---- the downtime window: the deadline passes with nothing running.
	clock.Advance(3 * time.Hour)

	// ---- incarnation 2: a brand-new Engine, same store.
	secondSink := &fakeFireSink{}
	_, secondEngine, err := New(Deps{Store: timers, Fire: secondSink, Clock: clock})
	if err != nil {
		t.Fatalf("New (incarnation 2): %v", err)
	}
	secondEngine.Start()
	defer secondEngine.Close()

	waitFor(t, 10*time.Second, func() bool { return secondSink.callCount() >= 1 })

	call := secondSink.lastCall()
	switch {
	case call.author != durableRestartAuthor:
		t.Fatalf("fire author = %q, want %q", call.author, durableRestartAuthor)
	case call.env.ID != fireMessageID(id):
		t.Fatalf("fire message id = %q, want %q", call.env.ID, fireMessageID(id))
	case call.env.Type != "timer.restart":
		t.Fatalf("fire type = %q, want the scheduled type", call.env.Type)
	case string(call.env.Payload) != `{"round":1}`:
		t.Fatalf("fire payload = %s, want the scheduled payload", call.env.Payload)
	case string(call.env.CorrelationID) != "corr-across-restart":
		t.Fatalf("fire correlation = %q, want the scheduled correlation", call.env.CorrelationID)
	case len(call.env.Audience) != 1 || call.env.Audience[0] != durableRestartAuthor:
		t.Fatalf("fire audience = %v, want the author alone", call.env.Audience)
	}
	// Late, and honest about it: TS is the instant it actually fired, three
	// hours after the deadline it slept through — not a back-dated FireAt.
	if want := clock.Now().UnixMilli(); call.env.TS != want {
		t.Fatalf("fire TS = %d, want the actual fire instant %d (deadline was %d)", call.env.TS, want, fireAt)
	}

	// No catch-up: the missed hours produce no backlog. Push the clock much
	// further and the count must stay at exactly one.
	clock.Advance(6 * time.Hour)
	if settled := waitStable(t, secondSink.callCount, 150*time.Millisecond); settled != 1 {
		t.Fatalf("late durable timer fired %d times, want exactly 1 (no catch-up)", settled)
	}
	if got := firstSink.callCount(); got != 0 {
		t.Fatalf("the closed engine's sink saw %d fires after Close", got)
	}

	// The row completed exactly once: it is in `fired` (one Ack clears it) and
	// nothing is left pending for a third incarnation to re-ring.
	if acked, err := timers.AckOwned(ctx, id, durableRestartAuthor); err != nil || !acked {
		t.Fatalf("AckOwned after the late fire = (%v, %v), want (true, nil)", acked, err)
	}
	if acked, err := timers.AckOwned(ctx, id, durableRestartAuthor); err != nil || acked {
		t.Fatalf("second AckOwned = (%v, %v), want (false, nil)", acked, err)
	}
	if _, ok, err := timers.NextFireAt(ctx); err != nil || ok {
		t.Fatalf("NextFireAt after completion = (%v, %v), want (false, nil)", ok, err)
	}
}
