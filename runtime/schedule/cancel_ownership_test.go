package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

const (
	cancelOwner    actor.ActorID = "agent:timer-owner"
	cancelStranger actor.ActorID = "agent:timer-stranger"
)

// memTimerExists reports whether the memory home currently holds id — the
// whitebox read the non-owner Cancel test needs, since the handle contract is
// deliberately ack-less and will never tell a caller whether anything happened.
func memTimerExists(engine *Engine, id TimerID) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	_, ok := engine.mem[id]
	return ok
}

// TestCancelByNonOwnerIsSilentNoOpAndLeaksNoExistence pins the author
// equality check on BOTH homes, through the welded handle rather than the raw
// store, and pins the two halves that make it a security property rather than
// a bookkeeping detail:
//
//   - EFFECT: a stranger's Cancel changes nothing. The owner's timer still
//     rings, on time, with its own author — proven by letting it actually fire
//     after the foreign Cancel, not by reading a bool the API never returns.
//   - SILENCE: the stranger cannot tell a foreign timer that EXISTS from an id
//     that never existed at all. Both answers are byte-identical `nil`, so
//     Cancel is not a probe for other actors' timers. (The handle is ack-less
//     precisely so `existed` can never become such a probe; this test is what
//     keeps the store's WHERE-clause check from being "helpfully" surfaced.)
//
// The durable half runs against a REAL sqlite store, so the check under test is
// timerStore.CancelOwned's actual `AND author_id=?` predicate, not a fake's
// hand-written if.
func TestCancelByNonOwnerIsSilentNoOpAndLeaksNoExistence(t *testing.T) {
	ctx := context.Background()
	timers := openDurableTimerRows(t, "timer-cancel-ownership")

	const startMs int64 = 1_700_000_000_000
	fireAt := startMs + int64(time.Hour/time.Millisecond)
	clock := newFakeClock(time.UnixMilli(startMs))
	sink := &fakeFireSink{}

	minter, engine, err := New(Deps{Store: timers, Fire: sink, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	ownerHandle := minter.MintAuthority(testAuthority{id: cancelOwner})
	strangerHandle := minter.MintAuthority(testAuthority{id: cancelStranger})

	durableID, err := ownerHandle.Schedule(ctx, ScheduleReq{
		Home: TimerHomeDurable, FireAt: fireAt, Type: "owned.durable",
	})
	if err != nil {
		t.Fatalf("owner Schedule (durable): %v", err)
	}
	memoryID, err := ownerHandle.Schedule(ctx, ScheduleReq{
		Home: TimerHomeMemory, FireAt: fireAt, Type: "owned.memory",
	})
	if err != nil {
		t.Fatalf("owner Schedule (memory): %v", err)
	}

	// --- silence: a foreign id and a nonexistent id answer identically.
	foreignDurable := strangerHandle.Cancel(ctx, durableID)
	foreignMemory := strangerHandle.Cancel(ctx, memoryID)
	neverExisted := strangerHandle.Cancel(ctx, TimerID("no-such-timer-id"))
	if foreignDurable != nil || foreignMemory != nil || neverExisted != nil {
		t.Fatalf("non-owner Cancel answers = (durable %v, memory %v, absent %v), want three silent nils",
			foreignDurable, foreignMemory, neverExisted)
	}

	// --- effect: nothing was removed from either home.
	if !memTimerExists(engine, memoryID) {
		t.Fatal("a stranger's Cancel removed the owner's memory-home timer")
	}
	due, err := timers.Due(ctx, fireAt)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 1 || due[0].ID != durableID || due[0].AuthorID != cancelOwner {
		t.Fatalf("durable rows after a stranger's Cancel = %+v, want the owner's row intact", due)
	}

	// --- effect, end to end: both survivors still ring, still as the owner.
	advanceUntil(t, clock, time.Hour, func() bool { return sink.callCount() >= 2 })
	fired := map[actor.ActorID]int{}
	sink.mu.Lock()
	for _, call := range sink.calls {
		fired[call.author]++
	}
	sink.mu.Unlock()
	if fired[cancelOwner] != 2 || fired[cancelStranger] != 0 {
		t.Fatalf("fires by author = %v, want exactly 2 for the owner and 0 for the stranger", fired)
	}

	// --- the owner's own Cancel is the one that bites. A fresh pair, cancelled
	// by their author, must be gone from both homes.
	nextFireAt := clock.Now().UnixMilli() + int64(time.Hour/time.Millisecond)
	ownDurable, err := ownerHandle.Schedule(ctx, ScheduleReq{
		Home: TimerHomeDurable, FireAt: nextFireAt, Type: "owned.durable.2",
	})
	if err != nil {
		t.Fatalf("owner Schedule (durable, round 2): %v", err)
	}
	ownMemory, err := ownerHandle.Schedule(ctx, ScheduleReq{
		Home: TimerHomeMemory, FireAt: nextFireAt, Type: "owned.memory.2",
	})
	if err != nil {
		t.Fatalf("owner Schedule (memory, round 2): %v", err)
	}
	if err := ownerHandle.Cancel(ctx, ownDurable); err != nil {
		t.Fatalf("owner Cancel (durable): %v", err)
	}
	if err := ownerHandle.Cancel(ctx, ownMemory); err != nil {
		t.Fatalf("owner Cancel (memory): %v", err)
	}
	if memTimerExists(engine, ownMemory) {
		t.Fatal("owner Cancel left the memory-home timer in place")
	}
	if _, ok, err := timers.NextFireAt(ctx); err != nil || ok {
		t.Fatalf("NextFireAt after the owner cancelled its own row = (%v, %v), want (false, nil)", ok, err)
	}
}
