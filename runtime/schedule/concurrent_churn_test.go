package schedule

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	churnWorkers    = 8
	churnIterations = 60
)

// churnFiredIDs snapshots how many times each message id reached the sink —
// the "exactly once, and it was the right one" evidence the churn test needs.
func churnFiredIDs(sink *fakeFireSink) map[message.ID]int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	out := make(map[message.ID]int, len(sink.calls))
	for _, call := range sink.calls {
		out[call.env.ID]++
	}
	return out
}

// TestConcurrentChurnNeverSwallowsTheEarliestKeeper is the -race soak over the
// engine's one genuinely concurrent seam: Schedule/Cancel run on arbitrary
// caller goroutines while the single run loop independently recomputes its due
// set and re-arms its alarm.
//
// The bug it hunts is not a data race (mu covers mem) but a LOST WAKE. `wake`
// is a capacity-1 coalescing channel: a pending token absorbs every further
// post, and the run loop's own "compute next, then arm the alarm" window is
// exactly where a token can be consumed by an iteration whose snapshot predates
// the timer that posted it. The design's answer is that the loop recomputes the
// FULL due set from scratch on every wake, so coalescing can only ever cost a
// redundant recompute — never a fire. Under a quiet, sequential test that
// answer is untestable; it only earns its keep under churn.
//
// Shape: two keepers (one per home, so BOTH nextFireAt branches are the minimum
// at some point) are scheduled with the earliest deadline in the system, in the
// middle of hundreds of concurrent schedule-then-cancel cycles across both
// homes and eight authors. Once the churn drains, the keepers must ring —
// exactly once each — with nothing else left behind in either home.
func TestConcurrentChurnNeverSwallowsTheEarliestKeeper(t *testing.T) {
	ctx := context.Background()
	rows := newFakeStore()
	sink := &fakeFireSink{}

	const startMs int64 = 1_700_000_000_000
	const keeperFireAt = startMs + 1
	churnFireAt := startMs + int64(time.Hour/time.Millisecond)
	clock := newFakeClock(time.UnixMilli(startMs))
	minter, engine := newTestEngine(t, rows, sink, clock)

	const (
		memKeeperAuthor     actor.ActorID = "agent:keeper-memory"
		durableKeeperAuthor actor.ActorID = "agent:keeper-durable"
	)

	start := make(chan struct{})
	failures := make(chan error, churnWorkers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < churnWorkers; worker++ {
		home := TimerHomeMemory
		if worker%2 == 1 {
			home = TimerHomeDurable
		}
		handle := minter.MintAuthority(testAuthority{id: actor.ActorID(fmt.Sprintf("agent:churn-%d", worker))})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < churnIterations; i++ {
				id, err := handle.Schedule(ctx, ScheduleReq{
					Home: home, FireAt: churnFireAt, Type: "churn.tick",
				})
				if err != nil {
					failures <- fmt.Errorf("churn Schedule (%s): %w", home, err)
					return
				}
				if err := handle.Cancel(ctx, id); err != nil {
					failures <- fmt.Errorf("churn Cancel (%s): %w", home, err)
					return
				}
			}
		}()
	}

	// The keepers are minted INTO the running churn, not before it: the window
	// under test is "an earlier deadline arrives while the loop is already
	// mid-iteration on a later one".
	close(start)
	memKeeperID, err := minter.MintAuthority(testAuthority{id: memKeeperAuthor}).Schedule(ctx, ScheduleReq{
		Home: TimerHomeMemory, FireAt: keeperFireAt, Type: "keeper.memory",
	})
	if err != nil {
		t.Fatalf("memory keeper Schedule: %v", err)
	}
	durableKeeperID, err := minter.MintAuthority(testAuthority{id: durableKeeperAuthor}).Schedule(ctx, ScheduleReq{
		Home: TimerHomeDurable, FireAt: keeperFireAt, Type: "keeper.durable",
	})
	if err != nil {
		t.Fatalf("durable keeper Schedule: %v", err)
	}

	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("churn worker: %v", err)
	}

	// One millisecond of fake time is enough to make BOTH keepers due and
	// nothing else (the churn deadlines sit an hour out). If a wake was
	// swallowed and no alarm is armed for the keepers, no amount of nudging
	// brings them back — which is precisely the failure this asserts.
	advanceUntil(t, clock, time.Millisecond, func() bool { return sink.callCount() >= 2 })

	if settled := waitStable(t, sink.callCount, 200*time.Millisecond); settled != 2 {
		t.Fatalf("fire count settled at %d, want exactly the 2 keepers", settled)
	}
	fired := churnFiredIDs(sink)
	if fired[fireMessageID(memKeeperID)] != 1 {
		t.Fatalf("memory keeper fired %d times, want 1 (fires=%v)", fired[fireMessageID(memKeeperID)], fired)
	}
	if fired[fireMessageID(durableKeeperID)] != 1 {
		t.Fatalf("durable keeper fired %d times, want 1 (fires=%v)", fired[fireMessageID(durableKeeperID)], fired)
	}

	// Every churn cycle cancelled what it scheduled, and both keepers completed:
	// neither home may be carrying anything at all.
	engine.mu.Lock()
	memLeft := len(engine.mem)
	engine.mu.Unlock()
	if memLeft != 0 {
		t.Fatalf("memory home holds %d timers after the churn drained, want 0", memLeft)
	}
	if left := rows.rowCount(); left != 0 {
		t.Fatalf("durable home holds %d rows after the churn drained, want 0", left)
	}
}
