package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// review_fix_test.go holds the regression tests for the actorbase review-fix
// batch (F1-F8, .dalek/pm/actorbase-review-fix-spec.md). Each is written to be
// RED against the pre-fix engine and GREEN after.

// F1: a panicking Proc must surface as a loud (non-nil) death through Dying(),
// not crash the process. (Pre-fix: proc(e) panics unrecovered → the whole test
// binary aborts.)
func TestEngine_ProcPanicSurfacesAsLoudDeath(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error { panic("boom in proc") }, nil
	}}
	if err := e.Start(ctx, &fakeActorContext{self: "actor:test"}); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	select {
	case err := <-e.Dying():
		if err == nil {
			t.Fatal("expected a loud (non-nil) death from a panicking Proc")
		}
		if !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("expected the panic surfaced in the death error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Dying() after a Proc panic")
	}
	_ = e.Stop(context.Background())
}

// F2: def.New returning an error must leave the engine Stop-able (Stop returns
// promptly). (Pre-fix: workerDone is still nil on the New-error path, so Stop's
// <-workerDone join blocks forever.)
func TestEngine_DefNewErrorLeavesEngineStoppable(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.def = Def{New: func() (Proc, error) {
		return nil, errors.New("construction failed")
	}}
	if err := e.Start(context.Background(), &fakeActorContext{self: "actor:test"}); err == nil {
		t.Fatal("expected Start to fail when def.New errors")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Stop(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung after a failed Start (nil workerDone join)")
	}
}

// F3: Call(Self()) must fail fast with ErrSelfCall and leave zero residue —
// no write, no out-station entry. (Pre-fix: no guard → the request is
// registered and written, and Wait would deadlock the single worker.)
func TestEngine_SelfCallFailsFastZeroResidue(t *testing.T) {
	pen := &fakePen{self: "actor:self"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = &fakeActorContext{self: "actor:self"}

	_, err := e.Call("actor:self", "greet", map[string]string{"x": "1"})
	if !errors.Is(err, ErrSelfCall) {
		t.Fatalf("expected ErrSelfCall, got %v", err)
	}
	if n := len(e.call.list()); n != 0 {
		t.Fatalf("expected zero out-station residue after a self-call, got %d", n)
	}
	if pen.count() != 0 {
		t.Fatalf("expected no write for a rejected self-call, got %d", pen.count())
	}
}

// registerEntry is the F4/F5 test setup helper: a bare in-flight out-station
// entry, no timer, no write yet.
func registerEntry(e *engine, id message.ID) {
	e.call.register(&message.Envelope{ID: id, Kind: message.KindRequest}, "actor:callee")
}

// F4a: once a final is matched (buffered), a late-firing author#2 timer
// (fireTimeout) must be a no-op — the entry stays, no self-close is written,
// and Wait still gets the real final. (Pre-fix: fireTimeout unconditionally
// deletes the entry and writes over the buffered final.)
func TestCallLedger_MatchThenFireTimeoutKeepsBufferedFinal(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	registerEntry(e, "req-f4a")

	if !e.call.match(responseEnv("req-f4a", message.StatusCompleted)) {
		t.Fatal("expected match to accept the final response")
	}
	writesBefore := pen.count()
	e.call.fireTimeout("req-f4a") // a timer that raced the landed final
	if pen.count() != writesBefore {
		t.Fatalf("fireTimeout wrote over a buffered final: %d extra writes", pen.count()-writesBefore)
	}
	env, ok, err := e.call.wait(context.Background(), "req-f4a", time.Second)
	if err != nil || !ok || env == nil {
		t.Fatalf("expected the buffered real final delivered, got ok=%v env=%v err=%v", ok, env, err)
	}
}

// F4b: Cancel() on a buffered-final entry must be a no-op (returns nil, writes
// nothing), and Wait still gets the real final. (Pre-fix: Cancel deletes the
// entry and writes a self-close, permanently losing the real final.)
func TestCallLedger_BufferedFinalCancelIsNoOp(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	registerEntry(e, "req-f4b")

	e.call.match(responseEnv("req-f4b", message.StatusCompleted))
	writesBefore := pen.count()
	if err := e.call.cancel("req-f4b"); err != nil {
		t.Fatalf("expected Cancel no-op to return nil, got %v", err)
	}
	if pen.count() != writesBefore {
		t.Fatalf("Cancel wrote a self-close over a buffered final: %d extra writes", pen.count()-writesBefore)
	}
	_, ok, err := e.call.wait(context.Background(), "req-f4b", time.Second)
	if err != nil || !ok {
		t.Fatalf("expected the buffered final still deliverable after Cancel, got ok=%v err=%v", ok, err)
	}
}

// F4c: a buffered-final entry must not appear in List() — it is no longer
// in-flight. (Pre-fix: list ranges every entry, including buffered-final ones.)
func TestCallLedger_BufferedFinalHiddenFromList(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	registerEntry(e, "req-f4c")

	e.call.match(responseEnv("req-f4c", message.StatusCompleted))
	for _, id := range e.call.list() {
		if id == "req-f4c" {
			t.Fatal("a buffered-final entry is still reported as in-flight by List()")
		}
	}
}

// F5: a rejected obligation write must surface an actorbase.closure_fault obs.
// (Pre-fix: fireTimeout swallows the write result with `_, _ =` and there is
// no fault sink.)
func TestCallLedger_ClosureFaultObservedOnWriteReject(t *testing.T) {
	pen := &fakePen{self: "actor:caller", reject: harness.HarnessRejectReason("policy_denied")}
	actx := &fakeActorContext{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = actx
	registerEntry(e, "req-f5")

	e.call.fireTimeout("req-f5")
	if got := actx.obsKindCounts()[actorrt.ObsKind("actorbase.closure_fault")]; got != 1 {
		t.Fatalf("expected exactly one closure_fault obs on a rejected write, got %d (kinds=%v)", got, actx.obsKindCounts())
	}
}

// F5 companion: a benign terminal-duplicate is the happy race and must stay
// silent (no closure_fault). Behavior.Respond maps a duplicate to a nil error,
// so this holds structurally.
func TestCallLedger_TerminalDuplicateIsSilent(t *testing.T) {
	pen := &fakePen{self: "actor:caller", reject: harness.HarnessTerminalDuplicate}
	actx := &fakeActorContext{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = actx
	registerEntry(e, "req-f5b")

	e.call.fireTimeout("req-f5b")
	if got := actx.obsKindCounts()[actorrt.ObsKind("actorbase.closure_fault")]; got != 0 {
		t.Fatalf("expected NO closure_fault on a benign terminal-duplicate, got %d", got)
	}
}

// F6: after the worker exits, a request racing into the pump (before Stop) must
// be rejected (overloaded), never admitted into a queue no one drains. The
// occupant is flipped to Draining BEFORE the death is announced, so reading
// Dying() is a deterministic barrier. (Pre-fix: occupant stays Running after
// the worker exits, so the pump admits the racing request.)
func TestEngine_WorkerExitRejectsRacingRequest(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error { return nil }, nil // exits at once
	}}
	if err := e.Start(ctx, &fakeActorContext{self: "actor:test"}); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	select {
	case <-e.Dying():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the worker to exit")
	}

	if err := e.Receive(context.Background(), newRequestEnv("req-race", -1)); err != nil {
		t.Fatalf("unexpected Receive error: %v", err)
	}
	if e.serve.len() != 0 {
		t.Fatalf("expected the racing request rejected, not admitted; serve len=%d", e.serve.len())
	}
	deadline := time.Now().Add(time.Second)
	for pen.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	last := pen.last()
	if last == nil {
		t.Fatal("expected an overloaded terminal for the rejected racing request")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
	}
	_ = json.Unmarshal(last.Payload, &payload)
	if payload.ErrorCode != "overloaded" {
		t.Fatalf("expected error_code=overloaded, got %+v", payload)
	}
	_ = e.Stop(context.Background())
}

// F7 (round-2 review overturned the drain): on stop the reject lane exits
// WITHOUT writing for whatever is still queued — by the time rejectStop
// closes, the incarnation's pen membrane is already fail-closed, so a drain
// would be theater that leaves truth untouched. Queued rejects fall to their
// callers' author#2 timers (spec §1.5: teardown 残留同兜底). The lane must
// still terminate promptly with a non-empty queue.
func TestEngine_RejectLaneStopExitsWithoutDrainWrites(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	// n=32: the random select may serve a few items before observing stop, but
	// serving ALL of them without the (removed) drain loop has probability
	// 2^-32 — the assertion below is effectively deterministic.
	const n = 32
	e := newTestEngine(t, pen, Hooks{}, 8, n)
	e.lifeCtx = context.Background()

	for i := 0; i < n; i++ {
		e.rejectQ <- newRequestEnv(message.ID(fmt.Sprintf("rej-%d", i)), -1)
	}
	close(e.rejectStop)
	done := make(chan struct{})
	go func() { e.runRejectLane(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reject lane did not terminate on stop with a backlog queued")
	}
	// No full-drain requirement: the random select may legitimately serve a
	// few queued items before observing stop, but must NOT loop until the
	// backlog is empty (the pre-round-2 drain would always write all n).
	if pen.count() == n && len(e.rejectQ) == 0 {
		t.Fatal("reject lane drained the full backlog on stop — the overturned F7 drain is back")
	}
}

// F4 (round-2 tightening): a wait that concedes to its deadline/ctx while
// match has ALREADY buffered the final must claim it instead of stranding it.
// claimBuffered is the under-lock re-check both branches use: flag and buffer
// are set in one lock scope by match, so observing final=true guarantees the
// envelope is claimable exactly once.
func TestCallLedger_ClaimBufferedWinsOverConcededWait(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	registerEntry(e, "req-claim")
	e.call.match(responseEnv("req-claim", message.StatusCompleted))

	env, ok := e.call.claimBuffered("req-claim")
	if !ok || env == nil {
		t.Fatal("expected claimBuffered to claim the buffered final")
	}
	if env2, ok2 := e.call.claimBuffered("req-claim"); ok2 {
		t.Fatalf("expected exactly-once claim, second claim got %v", env2)
	}
	// A cancelled ctx must still hand back an already-buffered final (the
	// "final landed just as I gave up" linearisation) through wait itself.
	registerEntry(e, "req-claim2")
	e.call.match(responseEnv("req-claim2", message.StatusCompleted))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, ok, err := e.call.wait(ctx, "req-claim2", 0)
	if err != nil || !ok || got == nil {
		t.Fatalf("expected buffered final to win over an already-done ctx, got ok=%v err=%v", ok, err)
	}
	// A genuine timeout (no final anywhere) leaves the entry a normal
	// in-flight row: listed, cancellable, awaitable later — never stranded.
	registerEntry(e, "req-claim3")
	if _, ok, _ := e.call.wait(context.Background(), "req-claim3", time.Millisecond); ok {
		t.Fatal("expected a genuine timeout with no final")
	}
	found := false
	for _, id := range e.call.list() {
		if id == "req-claim3" {
			found = true
		}
	}
	if !found {
		t.Fatal("a timed-out (non-final) entry must remain visible in List")
	}
}

// F8: a non-positive serve-ledger cap is a wiring bug and must panic at
// construction, not silently remap to 256. (Pre-fix: newServeLedger(_, 0)
// returns a cap-256 ledger.)
func TestServeLedger_NonPositiveCapPanics(t *testing.T) {
	life := func() context.Context { return context.Background() }
	for _, capacity := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected newServeLedger(cap=%d) to panic", capacity)
				}
			}()
			_ = newServeLedger(life, capacity)
		}()
	}
}
