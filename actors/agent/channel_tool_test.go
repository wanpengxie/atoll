package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

type metaFakeIPC struct {
	writes chan message.Envelope
}

func newMetaFakeIPC() *metaFakeIPC {
	return &metaFakeIPC{writes: make(chan message.Envelope, 1)}
}

func (f *metaFakeIPC) ChannelID() channel.ID           { return "ch-test" }
func (f *metaFakeIPC) WorkerID() string                { return "worker-test" }
func (f *metaFakeIPC) WorkerActorID() actor.ActorID    { return "agent:worker-test" }
func (f *metaFakeIPC) Triggers() <-chan TriggerPayload  { return nil }
func (f *metaFakeIPC) WriteEnvelope(_ context.Context, env message.Envelope) error {
	f.writes <- env
	return nil
}

// TestRouteTriggersDeliversFinalToActiveAwait asserts a final response whose
// parent matches an in-flight Submit is consumed by the caller's correlator
// (deliveredToWaiter) and NOT forwarded to the LLM trigger stream.
func TestRouteTriggersDeliversFinalToActiveAwait(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("tool-req-1", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Park a REAL Await on tool-req-1 so the inbound final has an active
	// awaiter — only then is the disposition DeliveredToAwait and the final
	// must be consumed (not forwarded). (Merely Register without Await leaves
	// no active awaiter → NoActiveWaiter + final → surfaced as a trigger; that
	// super-window path is covered by TestRouteTriggersSuperWindowFinalBecomesTrigger.)
	awaitDone := make(chan *message.Envelope, 1)
	go func() {
		env, _, _ := caller.Await(context.Background(), "tool-req-1", 2*time.Second)
		awaitDone <- env
	}()
	// let the Await goroutine park
	time.Sleep(30 * time.Millisecond)

	in := make(chan TriggerPayload, 2)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "response-1", "tool-req-1", "completed")
	in <- toolTrigger(message.KindRequest, "normal-trigger", "")
	close(in)

	// The parked Await consumed the final.
	select {
	case env := <-awaitDone:
		if env == nil || env.ParentID != "tool-req-1" {
			t.Fatalf("active Await did not receive the final: %v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("active Await never resolved on final")
	}

	got, ok := <-out
	if !ok {
		t.Fatal("routeTriggers closed before normal trigger")
	}
	if got.Envelope.ID != "normal-trigger" {
		t.Fatalf("routed id=%s want normal-trigger (final leaked into stream)", got.Envelope.ID)
	}
	if _, ok := <-out; ok {
		t.Fatal("more than the normal trigger leaked into stream")
	}
}

// TestRouteTriggersSuperWindowFinalBecomesTrigger is the F1 core regression:
// a future registered by a fast-path Submit whose Await ALREADY TIMED OUT
// (super-window degrade-to-ack) leaves the waiterSet registered. When the long
// call's final finally arrives, Deliver finds no active awaiter → buffers →
// NoActiveWaiter. The old code keyed on Registered()==true and SWALLOWED the
// final forever (the long-call result never came back). The fix drives purely
// off Disposition: NoActiveWaiter + final → surface as a new turn trigger, and
// clear the future so a later await_result cannot double-consume it.
func TestRouteTriggersSuperWindowFinalBecomesTrigger(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	// Simulate a fast-path Submit whose Await timed out: register, then run an
	// Await with a tiny window that expires before any final.
	caller.Futures.Register("tool-req-superwin", true)
	_, ok, err := caller.Await(context.Background(), "tool-req-superwin", 10*time.Millisecond)
	if err != nil || ok {
		t.Fatalf("setup: expected super-window timeout (ok=false,err=nil), got ok=%v err=%v", ok, err)
	}
	// The future is STILL registered after the timed-out Await.
	if !caller.Futures.Registered("tool-req-superwin") {
		t.Fatal("setup: future should still be registered after a timed-out fast-path Await")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 1)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "superwin-final", "tool-req-superwin", "completed")
	close(in)

	got, ok := <-out
	if !ok {
		t.Fatal("F1: super-window final was swallowed instead of surfacing as a trigger")
	}
	if got.Envelope.ID != "superwin-final" {
		t.Fatalf("F1: forwarded id=%s want superwin-final", got.Envelope.ID)
	}
	// The future was cleared so a later await_result cannot re-consume it.
	if caller.Futures.Registered("tool-req-superwin") {
		t.Fatal("F1: future should be cleared after surfacing the final as a trigger")
	}
}

// TestRouteTriggersFastFinalBeforeAwaitBuffered covers the F1 fast path race:
// Submit registered an expected Await, but the final loops back before Await has
// parked. That final must be buffered for Await and must not become a new turn
// trigger or be abandoned.
func TestRouteTriggersFastFinalBeforeAwaitBuffered(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("tool-req-fast-before-await", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 2)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "fast-final", "tool-req-fast-before-await", "completed")
	in <- toolTrigger(message.KindRequest, "normal-trigger", "")
	close(in)

	got, ok := <-out
	if !ok || got.Envelope.ID != "normal-trigger" {
		t.Fatalf("fast final leaked or normal trigger missing: got=%v ok=%v", got.Envelope.ID, ok)
	}
	if _, ok := <-out; ok {
		t.Fatal("fast-final-before-await leaked into trigger stream")
	}

	final, okAwait, err := caller.Await(context.Background(), "tool-req-fast-before-await", time.Second)
	if err != nil || !okAwait || final == nil {
		t.Fatalf("buffered fast final was not consumable by Await: ok=%v err=%v env=%v", okAwait, err, final)
	}
	if final.ID != "fast-final" {
		t.Fatalf("await final id=%s want fast-final", final.ID)
	}
}

// TestRouteTriggersFinalNeverAwaitAndTriggerDoubleConsumed exercises the
// production routeTriggers path under the Deliver vs await_result race. A final
// is allowed to either resolve a racing Await or surface as a trigger, but never
// both.
func TestRouteTriggersFinalNeverAwaitAndTriggerDoubleConsumed(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		b := &Bridge{}
		caller := b.caller()
		id := message.ID("tool-req-race-" + time.Now().Format("150405.000000000"))
		caller.Futures.Register(id, true)
		if env, ok, err := caller.Await(context.Background(), id, time.Nanosecond); err != nil || ok || env != nil {
			t.Fatalf("trial %d setup: expected timed-out fast path, got env=%v ok=%v err=%v", trial, env, ok, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan TriggerPayload, 1)
		out := b.routeTriggers(ctx, nil, in)
		start := make(chan struct{})
		awaited := make(chan bool, 1)

		go func() {
			<-start
			env, ok, err := caller.Await(context.Background(), id, 2*time.Millisecond)
			awaited <- err == nil && ok && env != nil
		}()
		go func() {
			<-start
			in <- toolTriggerWithStatus(message.KindResponse, "race-final", string(id), "completed")
			close(in)
		}()
		close(start)

		gotTrigger := false
		for payload := range out {
			if payload.Envelope.ID == "race-final" {
				gotTrigger = true
			}
		}
		gotAwait := <-awaited
		cancel()

		if gotTrigger && gotAwait {
			t.Fatalf("trial %d: final was both awaited and surfaced as trigger", trial)
		}
		if !gotTrigger && !gotAwait {
			t.Fatalf("trial %d: final was neither awaited nor surfaced", trial)
		}
	}
}

// TestRouteTriggersNoActiveWaiterFinalBecomesTrigger is the M4 case: a final
// with no local waiter (worker restart / abandoned / await timed out) is
// forwarded to the LLM as a new turn trigger, never quarantined.
func TestRouteTriggersNoActiveWaiterFinalBecomesTrigger(t *testing.T) {
	b := &Bridge{}
	// No Register for tool-req-missing → simulates a worker restart with an
	// empty registry.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 1)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "orphan-final", "tool-req-missing", "completed")
	close(in)

	got, ok := <-out
	if !ok {
		t.Fatal("M4: no-waiter final was dropped instead of becoming a trigger")
	}
	if got.Envelope.ID != "orphan-final" {
		t.Fatalf("M4: forwarded id=%s want orphan-final", got.Envelope.ID)
	}
}

// TestRouteTriggersProvisionalSwallowedFuturePending asserts a provisional
// response on an in-flight request is SWALLOWED (v1: ignore provisionals, wait
// for the final) — not forwarded to the LLM — and the future stays in flight.
func TestRouteTriggersProvisionalSwallowedFuturePending(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("tool-req-prov", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 2)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "prov-1", "tool-req-prov", "processing")
	in <- toolTrigger(message.KindRequest, "normal-trigger", "")
	close(in)

	// Provisional is swallowed; only the normal trigger comes through.
	got, ok := <-out
	if !ok || got.Envelope.ID != "normal-trigger" {
		t.Fatalf("provisional leaked: got=%v ok=%v", got.Envelope.ID, ok)
	}
	if _, ok := <-out; ok {
		t.Fatal("more than the normal trigger leaked")
	}
	// The future MUST remain in flight (a provisional never resolves it).
	if !caller.Futures.Registered("tool-req-prov") {
		t.Fatal("provisional erased the in-flight future")
	}
}

// TestRouteTriggersOrphanProvisionalSwallowed asserts a provisional with no
// registration at all (orphan progress) is swallowed, not forwarded.
func TestRouteTriggersOrphanProvisionalSwallowed(t *testing.T) {
	b := &Bridge{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 2)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "orphan-prov", "tool-req-none", "processing")
	in <- toolTrigger(message.KindRequest, "normal-trigger", "")
	close(in)

	got, ok := <-out
	if !ok || got.Envelope.ID != "normal-trigger" {
		t.Fatalf("orphan provisional leaked: got=%v ok=%v", got.Envelope.ID, ok)
	}
	if _, ok := <-out; ok {
		t.Fatal("more than the normal trigger leaked")
	}
}

// TestCallerSubmitAwaitFastPathInline drives Submit + a final delivered via
// the trigger loop and asserts Await returns the final inline.
func TestCallerSubmitAwaitFastPathInline(t *testing.T) {
	b := &Bridge{}
	ipc := newMetaFakeIPC()
	caller := b.caller()

	env := message.Envelope{ID: "req-fast", ChannelID: "ch-test", Kind: message.KindRequest}
	res, err := caller.Submit(context.Background(), ipc, env, 30000, true)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.RequestID != "req-fast" || !res.Ack.Accepted {
		t.Fatalf("submit result=%+v", res)
	}

	// Drain the write and deliver a final.
	<-ipc.writes
	final := toolTriggerWithStatus(message.KindResponse, "resp-fast", "req-fast", "completed").Envelope
	go func() {
		time.Sleep(5 * time.Millisecond)
		caller.Deliver(&final)
	}()

	got, ok, awaitErr := caller.Await(context.Background(), res.RequestID, time.Second)
	if awaitErr != nil {
		t.Fatalf("Await err: %v", awaitErr)
	}
	if !ok {
		t.Fatal("fast-path window elapsed without final")
	}
	if got.ID != "resp-fast" {
		t.Fatalf("await final id=%s", got.ID)
	}
}

// TestCallerAwaitWindowElapsesNoFinal asserts a window expiry with no final
// returns ok=false, err=nil (degrade to ack) and leaves the future pending so
// a later final still routes.
func TestCallerAwaitWindowElapsesNoFinal(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("req-slow", true)

	got, ok, err := caller.Await(context.Background(), "req-slow", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("window expiry should not be a hard error: %v", err)
	}
	if ok || got != nil {
		t.Fatalf("expected ok=false got=%v", got)
	}
	if !caller.Futures.Registered("req-slow") {
		t.Fatal("future should remain registered after window expiry")
	}
}

// TestCallerWaitNoneImmediateAck asserts window 0 returns immediately without
// parking (fan-out), leaving the future in flight.
func TestCallerWaitNoneImmediateAck(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("req-fanout", false)

	got, ok, err := caller.Await(context.Background(), "req-fanout", 0)
	if err != nil || ok || got != nil {
		t.Fatalf("waitNone: got=%v ok=%v err=%v", got, ok, err)
	}
	if !caller.Futures.Registered("req-fanout") {
		t.Fatal("fan-out future erased")
	}
	final := toolTriggerWithStatus(message.KindResponse, "resp-fanout", "req-fanout", "completed").Envelope
	if disp := caller.Deliver(&final); disp != callkit.NoActiveWaiter {
		t.Fatalf("wait=false final disp=%v want NoActiveWaiter", disp)
	}
}

// TestCallerAbandonThenFinalNoActiveWaiter asserts that after Abandon, a final
// routes through Deliver as NoActiveWaiter (would become a new turn trigger).
func TestCallerAbandonThenFinalNoActiveWaiter(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("req-aband", true)
	caller.Abandon("req-aband")

	final := toolTriggerWithStatus(message.KindResponse, "resp-aband", "req-aband", "completed").Envelope
	if disp := caller.Deliver(&final); disp != callkit.NoActiveWaiter {
		t.Fatalf("after abandon disposition=%v want NoActiveWaiter", disp)
	}
}

// TestCallerPendingListsInFlight asserts Pending returns the in-flight ids and
// drops them after a final is delivered to an Await.
func TestCallerPendingListsInFlight(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.Futures.Register("p1", true)
	caller.Futures.Register("p2", true)

	pending := caller.Pending()
	if len(pending) != 2 {
		t.Fatalf("pending=%v want 2", pending)
	}

	// Resolve p1 via an Await + Deliver race.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = caller.Await(context.Background(), "p1", time.Second)
	}()
	time.Sleep(10 * time.Millisecond)
	final := toolTriggerWithStatus(message.KindResponse, "resp-p1", "p1", "completed").Envelope
	caller.Deliver(&final)
	wg.Wait()

	if caller.Futures.Registered("p1") {
		t.Fatal("p1 still pending after final delivered to await")
	}
	if !caller.Futures.Registered("p2") {
		t.Fatal("p2 wrongly dropped")
	}
}

// TestCallerSubmitWriteFailureRollsBack asserts a write failure cancels the
// registered future (no leak).
func TestCallerSubmitWriteFailureRollsBack(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	ipc := &failWriteIPC{}
	env := message.Envelope{ID: "req-fail-write", Kind: message.KindRequest}
	if _, err := caller.Submit(context.Background(), ipc, env, 1000, true); err == nil {
		t.Fatal("Submit should fail on write error")
	}
	if caller.Futures.Registered("req-fail-write") {
		t.Fatal("future leaked after write failure")
	}
}

type failWriteIPC struct{ metaFakeIPC }

func (f *failWriteIPC) WriteEnvelope(context.Context, message.Envelope) error {
	return context.DeadlineExceeded
}

func toolTrigger(kind message.Kind, id, parentID string) TriggerPayload {
	return TriggerPayload{
		Envelope: message.Envelope{
			ID:            message.ID(id),
			ChannelID:     "ch-kimi",
			Type:          "xhs.publish",
			Kind:          kind,
			Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
			ParentID:      message.ID(parentID),
			CorrelationID: message.ID(parentID),
			Payload:       []byte(`{"status":"completed"}`),
		},
		CorrelationID: message.ID(parentID),
	}
}

// toolTriggerWithStatus is the status-aware variant of toolTrigger. status is
// written verbatim into payload.status so tests can exercise every position of
// the proto-layer0 §2.5.1 closed-set lattice: Layer 1 final (completed/
// failed), Layer 2 core provisional (processing, queued, …), and Layer 3
// business namespace (xhs.login_queued, …).
func toolTriggerWithStatus(kind message.Kind, id, parentID, status string) TriggerPayload {
	tp := toolTrigger(kind, id, parentID)
	tp.Envelope.Payload = []byte(`{"status":"` + status + `"}`)
	return tp
}
