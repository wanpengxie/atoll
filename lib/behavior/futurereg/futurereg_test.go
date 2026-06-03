package futurereg

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

func resp(parent message.ID, status string) *message.Envelope {
	return &message.Envelope{
		ID:       message.ID("resp:" + string(parent) + ":" + status),
		ParentID: parent,
		Kind:     message.KindResponse,
		Payload:  json.RawMessage(`{"status":"` + status + `"}`),
	}
}

// register-before-deliver: a final delivered while an Await is parked is
// handed to that Await.
func TestRegisterThenAwaitGetsFinal(t *testing.T) {
	r := New()
	h := r.Register("R1")
	done := make(chan struct{})
	var got *message.Envelope
	var awaitErr error
	go func() {
		got, awaitErr = h.Await(context.Background(), 2*time.Second)
		close(done)
	}()
	// give the goroutine time to park
	time.Sleep(20 * time.Millisecond)
	disp := r.Deliver(resp("R1", "completed"))
	if disp != DeliveredToAwait {
		t.Fatalf("disposition = %v want DeliveredToAwait", disp)
	}
	<-done
	if awaitErr != nil {
		t.Fatalf("await err: %v", awaitErr)
	}
	if got == nil || got.ParentID != "R1" {
		t.Fatalf("await got %v", got)
	}
}

// final-before-await buffer: final arrives before anyone awaits, and is not
// lost — a subsequent Await returns it.
func TestFinalBeforeAwaitNotLost(t *testing.T) {
	r := New()
	h := r.Register("R2")
	disp := r.Deliver(resp("R2", "completed"))
	if disp != BufferedPendingAwait {
		t.Fatalf("disposition = %v want BufferedPendingAwait", disp)
	}
	got, err := h.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("await err: %v", err)
	}
	if got == nil || got.ParentID != "R2" {
		t.Fatalf("await got %v", got)
	}
}

// provisional does not resolve Await; final does.
func TestProvisionalDoesNotResolveAwait(t *testing.T) {
	r := New()
	h := r.Register("R3")
	if disp := r.Deliver(resp("R3", "queued")); disp != NoActiveWaiter {
		t.Fatalf("provisional with no watcher disp=%v want NoActiveWaiter", disp)
	}
	got, err := h.Await(context.Background(), 200*time.Millisecond)
	if err == nil {
		t.Fatalf("await should have timed out, got %v", got)
	}
}

// Watch receives provisional + final; Await receives only final.
func TestWatchReceivesProvisionalAndFinal(t *testing.T) {
	r := New()
	h := r.Register("R4")
	w, err := h.Watch()
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if disp := r.Deliver(resp("R4", "processing")); disp != DeliveredToWatch {
		t.Fatalf("provisional disp=%v want DeliveredToWatch", disp)
	}
	if disp := r.Deliver(resp("R4", "completed")); disp != DeliveredToWatch {
		t.Fatalf("final disp=%v want DeliveredToWatch", disp)
	}
	var events []WatchEvent
	for ev := range w.Events() {
		events = append(events, ev)
	}
	if len(events) != 2 {
		t.Fatalf("watch events = %d want 2", len(events))
	}
	if events[0].IsFinal {
		t.Fatalf("first event should be provisional")
	}
	if !events[1].IsFinal {
		t.Fatalf("second event should be final")
	}
}

// Cancel (abandon): a parked Await wakes with ErrClosed; a subsequent final
// routes as NoActiveWaiter.
func TestCancelWakesAwaitAndDropsTrigger(t *testing.T) {
	r := New()
	h := r.Register("R5")
	done := make(chan error, 1)
	go func() {
		_, err := h.Await(context.Background(), 2*time.Second)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	r.Cancel("R5")
	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("await err=%v want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not wake on cancel")
	}
	if disp := r.Deliver(resp("R5", "completed")); disp != NoActiveWaiter {
		t.Fatalf("post-cancel final disp=%v want NoActiveWaiter", disp)
	}
}

// M2 atomic disposition: under concurrency, "await about to time out" vs
// "final arriving" must never double-deliver or lose. We run many trials
// with an Await whose timeout fires roughly when the final lands and assert
// exactly-one outcome (either the Await got it OR it surfaced as
// NoActiveWaiter, never both, never neither).
func TestAtomicDispositionRace(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		r := New()
		h := r.Register("RX")

		var awaitGotFinal int32
		var deliverToAwait int32
		var noWaiter int32

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			env, err := h.Await(context.Background(), 5*time.Millisecond)
			if err == nil && env != nil {
				atomic.StoreInt32(&awaitGotFinal, 1)
			}
		}()
		go func() {
			defer wg.Done()
			// land the final right around the await timeout window
			time.Sleep(5 * time.Millisecond)
			switch r.Deliver(resp("RX", "completed")) {
			case DeliveredToAwait:
				atomic.StoreInt32(&deliverToAwait, 1)
			case NoActiveWaiter:
				atomic.StoreInt32(&noWaiter, 1)
			}
		}()
		wg.Wait()

		// Deliver returns exactly one disposition (never both).
		if atomic.LoadInt32(&deliverToAwait) == 1 && atomic.LoadInt32(&noWaiter) == 1 {
			t.Fatalf("trial %d: both DeliveredToAwait and NoActiveWaiter (double)", trial)
		}
		if atomic.LoadInt32(&deliverToAwait) == 0 && atomic.LoadInt32(&noWaiter) == 0 {
			t.Fatalf("trial %d: no disposition recorded (lost)", trial)
		}
		// Exactly-once invariant: the final is either observed by the racing
		// Await or classified as a no-waiter final for caller surfacing. In no
		// case may the same final be observed twice.
		if atomic.LoadInt32(&awaitGotFinal) == 1 {
			// observed directly; a second await must not also see it
			env, err := h.Await(context.Background(), 50*time.Millisecond)
			if err == nil && env != nil {
				t.Fatalf("trial %d: final observed twice (double-deliver)", trial)
			}
		} else if registered(r, "RX") {
			// not observed by the racing Await → Deliver must have atomically
			// classified it as NoActiveWaiter and cleared the future so a later
			// await cannot double-consume the surfaced trigger.
			t.Fatalf("trial %d: surfaced final left future registered", trial)
		}
	}
}

// TestM2NoDoubleLossAwaitTimeoutVsFinal is the F2-strict invariant: when
// Deliver returns DeliveredToAwait, the awaiting Await MUST observe that final
// (it cannot simultaneously report a timeout). The earlier state machine had a
// window where the timer-fire select arm won while Deliver had already pushed
// the final + returned DeliveredToAwait — yielding "Await timed out AND Deliver
// said DeliveredToAwait", a double-loss. The single-lock resolveOnWake closes
// that: whoever takes the lock second observes the first's effect, so the two
// outcomes are mutually exclusive and consistent.
//
// Run many trials with the timer firing right as the final lands; assert that
// whenever Deliver==DeliveredToAwait the Await returned the final, and whenever
// the Await timed out Deliver==NoActiveWaiter and the future is cleared for
// caller surfacing.
func TestM2NoDoubleLossAwaitTimeoutVsFinal(t *testing.T) {
	const trials = 400
	for trial := 0; trial < trials; trial++ {
		r := New()
		h := r.Register("RM2")

		var awaitFinal int32 // 1 = Await returned the final
		var awaitTimeout int32
		var disp Disposition

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			env, err := h.Await(context.Background(), 3*time.Millisecond)
			if err == nil && env != nil {
				atomic.StoreInt32(&awaitFinal, 1)
			} else {
				atomic.StoreInt32(&awaitTimeout, 1)
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(3 * time.Millisecond)
			disp = r.Deliver(resp("RM2", "completed"))
		}()
		wg.Wait()

		gotFinal := atomic.LoadInt32(&awaitFinal) == 1
		gotTimeout := atomic.LoadInt32(&awaitTimeout) == 1

		switch disp {
		case DeliveredToAwait:
			// STRICT: Deliver claims it handed the final to the awaiter, so the
			// awaiter MUST have returned the final (never a timeout). This is
			// the exact double-loss the F2 fix forbids.
			if !gotFinal || gotTimeout {
				t.Fatalf("trial %d: disp=DeliveredToAwait but Await final=%v timeout=%v (double-loss)",
					trial, gotFinal, gotTimeout)
			}
		case NoActiveWaiter:
			// Await won the lock first (timed out), so Deliver classifies the
			// later final as a no-waiter trigger and clears the future. The
			// awaiter must have timed out and no later Await may consume it.
			if gotFinal {
				t.Fatalf("trial %d: disp=NoActiveWaiter but Await also got the final (double-deliver)", trial)
			}
			if registered(r, "RM2") {
				t.Fatalf("trial %d: NoActiveWaiter final left future registered", trial)
			}
		default:
			t.Fatalf("trial %d: unexpected disposition %v", trial, disp)
		}
	}
}

// TestWatchFinalNeverDroppedUnderProvisionalStorm is the F3 invariant: the
// watch buffer (cap 16) fills with provisionals, then a final arrives. The
// final must NOT be dropped — it must be delivered and be the last event
// before the stream closes. The old push() dropped on a full buffer, which
// could eat the final and make a downstream Await mis-report a timeout.
func TestWatchFinalNeverDroppedUnderProvisionalStorm(t *testing.T) {
	r := New()
	h := r.Register("RF3")
	w, err := h.Watch()
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Flood far past the 16-slot buffer with provisionals (no consumer reading).
	for i := 0; i < 100; i++ {
		r.Deliver(resp("RF3", "processing"))
	}
	// Now the final — must be delivered despite the full buffer.
	if disp := r.Deliver(resp("RF3", "completed")); disp != DeliveredToWatch {
		t.Fatalf("final disp=%v want DeliveredToWatch", disp)
	}

	// Drain the stream; the LAST event MUST be the final.
	var events []WatchEvent
	for ev := range w.Events() {
		events = append(events, ev)
	}
	if len(events) == 0 {
		t.Fatal("F3: watch stream delivered nothing (final dropped)")
	}
	last := events[len(events)-1]
	if !last.IsFinal {
		t.Fatalf("F3: last watch event is not the final (final dropped); got IsFinal=%v", last.IsFinal)
	}
	// And no provisional was ever marked final.
	for i := 0; i < len(events)-1; i++ {
		if events[i].IsFinal {
			t.Fatalf("F3: event %d wrongly marked final", i)
		}
	}
}

// TestDeliver_InvalidInput pins the honesty of the Disposition vocabulary:
// input that is not a routable response (nil / non-response Kind / unparseable
// status) classifies as InvalidDisposition — a distinct fact from a valid
// response that finds no waiter (NoActiveWaiter). The guard runs before any
// waiter lookup, so registration state is irrelevant.
func TestDeliver_InvalidInput(t *testing.T) {
	r := New()
	r.Register("R1") // a live waiter must not turn invalid input into a route

	if disp := r.Deliver(nil); disp != InvalidDisposition {
		t.Fatalf("nil envelope: disp=%v, want InvalidDisposition", disp)
	}
	nonResponse := &message.Envelope{ParentID: "R1", Kind: message.KindEvent,
		Payload: json.RawMessage(`{"status":"completed"}`)}
	if disp := r.Deliver(nonResponse); disp != InvalidDisposition {
		t.Fatalf("non-response Kind: disp=%v, want InvalidDisposition", disp)
	}
	badPayload := &message.Envelope{ParentID: "R1", Kind: message.KindResponse,
		Payload: json.RawMessage(`["not an object"]`)}
	if disp := r.Deliver(badPayload); disp != InvalidDisposition {
		t.Fatalf("unparseable status: disp=%v, want InvalidDisposition", disp)
	}
	// The live waiter survived — invalid input never touched it.
	if !registered(r, "R1") {
		t.Fatal("invalid input wrongly cleared a live waiter")
	}
}

func TestParseStatus(t *testing.T) {
	if s, err := parseStatusErr(json.RawMessage(`{"status":"completed"}`)); err != nil || s != "completed" {
		t.Fatalf("parseStatusErr = %q, %v", s, err)
	}
	if s, err := parseStatusErr(json.RawMessage(`null`)); err != nil || s != "" {
		t.Fatalf("parseStatusErr(null) = %q, %v", s, err)
	}
	if s, err := parseStatusErr(nil); err != nil || s != "" {
		t.Fatalf("parseStatusErr(nil) = %q, %v", s, err)
	}
	// Malformed payload surfaces an error (Deliver maps it to InvalidDisposition).
	if _, err := parseStatusErr(json.RawMessage(`["not an object"]`)); err == nil {
		t.Fatal("parseStatusErr on non-object payload: want error, got nil")
	}
}

// registered reports whether id is still in the registry, asked through the
// authoritative Pending() enumeration (membership is contains(Pending, id) —
// there is no separate membership-test method).
func registered(r *FutureRegistry, id message.ID) bool {
	for _, p := range r.Pending() {
		if p == id {
			return true
		}
	}
	return false
}

func TestPendingLists(t *testing.T) {
	r := New()
	r.Register("A")
	r.Register("B")
	ids := r.Pending()
	if len(ids) != 2 {
		t.Fatalf("pending = %v", ids)
	}
	r.Cancel("A")
	if got := r.Pending(); len(got) != 1 || got[0] != "B" {
		t.Fatalf("pending after cancel = %v", got)
	}
}
