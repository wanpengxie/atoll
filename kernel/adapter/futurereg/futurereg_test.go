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
	if disp != NoActiveWaiter {
		t.Fatalf("disposition = %v want NoActiveWaiter (buffered)", disp)
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
		// Exactly-once invariant: the final is never lost. Either the racing
		// Await observed it directly, or it must be recoverable by a fresh
		// Await (re-buffered when Await timed out as the final landed — the
		// detach path preserves it). In no case may the same final be
		// observed twice.
		if atomic.LoadInt32(&awaitGotFinal) == 1 {
			// observed directly; a second await must not also see it
			env, err := h.Await(context.Background(), 50*time.Millisecond)
			if err == nil && env != nil {
				t.Fatalf("trial %d: final observed twice (double-deliver)", trial)
			}
		} else {
			// not observed by the racing Await → must be recoverable
			env, err := h.Await(context.Background(), 200*time.Millisecond)
			if err != nil || env == nil {
				t.Fatalf("trial %d: final neither observed nor recoverable (lost): err=%v", trial, err)
			}
		}
	}
}

func TestParseStatus(t *testing.T) {
	if s := parseStatus(json.RawMessage(`{"status":"completed"}`)); s != "completed" {
		t.Fatalf("parseStatus = %q", s)
	}
	if s := parseStatus(json.RawMessage(`null`)); s != "" {
		t.Fatalf("parseStatus(null) = %q", s)
	}
	if s := parseStatus(nil); s != "" {
		t.Fatalf("parseStatus(nil) = %q", s)
	}
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
