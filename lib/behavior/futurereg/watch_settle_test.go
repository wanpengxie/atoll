package futurereg

import (
	"context"
	"testing"
	"time"
)

// A Watch that consumes a final-before-await buffer must SETTLE the waiterSet,
// mirroring Await's buffered-final path and Deliver's watched-final path: the
// final is the closure terminal, so after one consumer takes it the buffer is
// cleared and the set dropped. Regression: Watch used to emit the buffered
// final and close only its own stream, leaving finalBuf + the waiter behind —
// a later Watch/Await re-consumed the same final and Pending() leaked the id.
func TestWatchConsumesBufferedFinalAndSettles(t *testing.T) {
	r := New()
	h := r.Register("RW")
	if disp := r.Deliver(resp("RW", "completed")); disp != BufferedPendingAwait {
		t.Fatalf("disp=%v want BufferedPendingAwait", disp)
	}

	w, err := h.Watch()
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	var events []WatchEvent
	for ev := range w.Events() {
		events = append(events, ev)
	}
	if len(events) != 1 || !events[0].IsFinal {
		t.Fatalf("watch events=%+v want exactly one final", events)
	}

	// Settled: the waiter is gone from Pending().
	if p := r.Pending(); len(p) != 0 {
		t.Fatalf("Pending()=%v want empty after Watch consumed the buffered final", p)
	}

	// Not re-consumable: a follow-up Await reports closed, not the final again.
	if _, err := h.Await(context.Background(), 50*time.Millisecond); err != ErrClosed {
		t.Fatalf("second consume err=%v want ErrClosed", err)
	}
}
