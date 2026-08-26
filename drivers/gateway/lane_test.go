package gateway

import (
	"testing"
	"time"
)

// TestLaneFullAppliesBackpressureAtFixedCapacity: capacity remains bounded at 64,
// but a transiently full queue waits for its already-runnable drainer instead of
// misclassifying scheduling latency as a slow consumer.
func TestLaneFullAppliesBackpressureAtFixedCapacity(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < laneCapacity; i++ {
		if !l.push([]byte("x")) {
			t.Fatalf("push %d/%d should fit within capacity", i, laneCapacity)
		}
	}
	started := make(chan struct{})
	pushed := make(chan bool, 1)
	go func() {
		close(started)
		pushed <- l.push([]byte("backpressured"))
	}()
	<-started
	select {
	case ok := <-pushed:
		t.Fatalf("push past capacity returned before a drain: ok=%v", ok)
	case <-time.After(50 * time.Millisecond):
	}
	<-l.live
	select {
	case ok := <-pushed:
		if !ok {
			t.Fatal("backpressured push must succeed after the drainer makes room")
		}
	case <-time.After(time.Second):
		t.Fatal("backpressured push did not resume after the drainer made room")
	}
	select {
	case <-l.closed:
		t.Fatal("temporary queue pressure must not close the lane")
	default:
	}
	if laneCapacity != 64 {
		t.Fatalf("lane capacity = %d, want 64 (照 ws.go outbound)", laneCapacity)
	}
}

func TestLaneCloseReleasesBackpressuredPush(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < laneCapacity; i++ {
		if !l.push([]byte("x")) {
			t.Fatalf("push %d/%d should fit within capacity", i, laneCapacity)
		}
	}
	started := make(chan struct{})
	pushed := make(chan bool, 1)
	go func() {
		close(started)
		pushed <- l.push([]byte("blocked"))
	}()
	<-started
	select {
	case ok := <-pushed:
		t.Fatalf("full-lane push returned before Close: ok=%v", ok)
	case <-time.After(50 * time.Millisecond):
	}
	l.close()
	select {
	case ok := <-pushed:
		if ok {
			t.Fatal("push released by Close must be refused")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not release a backpressured push")
	}
}

// TestLaneCloseRefusesPush: a closed lane refuses further pushes (drain unblocked).
func TestLaneCloseRefusesPush(t *testing.T) {
	l := newLane(newCursor(nil))
	if !l.push([]byte("a")) {
		t.Fatal("first push should succeed")
	}
	l.close()
	if l.push([]byte("b")) {
		t.Fatal("push on a closed lane must be refused")
	}
	select {
	case <-l.closed:
	default:
		t.Fatal("lane should be closed")
	}
	l.close() // idempotent
}

func TestInteractiveHistoryDoesNotQueueBehindAttachBackfill(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < backfillLaneCapacity; i++ {
		if !l.pushBackfill([]byte("attach")) {
			t.Fatalf("attach backfill %d/%d should fit", i, backfillLaneCapacity)
		}
	}
	done := make(chan bool, 1)
	go func() { done <- l.pushHistory([]byte("interactive")) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("interactive history push was refused")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("interactive history shared backpressure with attach replay")
	}
	select {
	case got := <-l.history:
		if string(got) != "interactive" {
			t.Fatalf("history frame = %q", got)
		}
	default:
		t.Fatal("interactive history did not enter its own lane")
	}
}
