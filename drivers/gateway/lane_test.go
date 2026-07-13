package gateway

import "testing"

// TestLaneCapacityFullDisconnect: the lane accepts exactly LaneCapacity frames
// without a drainer, then the overflow push is refused (满策略 = 断连) and counted
// (DoD-11 lane 三断言: 容量 64 / 满断连 / drop 计数).
func TestLaneCapacityFullDisconnect(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < LaneCapacity; i++ {
		if !l.push([]byte("x")) {
			t.Fatalf("push %d/%d should fit within capacity", i, LaneCapacity)
		}
	}
	if l.push([]byte("overflow")) {
		t.Fatal("push past capacity should be refused (满 → 断连)")
	}
	if got := l.DroppedCount(); got != 1 {
		t.Fatalf("dropped count = %d, want 1", got)
	}
	if !l.fullBreak.Load() {
		t.Fatal("full push should latch the disconnect signal")
	}
	if LaneCapacity != 64 {
		t.Fatalf("lane capacity = %d, want 64 (照 ws.go outbound)", LaneCapacity)
	}
}

// TestLaneDropCountAccumulates: every overflow push increments the drop tally.
func TestLaneDropCountAccumulates(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < LaneCapacity; i++ {
		l.push([]byte("x"))
	}
	for i := 0; i < 5; i++ {
		l.push([]byte("drop"))
	}
	if got := l.DroppedCount(); got != 5 {
		t.Fatalf("dropped = %d, want 5", got)
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
	if !l.isClosed() {
		t.Fatal("lane should report closed")
	}
	l.close() // idempotent
}
