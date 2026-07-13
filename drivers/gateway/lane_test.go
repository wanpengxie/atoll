package gateway

import "testing"

// TestLaneCapacityFullDisconnect: the lane accepts exactly LaneCapacity frames
// without a drainer, then the overflow push is refused, counted, AND seals the
// lane itself (满 → 自封断连 — the disconnect verdict is the lane's own, not a
// caller convention; DoD-11 lane 三断言: 容量 64 / 满自封 / drop 计数).
func TestLaneCapacityFullDisconnect(t *testing.T) {
	l := newLane(newCursor(nil))
	for i := 0; i < LaneCapacity; i++ {
		if !l.push([]byte("x")) {
			t.Fatalf("push %d/%d should fit within capacity", i, LaneCapacity)
		}
	}
	if l.push([]byte("overflow")) {
		t.Fatal("push past capacity should be refused (满 → 自封断连)")
	}
	if got := l.DroppedCount(); got != 1 {
		t.Fatalf("dropped count = %d, want 1", got)
	}
	if !l.isClosed() {
		t.Fatal("overflow must seal the lane itself — a caller ignoring ok=false must not leave a doomed lane half-alive")
	}
	if l.push([]byte("after-seal")) {
		t.Fatal("push after the overflow seal must be refused")
	}
	if got := l.DroppedCount(); got != 1 {
		t.Fatalf("post-seal pushes are refusals of a closed lane, not new drops: dropped = %d, want 1", got)
	}
	if LaneCapacity != 64 {
		t.Fatalf("lane capacity = %d, want 64 (照 ws.go outbound)", LaneCapacity)
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
