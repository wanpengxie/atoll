package gateway

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestLaneCursorReconnectRecovery: a reconnect frame's since map seeds the cursor,
// so the read pump resumes AFTER the last-seen seq — no replay from 0 (DoD-6 map
// 形游标重连恢复, 今日单元素退化).
func TestLaneCursorReconnectRecovery(t *testing.T) {
	ch := channel.ID("chan-A")
	cur := newCursor(map[channel.ID]int64{ch: 42})
	if got := cur.at(ch); got != 42 {
		t.Fatalf("reconnect cursor = %d, want the since seq 42", got)
	}
	// A fresh (server-restart) cursor with no since is zero state (DoD-6 server 重启
	// 零游标态).
	fresh := newCursor(nil)
	if got := fresh.at(ch); got != 0 {
		t.Fatalf("fresh cursor = %d, want 0 (server 零持久化)", got)
	}
}

// TestLaneCursorCrossChannelIndependent: seq is a PER-CHANNEL local number, so the
// same seq value on two channels never crosses and advancing one leaves the other
// untouched (DoD-6 两绑定跨频道同 seq 值不串).
func TestLaneCursorCrossChannelIndependent(t *testing.T) {
	a := channel.ID("A")
	b := channel.ID("B")
	cur := newCursor(nil)
	cur.advance(a, 10)
	if cur.at(b) != 0 {
		t.Fatalf("advancing A must not move B; B = %d, want 0", cur.at(b))
	}
	cur.advance(b, 3)
	if cur.at(a) != 10 || cur.at(b) != 3 {
		t.Fatalf("cross-channel components crossed: A=%d B=%d, want 10/3", cur.at(a), cur.at(b))
	}
}

// TestLaneCursorAdvanceMonotonic: the cursor only ever moves forward — a stale
// lower seq (or a receipt's write-seq the client wrongly folded) can never rewind
// it (DoD-6 receipt 不推游标: only feed advance touches the cursor, and only forward).
func TestLaneCursorAdvanceMonotonic(t *testing.T) {
	ch := channel.ID("A")
	cur := newCursor(nil)
	cur.advance(ch, 5)
	cur.advance(ch, 3) // stale / lower — must not rewind
	if got := cur.at(ch); got != 5 {
		t.Fatalf("cursor rewound to %d, want 5 (forward-only)", got)
	}
	cur.advance(ch, 8)
	if got := cur.at(ch); got != 8 {
		t.Fatalf("cursor = %d, want 8", got)
	}
}
