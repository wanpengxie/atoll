// NF2 contiguity-bound unit test against the gateway-internal
// viewcacheReplayer. It stays in the gateway package so the test can
// construct the package-private adapter directly and assert that a
// since_seq replay never hands the client gap-buffered rows beyond the
// contiguous cursor (last_received_seq).
package gateway

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/store"
	"github.com/wanpengxie/ActOS/server/viewcache"
)

func replayerTestFrame(ch channel.ID, seq viewsync.Seq) viewsync.PushFrame {
	id := message.ID("m-" + seqStr(int64(seq)))
	return viewsync.PushFrame{
		ChannelID: ch,
		Seq:       seq,
		MessageID: id,
		Envelope: message.Envelope{
			ID:         id,
			TS:         int64(seq) * 1000,
			ChannelID:  ch,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.SystemActorID},
			Seq:        int64(seq),
		},
	}
}

func seqStr(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// TestViewcacheReplayer_StopsAtContiguousCursor is the NF2 regression:
// viewcache applies seq 1,2,4 (3 missing → 4 is gap-buffered, cursor
// stays at 2). A since_seq=0 replay MUST only return seq 1,2 — the
// contiguous prefix — never the gap-buffered seq 4. seq 4 reaches the
// client via live fanout once seq 3 arrives and the cursor advances.
func TestViewcacheReplayer_StopsAtContiguousCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "vc.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	vc := viewcache.NewService(db)
	// Disable the fire-and-forget gap resync so the gap stays open for
	// the duration of the assertion (no resyncer wired → no-op anyway,
	// but be explicit).
	vc.SetFireResyncForTest(func(channel.ID, viewsync.Seq, viewsync.Seq) {})

	ch := channel.ID("ch-gap")
	if _, err := vc.Apply(ctx, replayerTestFrame(ch, 1)); err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	if _, err := vc.Apply(ctx, replayerTestFrame(ch, 2)); err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	// seq 3 missing → seq 4 is gap-buffered (persisted, cursor unchanged).
	if _, err := vc.Apply(ctx, replayerTestFrame(ch, 4)); err != nil {
		t.Fatalf("apply 4: %v", err)
	}

	cursor, err := vc.Cursor(ctx, ch)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if cursor != 2 {
		t.Fatalf("contiguous cursor=%d want=2 (seq 4 must be gap-buffered)", cursor)
	}

	r := viewcacheReplayer{vc: vc}
	rows, err := r.ReplayMessages(ctx, ch, 0, 500)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	got := make([]int64, 0, len(rows))
	for _, m := range rows {
		got = append(got, int64(m.Seq))
	}
	want := []int64{1, 2}
	if len(got) != len(want) {
		t.Fatalf("replayed seqs=%v want=%v (gap-buffered seq 4 must not be replayed)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replayed seqs=%v want=%v", got, want)
		}
	}

	// After the gap fills (seq 3 arrives), the cursor advances to 4 and a
	// fresh replay returns the full contiguous prefix.
	if _, err := vc.Apply(ctx, replayerTestFrame(ch, 3)); err != nil {
		t.Fatalf("apply 3: %v", err)
	}
	cursor, err = vc.Cursor(ctx, ch)
	if err != nil {
		t.Fatalf("cursor after fill: %v", err)
	}
	if cursor != 4 {
		t.Fatalf("cursor after gap fill=%d want=4", cursor)
	}
	rows, err = r.ReplayMessages(ctx, ch, 0, 500)
	if err != nil {
		t.Fatalf("replay after fill: %v", err)
	}
	got = got[:0]
	for _, m := range rows {
		got = append(got, int64(m.Seq))
	}
	want = []int64{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("post-fill replayed seqs=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("post-fill replayed seqs=%v want=%v", got, want)
		}
	}
}
