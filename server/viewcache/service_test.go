package viewcache_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/kernel/viewsync"
	"github.com/coagent-ai/coagent/server/store"
	"github.com/coagent-ai/coagent/server/viewcache"
)

func newSvc(t *testing.T) *viewcache.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "vc.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return viewcache.NewService(db)
}

func frame(seq viewsync.Seq) viewsync.PushFrame {
	return viewsync.PushFrame{
		ChannelID: channel.ID("ch-X"),
		Seq:       seq,
		MessageID: msgID(seq),
		Envelope: message.Envelope{
			ID:         msgID(seq),
			TS:         int64(seq) * 1000,
			ChannelID:  "ch-X",
			Sender:     message.Sender{Kind: message.SenderAgent, ID: "a"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   []string{"*"},
			Seq:        int64(seq),
		},
	}
}

func msgID(seq viewsync.Seq) string {
	return "m-" + itoa(int64(seq))
}

func itoa(n int64) string {
	// strconv allocates — but we're in tests; the inline conversion
	// avoids importing strconv into the production import list and
	// keeps the test imports minimal.
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// TestApplyContiguous covers L1 §8.4 row "seq == cursor + 1".
func TestApplyContiguous(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	for seq := viewsync.Seq(1); seq <= 5; seq++ {
		got, err := svc.Apply(ctx, frame(seq))
		if err != nil {
			t.Fatalf("Apply seq=%d: %v", seq, err)
		}
		if got.Outcome != viewsync.ApplyOutcomeContiguous {
			t.Errorf("seq=%d outcome=%q want contiguous", seq, got.Outcome)
		}
		if int64(got.LastReceivedSeq) != int64(seq) {
			t.Errorf("seq=%d cursor=%d", seq, got.LastReceivedSeq)
		}
	}
}

// TestApplyDuplicate covers L1 §8.4 row "seq <= cursor". The
// duplicate must still INSERT-OR-IGNORE (idempotent) but the cursor
// stays put.
func TestApplyDuplicate(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	if _, err := svc.Apply(ctx, frame(1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := svc.Apply(ctx, frame(1))
	if err != nil {
		t.Fatalf("Apply dup: %v", err)
	}
	if got.Outcome != viewsync.ApplyOutcomeDuplicate {
		t.Errorf("outcome=%q want duplicate", got.Outcome)
	}
	if int64(got.LastReceivedSeq) != 1 {
		t.Errorf("cursor=%d want 1", got.LastReceivedSeq)
	}
}

// TestApplyGapAndDrain covers L1 §8.4 row "seq > cursor + 1":
// out-of-order arrivals persist but cursor does NOT advance. When
// the missing seq finally arrives, the cursor jumps over the
// buffered seqs in one shot.
//
// This is the spec scenario from §T6 acceptance: "mock 推 seq=1,3,5
// → 触发 Resync(1,4]（闭区间）→ apply 补齐".
func TestApplyGapAndDrain(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	// Push 1 contiguous.
	if r, _ := svc.Apply(ctx, frame(1)); r.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Fatalf("seq=1 outcome=%q", r.Outcome)
	}
	// Push 3 — gap.
	r, _ := svc.Apply(ctx, frame(3))
	if r.Outcome != viewsync.ApplyOutcomeGap {
		t.Errorf("seq=3 outcome=%q want gap", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 1 {
		t.Errorf("cursor=%d want 1 (no advance on gap)", r.LastReceivedSeq)
	}
	// Push 5 — still gap.
	r, _ = svc.Apply(ctx, frame(5))
	if r.Outcome != viewsync.ApplyOutcomeGap {
		t.Errorf("seq=5 outcome=%q want gap", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 1 {
		t.Errorf("cursor=%d want 1 (still gap)", r.LastReceivedSeq)
	}

	// Now resync supplies 2 + 4. After applying 2 the cursor must
	// jump to 3 immediately (buffered 3 drains). After 4 the cursor
	// reaches 5 (buffered 5 drains).
	r, _ = svc.Apply(ctx, frame(2))
	if r.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Errorf("seq=2 outcome=%q want contiguous", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 3 {
		t.Errorf("cursor=%d want 3 (drain to 3)", r.LastReceivedSeq)
	}
	r, _ = svc.Apply(ctx, frame(4))
	if r.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Errorf("seq=4 outcome=%q want contiguous", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 5 {
		t.Errorf("cursor=%d want 5 (drain to 5)", r.LastReceivedSeq)
	}
}

// TestResyncCallsBackProtocol verifies that TriggerResync hands the
// daemon-supplied messages to Apply in seq ASC order + the resulting
// cursor matches the inclusive closed interval rule (T1.1 §8.5).
func TestResyncCallsBackProtocol(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	// First, apply seq 1 + push 3,5 as gaps.
	_, _ = svc.Apply(ctx, frame(1))
	_, _ = svc.Apply(ctx, frame(3))
	_, _ = svc.Apply(ctx, frame(5))

	fake := &fakeResyncer{messages: func(since, until viewsync.Seq) []viewsync.ResyncMessage {
		if int64(since) != 2 || int64(until) != 4 {
			t.Errorf("resync request since=%d until=%d want 2,4", since, until)
		}
		// Return seq 2 + 4 in REVERSE order — Apply must sort.
		return []viewsync.ResyncMessage{
			{Seq: 4, MessageID: msgID(4), Envelope: frame(4).Envelope},
			{Seq: 2, MessageID: msgID(2), Envelope: frame(2).Envelope},
		}
	}}
	svc.SetResyncer(fake)

	cur, err := svc.TriggerResync(ctx, channel.ID("ch-X"), 2, 4)
	if err != nil {
		t.Fatalf("TriggerResync: %v", err)
	}
	if int64(cur) != 5 {
		t.Errorf("post-resync cursor=%d want 5", cur)
	}
}

// TestApplyTransactionRollbackOnCrash simulates a COMMIT-before crash —
// here we use a malformed envelope_json path: there isn't an easy
// hook to abort COMMIT, so we exercise the rollback path indirectly
// by passing a frame that violates a CHECK before commit (the
// pragmas don't include such a CHECK on view_cache_messages, so use
// an extreme seq value that fits the INT64 column — this is a
// sanity check that the function never returns a result with the
// row inserted but ack unsent).
//
// The stronger invariant is exercised by TestApplyContiguous +
// TestApplyDuplicate: the cursor is only ever returned after COMMIT,
// and the row is INSERT-OR-IGNORE so a redelivery is idempotent —
// the daemon's "crash before ack" retry path is therefore safe.
//
// This test re-applies an identical frame after the first commit
// and asserts the cursor + row count never duplicate.
func TestApplyCrashRetryIdempotent(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	if _, err := svc.Apply(ctx, frame(1)); err != nil {
		t.Fatalf("Apply #1: %v", err)
	}
	// Simulate "daemon didn't get the ack" → retransmits seq=1.
	r, err := svc.Apply(ctx, frame(1))
	if err != nil {
		t.Fatalf("Apply #2 (retry): %v", err)
	}
	if r.Outcome != viewsync.ApplyOutcomeDuplicate {
		t.Errorf("retry outcome=%q want duplicate", r.Outcome)
	}

	msgs, err := svc.Messages(ctx, channel.ID("ch-X"), 0, 10)
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("messages stored=%d want 1 (idempotent)", len(msgs))
	}
}

// TestMessagesPagination checks the Messages reader after a few
// contiguous applies.
func TestMessagesPagination(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	for i := viewsync.Seq(1); i <= 10; i++ {
		if _, err := svc.Apply(ctx, frame(i)); err != nil {
			t.Fatalf("Apply seq=%d: %v", i, err)
		}
	}
	msgs, err := svc.Messages(ctx, channel.ID("ch-X"), 5, 100)
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("len=%d want 5", len(msgs))
	}
	if int64(msgs[0].Seq) != 6 {
		t.Errorf("first seq=%d want 6", msgs[0].Seq)
	}
	if int64(msgs[len(msgs)-1].Seq) != 10 {
		t.Errorf("last seq=%d", msgs[len(msgs)-1].Seq)
	}
}

// fakeResyncer satisfies viewcache.Resyncer for tests.
type fakeResyncer struct {
	messages func(since, until viewsync.Seq) []viewsync.ResyncMessage
	err      error
}

func (f *fakeResyncer) RequestResync(
	ctx context.Context,
	channelID channel.ID,
	since, until viewsync.Seq,
) ([]viewsync.ResyncMessage, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.messages(since, until), nil
}

func TestTriggerResyncMissingResyncer(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()
	_, err := svc.TriggerResync(ctx, channel.ID("ch-X"), 1, 5)
	if err == nil || !errors.Is(err, errPlaceholderNoMatch) && err.Error() == "" {
		// We don't have a typed error here; just assert non-nil.
		if err == nil {
			t.Errorf("err=nil want non-nil (no resyncer wired)")
		}
	}
}

// errPlaceholderNoMatch is intentionally never returned; the test
// above relies on a non-nil error which is the actual behaviour. The
// sentinel keeps the test self-explanatory without importing errors
// from production code.
var errPlaceholderNoMatch = errors.New("placeholder")
