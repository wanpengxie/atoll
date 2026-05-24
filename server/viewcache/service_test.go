package viewcache_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/store"
	"github.com/wanpengxie/ActOS/server/viewcache"
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

func msgID(seq viewsync.Seq) message.ID {
	return message.ID("m-" + itoa(int64(seq)))
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
		// FIX-T5: natural contiguous (no buffered drain) must NOT
		// populate DrainedMessages — gateway falls back to fan-out
		// of the current frame.
		if len(got.DrainedMessages) != 0 {
			t.Errorf("seq=%d drained=%v want empty (no buffered drain)", seq, got.DrainedMessages)
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
	// FIX-T5: drained list is [current=2, buffered=3] in seq ASC.
	if got := seqsOf(r.DrainedMessages); !equalSeqs(got, []viewsync.Seq{2, 3}) {
		t.Errorf("seq=2 drained=%v want [2 3]", got)
	}
	r, _ = svc.Apply(ctx, frame(4))
	if r.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Errorf("seq=4 outcome=%q want contiguous", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 5 {
		t.Errorf("cursor=%d want 5 (drain to 5)", r.LastReceivedSeq)
	}
	// FIX-T5: drained list is [current=4, buffered=5].
	if got := seqsOf(r.DrainedMessages); !equalSeqs(got, []viewsync.Seq{4, 5}) {
		t.Errorf("seq=4 drained=%v want [4 5]", got)
	}
}

// TestApplyGapTriggersResync covers FIX-T5 part 3: when Apply detects
// a gap (seq > cursor+1) it MUST fire a TriggerResync for the missing
// closed interval [cursor+1, seq-1]. Test installs a fake fire-resync
// hook + asserts the captured (since, until) window.
func TestApplyGapTriggersResync(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	// Wire a resyncer that returns no messages — the auto-trigger
	// path only needs the call signature; subsequent Applys are not
	// exercised here.
	fake := &fakeResyncer{messages: func(since, until viewsync.Seq) []viewsync.ResyncMessage {
		return nil
	}}
	svc.SetResyncer(fake)

	type call struct{ since, until viewsync.Seq }
	calls := make(chan call, 4)
	svc.SetFireResyncForTest(func(channelID channel.ID, since, until viewsync.Seq) {
		calls <- call{since, until}
	})

	// Push seq=1 (contiguous) — no trigger expected.
	if _, err := svc.Apply(ctx, frame(1)); err != nil {
		t.Fatalf("Apply seq=1: %v", err)
	}
	// Push seq=3 — gap [2,2]; trigger expected.
	r, err := svc.Apply(ctx, frame(3))
	if err != nil {
		t.Fatalf("Apply seq=3: %v", err)
	}
	if r.Outcome != viewsync.ApplyOutcomeGap {
		t.Errorf("seq=3 outcome=%q want gap", r.Outcome)
	}
	if int64(r.LastReceivedSeq) != 1 {
		t.Errorf("seq=3 cursor=%d want 1", r.LastReceivedSeq)
	}
	if len(r.DrainedMessages) != 0 {
		t.Errorf("seq=3 drained=%v want empty (gap path)", r.DrainedMessages)
	}
	select {
	case c := <-calls:
		if int64(c.since) != 2 || int64(c.until) != 2 {
			t.Errorf("gap trigger window=[%d,%d] want [2,2]", c.since, c.until)
		}
	default:
		t.Fatal("gap did not trigger resync")
	}

	// Push seq=5 immediately after the first gap. R7-7 rate-limits
	// automatic gap resyncs, so the durable row is retained but no
	// second fire-and-forget trigger is emitted inside the cooldown.
	r, err = svc.Apply(ctx, frame(5))
	if err != nil {
		t.Fatalf("Apply seq=5: %v", err)
	}
	if r.Outcome != viewsync.ApplyOutcomeGap {
		t.Errorf("seq=5 outcome=%q want gap", r.Outcome)
	}
	select {
	case c := <-calls:
		t.Fatalf("seq=5 fired resync during cooldown: [%d,%d]", c.since, c.until)
	default:
	}
}

// TestApplyResyncRace covers FIX-T5 ordering: live push seq=5 lands
// first (gap), then resync supplies seq=3,4. The fan-out order the
// gateway will see — i.e. the catenation of DrainedMessages across
// Applys — MUST be [3, 4, 5] (no skip, no duplicate).
func TestApplyResyncRace(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	// Resyncer not needed — we manually call Apply for the resync
	// messages. Just suppress the auto-trigger goroutine.
	svc.SetFireResyncForTest(func(channel.ID, viewsync.Seq, viewsync.Seq) {})

	// Establish cursor=2.
	if _, err := svc.Apply(ctx, frame(1)); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	if _, err := svc.Apply(ctx, frame(2)); err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	// Live push seq=5 arrives first → gap, buffered.
	r5, _ := svc.Apply(ctx, frame(5))
	if r5.Outcome != viewsync.ApplyOutcomeGap {
		t.Fatalf("seq=5 outcome=%q want gap", r5.Outcome)
	}
	if len(r5.DrainedMessages) != 0 {
		t.Errorf("seq=5 drained=%v want empty", r5.DrainedMessages)
	}

	// Resync brings 3 → contiguous, no buffer to drain yet.
	r3, _ := svc.Apply(ctx, frame(3))
	if r3.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Fatalf("seq=3 outcome=%q want contiguous", r3.Outcome)
	}
	if int64(r3.LastReceivedSeq) != 3 {
		t.Errorf("seq=3 cursor=%d want 3", r3.LastReceivedSeq)
	}
	if len(r3.DrainedMessages) != 0 {
		t.Errorf("seq=3 drained=%v want empty (no buffer)", r3.DrainedMessages)
	}

	// Resync brings 4 → contiguous, drains buffered 5.
	r4, _ := svc.Apply(ctx, frame(4))
	if r4.Outcome != viewsync.ApplyOutcomeContiguous {
		t.Fatalf("seq=4 outcome=%q want contiguous", r4.Outcome)
	}
	if int64(r4.LastReceivedSeq) != 5 {
		t.Errorf("seq=4 cursor=%d want 5 (drain 5)", r4.LastReceivedSeq)
	}
	if got := seqsOf(r4.DrainedMessages); !equalSeqs(got, []viewsync.Seq{4, 5}) {
		t.Errorf("seq=4 drained=%v want [4 5]", got)
	}

	// Concatenate fan-out the gateway would emit, in Apply order:
	// r5 (gap, no fan-out), r3 [3], r4 [4, 5] → 3, 4, 5.
	var faned []viewsync.Seq
	for _, df := range r3.DrainedMessages {
		faned = append(faned, df.Seq)
	}
	if r3.Outcome == viewsync.ApplyOutcomeContiguous && len(r3.DrainedMessages) == 0 {
		faned = append(faned, 3) // gateway fan-outs current frame
	}
	for _, df := range r4.DrainedMessages {
		faned = append(faned, df.Seq)
	}
	if !equalSeqs(faned, []viewsync.Seq{3, 4, 5}) {
		t.Errorf("fan-out order=%v want [3 4 5]", faned)
	}
}

// seqsOf extracts the seq field from a slice of PushFrames.
func seqsOf(frames []viewsync.PushFrame) []viewsync.Seq {
	out := make([]viewsync.Seq, len(frames))
	for i, f := range frames {
		out[i] = f.Seq
	}
	return out
}

func equalSeqs(a, b []viewsync.Seq) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

func TestRecoverGapsTriggersDurableGap(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	_, _ = svc.Apply(ctx, frame(1))
	_, _ = svc.Apply(ctx, frame(4))

	var gotSince, gotUntil viewsync.Seq
	fake := &fakeResyncer{messages: func(since, until viewsync.Seq) []viewsync.ResyncMessage {
		gotSince, gotUntil = since, until
		return []viewsync.ResyncMessage{
			{Seq: 2, MessageID: msgID(2), Envelope: frame(2).Envelope},
			{Seq: 3, MessageID: msgID(3), Envelope: frame(3).Envelope},
		}
	}}
	svc.SetResyncer(fake)

	if err := svc.RecoverGaps(ctx); err != nil {
		t.Fatalf("RecoverGaps: %v", err)
	}
	if gotSince != 2 || gotUntil != 3 {
		t.Fatalf("gap window=[%d,%d] want [2,3]", gotSince, gotUntil)
	}
	cur, err := svc.Cursor(ctx, "ch-X")
	if err != nil {
		t.Fatal(err)
	}
	if cur != 4 {
		t.Fatalf("cursor=%d want 4", cur)
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
