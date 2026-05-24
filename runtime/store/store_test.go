package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// TestSchema_TablesPresent verifies OpenChannel installs every table.
func TestSchema_TablesPresent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, name := range store.ChannelLocalTables {
		var got string
		err := db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name,
		).Scan(&got)
		if err != nil {
			t.Errorf("table %q missing: %v", name, err)
		}
	}
}

// TestMessageAppend_OutboxRoundTrip verifies Append inserts both the
// messages row and the view_sync_outbox row in a single transaction.
func TestMessageAppend_OutboxRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, channel.ID("ch-1"))

	env := &message.Envelope{
		ID:         "m-1",
		TS:         1000,
		TSReceived: 1100,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "channel.created",
		Payload:    json.RawMessage(`{"ok":true}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:channel-agent"},
	}
	res, err := msgs.Append(ctx, env, klog.FencingTuple{})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if res.Seq != 1 {
		t.Errorf("expected seq=1, got %d", res.Seq)
	}
	if res.Deduped {
		t.Error("first append should not be deduped")
	}

	pending, err := outbox.PendingPage(ctx, 10)
	if err != nil {
		t.Fatalf("PendingPage: %v", err)
	}
	if len(pending) != 1 || pending[0].Seq != viewsync.Seq(1) || pending[0].MessageID != "m-1" {
		t.Fatalf("unexpected outbox: %+v", pending)
	}
	got, ok, err := msgs.FindByID(ctx, channel.ID("ch-1"), "m-1")
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if got.Sender.ID != actor.ActorID("agent:alpha") {
		t.Errorf("sender.id=%q want agent:alpha", got.Sender.ID)
	}
	if got.Sender.Kind != actor.KindAgent {
		t.Errorf("sender.kind=%q want %q", got.Sender.Kind, actor.KindAgent)
	}

	// Append same envelope again → dedupe path, no new outbox row.
	res2, err := msgs.Append(ctx, env, klog.FencingTuple{})
	if err != nil {
		t.Fatalf("append dedupe: %v", err)
	}
	if !res2.Deduped {
		t.Error("second append should be deduped")
	}
	pending2, _ := outbox.PendingPage(ctx, 10)
	if len(pending2) != 1 {
		t.Errorf("dedupe should not add outbox row, got %d", len(pending2))
	}
}

// TestOutbox_MarkPushedAndAck verifies the push status flip + ack GC.
func TestOutbox_MarkPushedAndAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, channel.ID("ch-1"))

	// Insert 3 messages.
	for i := 0; i < 3; i++ {
		env := newSimpleEnvelope(i + 1)
		if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	pending, _ := outbox.PendingPage(ctx, 10)
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending, got %d", len(pending))
	}

	if err := outbox.MarkPushed(ctx, viewsync.Seq(1), 9999); err != nil {
		t.Fatal(err)
	}
	pendingAfterPush, _ := outbox.PendingPage(ctx, 10)
	if len(pendingAfterPush) != 3 {
		t.Errorf("after mark-pushed expected 3 retry-eligible rows, got %d", len(pendingAfterPush))
	}

	if err := outbox.AckUpTo(ctx, viewsync.Seq(2)); err != nil {
		t.Fatal(err)
	}
	hi, _, _ := outbox.HighestSeq(ctx)
	if hi != viewsync.Seq(3) {
		t.Errorf("after ack-up-to=2 expected highest=3, got %d", hi)
	}
}

// TestMessages_PendingDue_FutureMessagesGated covers FIX-T3 scheduler
// scan: only rows with `not_before <= now AND delivered_at IS NULL` are
// returned. MarkDelivered transitions a row out of the scan set.
func TestMessages_PendingDue_FutureMessagesGated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	mkEnv := func(id string, notBefore *int64) *message.Envelope {
		return &message.Envelope{
			ID:         message.ID(id),
			TS:         1000,
			TSReceived: 1000,
			ChannelID:  "ch-1",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Kind:       message.KindEvent,
			Type:       "tick",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"agent:channel-agent"},
			NotBefore:  notBefore,
		}
	}
	nb500 := int64(500)
	nb2000 := int64(2000)
	if _, err := msgs.Append(ctx, mkEnv("m-immediate", nil), klog.FencingTuple{}); err != nil {
		t.Fatal(err)
	}
	if _, err := msgs.Append(ctx, mkEnv("m-past", &nb500), klog.FencingTuple{}); err != nil {
		t.Fatal(err)
	}
	if _, err := msgs.Append(ctx, mkEnv("m-future", &nb2000), klog.FencingTuple{}); err != nil {
		t.Fatal(err)
	}

	// At t=1000: m-immediate has no not_before → not in scan; m-past
	// is due; m-future is not yet due.
	due, err := msgs.PendingDue(ctx, 1000, 10)
	if err != nil {
		t.Fatalf("pending due: %v", err)
	}
	if len(due) != 1 || due[0].ID != "m-past" {
		t.Fatalf("at t=1000 expected [m-past], got %+v", ids(due))
	}

	// MarkDelivered drops m-past out of the scan set.
	if err := msgs.MarkDelivered(ctx, "m-past", 1100); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	due, _ = msgs.PendingDue(ctx, 1000, 10)
	if len(due) != 0 {
		t.Errorf("after MarkDelivered expected empty, got %+v", ids(due))
	}

	// Advance the clock past not_before — m-future becomes due.
	due, _ = msgs.PendingDue(ctx, 2500, 10)
	if len(due) != 1 || due[0].ID != "m-future" {
		t.Errorf("at t=2500 expected [m-future], got %+v", ids(due))
	}

	// MarkDelivered is idempotent: re-calling on an already-delivered
	// row is a no-op (rowsAffected=0) and must not error.
	if err := msgs.MarkDelivered(ctx, "m-past", 9999); err != nil {
		t.Errorf("re-MarkDelivered should be idempotent: %v", err)
	}
	if err := msgs.MarkDelivered(ctx, "missing", 9999); err != nil {
		t.Errorf("MarkDelivered missing row should be no-op: %v", err)
	}

	// Limit clamp: passing 0 falls back to the default page size — sanity
	// check it doesn't panic and still returns the row.
	due, err = msgs.PendingDue(ctx, 2500, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Errorf("limit=0 default expected 1 row, got %d", len(due))
	}
}

func TestMessages_MarkDeliveryErrorKeepsRowRetryable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	env := newSimpleEnvelope(1)
	if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := msgs.MarkDeliveryError(ctx, env.ID, 1200, "worker push failed"); err != nil {
		t.Fatalf("MarkDeliveryError: %v", err)
	}
	got, ok, err := msgs.FindByID(ctx, channel.ID("ch-1"), env.ID)
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if got.DeliveredAt != nil {
		t.Fatalf("DeliveredAt=%v want nil after delivery error", *got.DeliveredAt)
	}
	if got.DeliveryFailedAt == nil || *got.DeliveryFailedAt != 1200 {
		t.Fatalf("DeliveryFailedAt=%v want 1200", got.DeliveryFailedAt)
	}
	if got.LastError != "worker push failed" {
		t.Fatalf("LastError=%q want worker push failed", got.LastError)
	}
	if got.Attempts != 1 {
		t.Fatalf("Attempts=%d want 1", got.Attempts)
	}

	if err := msgs.MarkDelivered(ctx, env.ID, 1300); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	got, ok, err = msgs.FindByID(ctx, channel.ID("ch-1"), env.ID)
	if err != nil || !ok {
		t.Fatalf("FindByID after delivered ok=%v err=%v", ok, err)
	}
	if got.DeliveredAt == nil || *got.DeliveredAt != 1300 {
		t.Fatalf("DeliveredAt=%v want 1300", got.DeliveredAt)
	}
	if got.DeliveryFailedAt != nil {
		t.Fatalf("DeliveryFailedAt=%v want nil after MarkDelivered", *got.DeliveryFailedAt)
	}
	if got.LastError != "" {
		t.Fatalf("LastError=%q want cleared", got.LastError)
	}
}

// TestMessages_LongPendingRequests covers the L1 §6.4 scan filter that
// powers the long-pending scheduler fallback. Each sub-case seeds a row
// the daemon would synthesise a failed terminal for (or skip) and
// asserts the SQL filter behaviour:
//
//   - request with expires_at IN THE FUTURE       → not yet due, skip.
//   - request without expires_at                  → no SLA, skip.
//   - request with expires_at=0 sentinel          → no SLA, skip.
//   - request past expires_at, no response        → return (timeout).
//   - request past expires_at, non-terminal resp  → return (partial does
//     not satisfy The One Law, scheduler still emits the failed terminal).
//   - request past expires_at, terminal resp on disk → already settled,
//     skip (NOT EXISTS guard).
//   - event past expires_at                       → skip (kind!=request).
//
// The limit clamp + monotonic ordering also asserted so the daemon
// integration test can trust the slice order.
func TestMessages_LongPendingRequests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	mkReq := func(id string, expiresAt *int64) *message.Envelope {
		return &message.Envelope{
			ID:         message.ID(id),
			TS:         1000,
			TSReceived: 1000,
			ChannelID:  "ch-1",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Kind:       message.KindRequest,
			Type:       "xhs.publish",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"tool:xhs-adapter"},
			ExpiresAt:  expiresAt,
		}
	}

	pastDeadline := int64(900)
	futureDeadline := int64(2000)
	zeroDeadline := int64(0)

	// Rows the scheduler must return at t=1500.
	if _, err := msgs.Append(ctx, mkReq("req-overdue-orphan", &pastDeadline), klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-overdue-orphan: %v", err)
	}

	// Row with a non-terminal interim response — still counts as
	// overdue because The One Law cares about *terminal* responses.
	partialReq := mkReq("req-overdue-partial", &pastDeadline)
	if _, err := msgs.Append(ctx, partialReq, klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-overdue-partial: %v", err)
	}
	interim := &message.Envelope{
		ID:         "resp-partial",
		TS:         950,
		TSReceived: 950,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindTool, ID: "tool:xhs-adapter"},
		Kind:       message.KindResponse,
		Type:       "xhs.publish",
		Payload:    json.RawMessage(`{"status":"in_progress"}`),
		ParentID:   "req-overdue-partial",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:a"},
	}
	if _, err := msgs.Append(ctx, interim, klog.FencingTuple{}); err != nil {
		t.Fatalf("append interim response: %v", err)
	}

	// Rows the scheduler must skip.
	if _, err := msgs.Append(ctx, mkReq("req-future-deadline", &futureDeadline), klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-future-deadline: %v", err)
	}
	if _, err := msgs.Append(ctx, mkReq("req-no-deadline", nil), klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-no-deadline: %v", err)
	}
	if _, err := msgs.Append(ctx, mkReq("req-zero-deadline", &zeroDeadline), klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-zero-deadline: %v", err)
	}

	// Settled-already row: terminal response sitting on disk → scheduler
	// must skip the parent request even though its expires_at has passed.
	if _, err := msgs.Append(ctx, mkReq("req-settled", &pastDeadline), klog.FencingTuple{}); err != nil {
		t.Fatalf("append req-settled: %v", err)
	}
	terminal := &message.Envelope{
		ID:         "resp-settled-terminal",
		TS:         960,
		TSReceived: 960,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindTool, ID: "tool:xhs-adapter"},
		Kind:       message.KindResponse,
		Type:       "xhs.publish",
		Payload:    json.RawMessage(`{"status":"completed"}`),
		ParentID:   "req-settled",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:a"},
		IsTerminal: true,
	}
	if _, err := msgs.Append(ctx, terminal, klog.FencingTuple{}); err != nil {
		t.Fatalf("append terminal response: %v", err)
	}

	// Event past expires_at must NOT be returned — only requests can
	// be settled by a terminal response.
	if _, err := msgs.Append(ctx, &message.Envelope{
		ID:         "evt-overdue",
		TS:         1000,
		TSReceived: 1000,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "noise.tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:channel-agent"},
		ExpiresAt:  &pastDeadline,
	}, klog.FencingTuple{}); err != nil {
		t.Fatalf("append evt-overdue: %v", err)
	}

	due, err := msgs.LongPendingRequests(ctx, 1500, 64)
	if err != nil {
		t.Fatalf("LongPendingRequests: %v", err)
	}
	gotIDs := ids(due)
	want := []string{"req-overdue-orphan", "req-overdue-partial"}
	if len(gotIDs) != len(want) {
		t.Fatalf("LongPendingRequests at t=1500: got %v want %v", gotIDs, want)
	}
	for i, id := range want {
		if gotIDs[i] != id {
			t.Errorf("[%d] got %s want %s (order matters — seq ASC contract)", i, gotIDs[i], id)
		}
	}

	// Sanity: clamping limit=0 → default 64; passing limit=1 caps to 1.
	if got, err := msgs.LongPendingRequests(ctx, 1500, 0); err != nil {
		t.Fatalf("LongPendingRequests limit=0: %v", err)
	} else if len(got) != 2 {
		t.Errorf("LongPendingRequests limit=0 default expected 2, got %d", len(got))
	}
	if got, err := msgs.LongPendingRequests(ctx, 1500, 1); err != nil {
		t.Fatalf("LongPendingRequests limit=1: %v", err)
	} else if len(got) != 1 {
		t.Errorf("LongPendingRequests limit=1 expected 1 row, got %d", len(got))
	}

	// Before any deadline passes — empty result.
	if got, err := msgs.LongPendingRequests(ctx, 500, 64); err != nil {
		t.Fatalf("LongPendingRequests early: %v", err)
	} else if len(got) != 0 {
		t.Errorf("LongPendingRequests at t=500 expected empty, got %v", ids(got))
	}
}

func TestMessages_ReceiverUnavailableRequests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	areg := store.NewActorRegistry(db)
	for _, rec := range []actorreg.Record{
		{ID: "agent:active", Kind: actor.KindAgent, CreatedAt: 1},
		{ID: "agent:gone", Kind: actor.KindAgent, CreatedAt: 1},
	} {
		if err := areg.Insert(ctx, rec); err != nil {
			t.Fatalf("insert actor %s: %v", rec.ID, err)
		}
	}
	if err := areg.Deregister(ctx, "agent:gone", 2); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	msgs := store.NewMessages(db)
	mkReq := func(id, audience string) *message.Envelope {
		return &message.Envelope{
			ID:         message.ID(id),
			TS:         1000,
			TSReceived: 1000,
			ChannelID:  "ch-1",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Kind:       message.KindRequest,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.ActorID(audience)},
		}
	}
	for _, env := range []*message.Envelope{
		mkReq("req-active", "agent:active"),
		mkReq("req-gone", "agent:gone"),
		mkReq("req-missing", "agent:missing"),
		mkReq("req-settled-missing", "agent:missing"),
	} {
		if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
			t.Fatalf("append %s: %v", env.ID, err)
		}
	}
	terminal := &message.Envelope{
		ID:         "resp-settled-missing",
		TS:         1001,
		TSReceived: 1001,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindResponse,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"status":"failed","reason":"receiver_unavailable"}`),
		ParentID:   "req-settled-missing",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:a"},
		IsTerminal: true,
	}
	if _, err := msgs.Append(ctx, terminal, klog.FencingTuple{}); err != nil {
		t.Fatalf("append terminal: %v", err)
	}
	if _, err := msgs.Append(ctx, &message.Envelope{
		ID:         "evt-missing",
		TS:         1000,
		TSReceived: 1000,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "noise.tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:missing"},
	}, klog.FencingTuple{}); err != nil {
		t.Fatalf("append event: %v", err)
	}

	got, err := msgs.ReceiverUnavailableRequests(ctx, 64)
	if err != nil {
		t.Fatalf("ReceiverUnavailableRequests: %v", err)
	}
	want := []string{"req-gone", "req-missing"}
	if gotIDs := ids(got); !equalStrings(gotIDs, want) {
		t.Fatalf("ReceiverUnavailableRequests got %v want %v", gotIDs, want)
	}
}

func TestMessages_ConcurrentTerminalDuplicateClassified(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	parentID := "req-terminal-race"
	results := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, id := range []string{"resp-race-a", "resp-race-b"} {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			<-start
			_, err := msgs.Append(ctx, &message.Envelope{
				ID:         message.ID(id),
				TS:         int64(1000 + i),
				TSReceived: int64(1000 + i),
				ChannelID:  "ch-1",
				Sender:     message.Sender{Kind: actor.KindTool, ID: "tool:xhs-adapter"},
				Kind:       message.KindResponse,
				Type:       "xhs.publish",
				Payload:    json.RawMessage(`{"status":"completed"}`),
				ParentID:   message.ID(parentID),
				Visibility: message.VisibilityPublic,
				Audience:   message.Audience{"agent:a"},
				IsTerminal: true,
			}, klog.FencingTuple{})
			results <- err
		}(i, id)
	}
	close(start)
	wg.Wait()
	close(results)

	var accepted, duplicates int
	for err := range results {
		if err == nil {
			accepted++
			continue
		}
		var appendErr *klog.AppendError
		if !errors.As(err, &appendErr) {
			t.Fatalf("loser error type=%T value=%v, want *AppendError", err, err)
		}
		if appendErr.Reason != message.HarnessTerminalDuplicate {
			t.Fatalf("append reason=%s want %s detail=%s", appendErr.Reason, message.HarnessTerminalDuplicate, appendErr.Detail)
		}
		duplicates++
	}
	if accepted != 1 || duplicates != 1 {
		t.Fatalf("accepted=%d duplicates=%d want 1/1", accepted, duplicates)
	}
}

// ids extracts envelope ids in slice order for diff-friendly assertions.
func ids(rows []message.Envelope) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.ID)
	}
	return out
}

func equalStrings(a, b []string) bool {
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

func newSimpleEnvelope(seq int) *message.Envelope {
	return &message.Envelope{
		ID:         message.ID("m-" + itoa(seq)),
		TS:         int64(1000 + seq),
		TSReceived: int64(1000 + seq),
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:channel-agent"},
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestActorRegistry verifies Insert + cursor seed + Lookup + Deregister.
func TestActorRegistry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	reg := store.NewActorRegistry(db)
	cur := store.NewCursors(db)

	rec := actorreg.Record{
		ID:        "agent:alpha",
		Kind:      actor.KindAgent,
		Binding:   actor.BindingEmbedded,
		CreatedAt: 1000,
	}
	if err := reg.Insert(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, ok, err := reg.Lookup(ctx, "agent:alpha")
	if err != nil || !ok {
		t.Fatalf("lookup ok=%v err=%v", ok, err)
	}
	if got.ID != actor.ActorID("agent:alpha") {
		t.Errorf("actor id=%q want agent:alpha", got.ID)
	}
	if got.Kind != actor.KindAgent {
		t.Errorf("actor kind=%q want %q", got.Kind, actor.KindAgent)
	}
	if got.Binding != actor.BindingEmbedded || !got.IsActive() {
		t.Errorf("got=%+v", got)
	}

	// Cursor was seeded in same tx.
	c, ok, err := cur.Get(ctx, "agent:alpha")
	if err != nil || !ok {
		t.Fatalf("cursor get ok=%v err=%v", ok, err)
	}
	if c.LastConsumedSeq != 0 {
		t.Errorf("expected cursor seq=0, got %d", c.LastConsumedSeq)
	}

	// Advance is monotonic.
	if ok, err := cur.Advance(ctx, "agent:alpha", klog.Seq(5), "m-5", 2000); err != nil || !ok {
		t.Fatalf("advance ok=%v err=%v", ok, err)
	}
	if ok, err := cur.Advance(ctx, "agent:alpha", klog.Seq(3), "m-3", 2100); err != nil || ok {
		t.Fatalf("non-monotonic advance should fail: ok=%v err=%v", ok, err)
	}

	// Deregister.
	if err := reg.Deregister(ctx, "agent:alpha", 9000); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	got, ok, err = reg.Lookup(ctx, "agent:alpha")
	if err != nil || !ok {
		t.Fatalf("lookup after deregister: ok=%v err=%v", ok, err)
	}
	if got.IsActive() {
		t.Error("should be inactive after deregister")
	}

	// ListActive excludes deregistered.
	active, err := reg.ListActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active, got %d", len(active))
	}
}

// TestActorRegistry_AllBindings_RoundTrip writes each binding wire
// value (proto-foundation §2.5.1 closed set) and verifies the schema
// CHECK constraint accepts it + Lookup returns the same canonical form.
func TestActorRegistry_AllBindings_RoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	reg := store.NewActorRegistry(db)
	cases := []struct {
		id      actor.ActorID
		binding actor.Binding
	}{
		{"tool:emb", actor.BindingEmbedded},
		{"tool:out", actor.BindingRuntimeOutbound},
		{"tool:relay", actor.BindingRuntimeInboundViaRelay},
	}
	for _, tc := range cases {
		if err := reg.Insert(ctx, actorreg.Record{
			ID:        tc.id,
			Kind:      actor.KindTool,
			Binding:   tc.binding,
			CreatedAt: 1000,
		}); err != nil {
			t.Fatalf("Insert(%s, %s): %v", tc.id, tc.binding, err)
		}
		got, ok, err := reg.Lookup(ctx, tc.id)
		if err != nil || !ok {
			t.Fatalf("Lookup(%s): ok=%v err=%v", tc.id, ok, err)
		}
		if got.Binding != tc.binding {
			t.Errorf("Binding round-trip for %s: got=%q want=%q", tc.id, got.Binding, tc.binding)
		}
	}
}

// TestLedger_ReserveCommit verifies idempotent reserve + commit.
func TestLedger_ReserveCommit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	l := store.NewLedger(db)
	key, err := ledger.DeriveKey("turn-1", "act-1")
	if err != nil {
		t.Fatal(err)
	}

	entry := ledger.Entry{
		Key:        key,
		TurnID:     "turn-1",
		ActorID:    "agent:alpha",
		EnvelopeID: "env-1",
		Status:     ledger.StatusReserved,
		ReservedAt: 1000,
	}
	got, err := l.Reserve(ctx, entry)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got.EnvelopeID != "env-1" {
		t.Errorf("got envelopeID=%q", got.EnvelopeID)
	}

	// Reserve again with different envelope_id → existing returned.
	again, err := l.Reserve(ctx, ledger.Entry{
		Key:        key,
		TurnID:     "turn-1",
		ActorID:    "agent:alpha",
		EnvelopeID: "env-different",
		Status:     ledger.StatusReserved,
		ReservedAt: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.EnvelopeID != "env-1" {
		t.Errorf("idempotent reserve should return original envelope_id, got %q", again.EnvelopeID)
	}

	// Commit.
	if err := l.Commit(ctx, key, 3000); err != nil {
		t.Fatalf("commit: %v", err)
	}
	final, ok, _ := l.Find(ctx, key)
	if !ok || final.Status != ledger.StatusCommitted || final.CommittedAt != 3000 {
		t.Errorf("post-commit final=%+v ok=%v", final, ok)
	}

	// Commit again — idempotent.
	if err := l.Commit(ctx, key, 4000); err != nil {
		t.Errorf("idempotent commit failed: %v", err)
	}
}

// TestChannelLock verifies insert + upgrade + refresh + validate-write.
func TestChannelLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	lock := store.NewChannelLock(db)

	row := store.ChannelLockRow{
		ChannelID:    "ch-1",
		FencingToken: placement.FencingToken("tok-1"),
		OwnerEpoch:   placement.OwnerEpoch(1),
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(1),
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}
	if err := lock.Insert(ctx, row); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := lock.ValidateWrite(ctx, placement.FencingToken("tok-1"), placement.DaemonEpoch(1)); err != nil {
		t.Errorf("ValidateWrite should pass: %v", err)
	}
	if err := lock.ValidateWrite(ctx, placement.FencingToken("tok-2"), placement.DaemonEpoch(1)); err == nil {
		t.Error("ValidateWrite should fail on fencing mismatch")
	}
	if err := lock.ValidateWrite(ctx, placement.FencingToken("tok-1"), placement.DaemonEpoch(2)); err == nil {
		t.Error("ValidateWrite should fail on daemon_epoch mismatch")
	}

	// RefreshDaemon bumps only daemon_epoch.
	if err := lock.RefreshDaemon(ctx, placement.DaemonEpoch(7), 3000); err != nil {
		t.Fatal(err)
	}
	got, _, _ := lock.Get(ctx)
	if got.DaemonEpoch != 7 || got.FencingToken != "tok-1" {
		t.Errorf("after refresh got=%+v", got)
	}
}
