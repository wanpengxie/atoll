package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/ledger"
	klog "github.com/coagent-ai/coagent/kernel/log"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/kernel/viewsync"
	"github.com/coagent-ai/coagent/runtime/store"
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
		Sender:     message.Sender{Kind: message.SenderAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "channel.created",
		Payload:    json.RawMessage(`{"ok":true}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
	}
	res, err := msgs.Append(ctx, env)
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

	// Append same envelope again → dedupe path, no new outbox row.
	res2, err := msgs.Append(ctx, env)
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
		if _, err := msgs.Append(ctx, env); err != nil {
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
	if len(pendingAfterPush) != 2 {
		t.Errorf("after mark-pushed expected 2 pending, got %d", len(pendingAfterPush))
	}

	if err := outbox.AckUpTo(ctx, viewsync.Seq(2)); err != nil {
		t.Fatal(err)
	}
	hi, _, _ := outbox.HighestSeq(ctx)
	if hi != viewsync.Seq(3) {
		t.Errorf("after ack-up-to=2 expected highest=3, got %d", hi)
	}
}

func newSimpleEnvelope(seq int) *message.Envelope {
	return &message.Envelope{
		ID:         "m-" + itoa(seq),
		TS:         int64(1000 + seq),
		TSReceived: int64(1000 + seq),
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: message.SenderAgent, ID: "agent:a"},
		Kind:       message.KindEvent,
		Type:       "tick",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
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

	rec := actor.Record{
		ID:        "agent:alpha",
		Kind:      message.SenderAgent,
		Binding:   actor.BindingInProcess,
		CreatedAt: 1000,
	}
	if err := reg.Insert(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, ok, err := reg.Lookup(ctx, "agent:alpha")
	if err != nil || !ok {
		t.Fatalf("lookup ok=%v err=%v", ok, err)
	}
	if got.Binding != actor.BindingInProcess || !got.IsActive() {
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
		FencingToken: placement.FencingToken(1),
		OwnerEpoch:   placement.OwnerEpoch(1),
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(1),
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}
	if err := lock.Insert(ctx, row); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := lock.ValidateWrite(ctx, placement.FencingToken(1), placement.DaemonEpoch(1)); err != nil {
		t.Errorf("ValidateWrite should pass: %v", err)
	}
	if err := lock.ValidateWrite(ctx, placement.FencingToken(2), placement.DaemonEpoch(1)); err == nil {
		t.Error("ValidateWrite should fail on fencing mismatch")
	}
	if err := lock.ValidateWrite(ctx, placement.FencingToken(1), placement.DaemonEpoch(2)); err == nil {
		t.Error("ValidateWrite should fail on daemon_epoch mismatch")
	}

	// Upgrade epoch.
	ok, err := lock.UpgradeEpoch(ctx,
		placement.FencingToken(2), placement.OwnerEpoch(2),
		placement.DaemonID("daemon-B"), placement.DaemonEpoch(5), 2000)
	if err != nil || !ok {
		t.Fatalf("UpgradeEpoch: ok=%v err=%v", ok, err)
	}
	// Same token should be CAS-rejected.
	ok2, _ := lock.UpgradeEpoch(ctx,
		placement.FencingToken(2), placement.OwnerEpoch(3),
		placement.DaemonID("daemon-C"), placement.DaemonEpoch(6), 2100)
	if ok2 {
		t.Error("UpgradeEpoch CAS should reject same-token write")
	}

	// RefreshDaemon bumps only daemon_epoch.
	if err := lock.RefreshDaemon(ctx, placement.DaemonEpoch(7), 3000); err != nil {
		t.Fatal(err)
	}
	got, _, _ := lock.Get(ctx)
	if got.DaemonEpoch != 7 || got.FencingToken != 2 {
		t.Errorf("after refresh got=%+v", got)
	}
}
