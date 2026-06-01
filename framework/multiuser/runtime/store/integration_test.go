package store_test

// Integration tests whose SUBJECT is the release-specific multiuser store
// surface — the daemon-side fencing mirror (ChannelLock) and the view-sync
// OUTBOX (ViewSyncOutbox) — wired against the pure substrate runtime/store
// (imported as basestore). These previously lived in runtime/store/*_test.go
// and forced the reusable substrate's test suite to import this framework
// layer UPWARD; they belong here, next to the concrete implementations.

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	store "github.com/wanpengxie/ActOS/framework/multiuser/runtime/store"
	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/fencing"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	basestore "github.com/wanpengxie/ActOS/runtime/store"
)

// newLockedChannel opens a fresh channel sqlite and seeds a channel_lock row.
func newLockedChannel(t *testing.T, token fencing.FencingToken, daemonEpoch fencing.DaemonEpoch) (*sql.DB, *store.ChannelLock) {
	t.Helper()
	ctx := context.Background()
	db, err := basestore.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), basestore.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	lock := store.NewChannelLock(db)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    "ch-fence",
		FencingToken: token,
		OwnerEpoch:   placement.OwnerEpoch(1),
		DaemonID:     "daemon-A",
		DaemonEpoch:  daemonEpoch,
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}); err != nil {
		t.Fatalf("seed channel_lock: %v", err)
	}
	return db, lock
}

// TestChannelLock verifies insert + upgrade + refresh + validate-write.
func TestChannelLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := basestore.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), basestore.OpenOptions{})
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

// TestChannelLock_ValidateWriteTx covers the in-tx fencing gate in isolation.
func TestChannelLock_ValidateWriteTx(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := basestore.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), basestore.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	lock := store.NewChannelLock(db)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    "ch-1",
		FencingToken: fencing.FencingToken("tok-5"),
		OwnerEpoch:   placement.OwnerEpoch(1),
		DaemonID:     "daemon-A",
		DaemonEpoch:  fencing.DaemonEpoch(2),
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	runInTx := func(token fencing.FencingToken, epoch fencing.DaemonEpoch) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		return lock.ValidateWriteTx(ctx, tx, token, epoch)
	}

	cases := []struct {
		name    string
		token   fencing.FencingToken
		epoch   fencing.DaemonEpoch
		wantErr bool
	}{
		{"match", "tok-5", 2, false},
		{"token mismatch (different value 'lower')", "tok-4", 2, true},
		{"token mismatch (different value 'higher')", "tok-6", 2, true},
		{"epoch lower", "tok-5", 1, true},
		{"epoch higher", "tok-5", 3, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runInTx(tc.token, tc.epoch)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !basestore.IsFencingStale(err) {
					t.Errorf("expected FencingStaleError, got %v", err)
				}
			} else if err != nil {
				t.Errorf("expected ok, got %v", err)
			}
		})
	}

	// Lock row missing → also stale.
	t.Run("lock row missing", func(t *testing.T) {
		dir := t.TempDir()
		db, err := basestore.OpenChannel(ctx, filepath.Join(dir, "empty.sqlite"), basestore.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		emptyLock := store.NewChannelLock(db)
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()
		err = emptyLock.ValidateWriteTx(ctx, tx, fencing.FencingToken("tok-5"), fencing.DaemonEpoch(2))
		if !basestore.IsFencingStale(err) {
			t.Errorf("missing row: want FencingStaleError, got %v", err)
		}
	})

	// Nil tx → error path.
	t.Run("nil tx", func(t *testing.T) {
		if err := lock.ValidateWriteTx(ctx, nil, fencing.FencingToken("tok-5"), fencing.DaemonEpoch(2)); err == nil {
			t.Fatal("nil tx: expected error")
		}
	})
}

func TestChannelLockTakeoverRotatesTupleWithoutInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, lock := newLockedChannel(t, "tok-old", 1)

	if err := lock.Takeover(ctx, store.ChannelLockRow{
		ChannelID:    "ch-fence",
		FencingToken: "tok-new",
		OwnerEpoch:   2,
		DaemonID:     "daemon-B",
		DaemonEpoch:  9,
		AcquiredAt:   2000,
		RefreshedAt:  2000,
	}, 1); err != nil {
		t.Fatalf("Takeover: %v", err)
	}
	row, ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v", ok, err)
	}
	if row.FencingToken != "tok-new" || row.OwnerEpoch != 2 || row.DaemonID != "daemon-B" || row.DaemonEpoch != 9 {
		t.Fatalf("row after takeover=%+v", row)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_lock`).Scan(&count); err != nil {
		t.Fatalf("count channel_lock: %v", err)
	}
	if count != 1 {
		t.Fatalf("channel_lock rows=%d want 1", count)
	}
	if err := lock.Takeover(ctx, row, 1); err == nil {
		t.Fatalf("stale takeover CAS succeeded")
	}
}

// TestMessageAppend_OutboxRoundTrip verifies substrate Append inserts both
// the messages row and the framework view_sync_outbox row in one tx.
func TestMessageAppend_OutboxRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := basestore.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), basestore.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := store.NewViewSyncOutbox(db, channel.ID("ch-1"))
	msgs := basestore.NewMessagesWithObservers(db, nil, outbox)
	note := "source thread"
	crossRefs := []message.CrossChannelRef{{ChannelID: "ch-remote", MessageID: "msg-source", Note: &note}}

	env := &message.Envelope{
		ID:               "m-1",
		TS:               1000,
		TSReceived:       1100,
		ChannelID:        "ch-1",
		Sender:           message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:             message.KindEvent,
		Type:             "channel.created",
		Payload:          json.RawMessage(`{"ok":true}`),
		Visibility:       message.VisibilityPublic,
		Audience:         message.Audience{"agent:channel-agent"},
		CrossChannelRefs: &crossRefs,
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
	if got.CrossChannelRefs == nil || !reflect.DeepEqual(*got.CrossChannelRefs, crossRefs) {
		t.Errorf("cross_channel_refs=%+v want %+v", got.CrossChannelRefs, crossRefs)
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
	db, err := basestore.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), basestore.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	outbox := store.NewViewSyncOutbox(db, channel.ID("ch-1"))
	msgs := basestore.NewMessagesWithObservers(db, nil, outbox)

	// Insert 3 messages.
	for i := 0; i < 3; i++ {
		env := &message.Envelope{
			ID:         message.ID("m-" + strconv.Itoa(i+1)),
			TS:         int64(1000 + i + 1),
			TSReceived: int64(1000 + i + 1),
			ChannelID:  "ch-1",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Kind:       message.KindEvent,
			Type:       "tick",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"agent:channel-agent"},
		}
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
