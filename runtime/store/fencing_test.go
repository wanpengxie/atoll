package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// fencedFixture spins up a fresh channel sqlite with a seeded
// channel_lock row and the WithLock variants of Messages + Ledger. The
// shared (token, epoch) tuple is the one a non-stale daemon is supposed
// to present.
type fencedFixture struct {
	db    *sql.DB
	lock  *store.ChannelLock
	msgs  *store.Messages
	led   *store.Ledger
	token placement.FencingToken
	epoch placement.DaemonEpoch
}

func newFencedFixture(t *testing.T, token placement.FencingToken, epoch placement.DaemonEpoch) *fencedFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
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
		DaemonEpoch:  epoch,
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}); err != nil {
		t.Fatalf("seed channel_lock: %v", err)
	}

	return &fencedFixture{
		db:    db,
		lock:  lock,
		msgs:  store.NewMessagesWithLock(db, lock),
		led:   store.NewLedgerWithLock(db, lock),
		token: token,
		epoch: epoch,
	}
}

func newFencedEnvelope(id string) *message.Envelope {
	return &message.Envelope{
		ID:         message.ID(id),
		TS:         1000,
		TSReceived: 1100,
		ChannelID:  "ch-fence",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "channel.created",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"*"},
	}
}

func TestChannelLockTakeoverRotatesTupleWithoutInsert(t *testing.T) {
	t.Parallel()
	fx := newFencedFixture(t, "tok-old", 1)
	ctx := context.Background()

	if err := fx.lock.Takeover(ctx, store.ChannelLockRow{
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
	row, ok, err := fx.lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v", ok, err)
	}
	if row.FencingToken != "tok-new" || row.OwnerEpoch != 2 || row.DaemonID != "daemon-B" || row.DaemonEpoch != 9 {
		t.Fatalf("row after takeover=%+v", row)
	}
	var count int
	if err := fx.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_lock`).Scan(&count); err != nil {
		t.Fatalf("count channel_lock: %v", err)
	}
	if count != 1 {
		t.Fatalf("channel_lock rows=%d want 1", count)
	}
	if err := fx.lock.Takeover(ctx, row, 1); err == nil {
		t.Fatalf("stale takeover CAS succeeded")
	}
}

// outboxRowCount returns the number of view_sync_outbox rows — used to
// prove the outbox is NOT polluted on a fencing-reject path (L1 §8.6).
func outboxRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM view_sync_outbox`).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func messagesRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&n); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	return n
}

func ledgerRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM action_ledger`).Scan(&n); err != nil {
		t.Fatalf("count action_ledger: %v", err)
	}
	return n
}

// TestMessages_FencingEnforced is the FIX-T6 reject table for Append.
// Every row exercises one branch of the fencing gate.
func TestMessages_FencingEnforced(t *testing.T) {
	cases := []struct {
		name       string
		token      placement.FencingToken
		epoch      placement.DaemonEpoch
		supply     bool
		wantReject bool
		wantReason message.HarnessRejectReason
	}{
		{
			name:   "match — accept",
			token:  placement.FencingToken("tok-7"),
			epoch:  placement.DaemonEpoch(3),
			supply: true,
		},
		{
			name:       "stale token — caller below lock",
			token:      placement.FencingToken("tok-6"), // lock=7
			epoch:      placement.DaemonEpoch(3),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "stale token — caller above lock (newer worker view)",
			token:      placement.FencingToken("tok-8"),
			epoch:      placement.DaemonEpoch(3),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "daemon_epoch mismatch — stale daemon",
			token:      placement.FencingToken("tok-7"),
			epoch:      placement.DaemonEpoch(99),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "missing fencing — bare append rejected",
			token:      placement.FencingToken("tok-7"),
			epoch:      placement.DaemonEpoch(3),
			supply:     false,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFencedFixture(t, placement.FencingToken("tok-7"), placement.DaemonEpoch(3))
			ctx := context.Background()
			var fencing klog.FencingTuple
			if tc.supply {
				fencing = klog.FencingTuple{Token: tc.token, Epoch: tc.epoch}
			}

			env := newFencedEnvelope("m-" + tc.name)
			_, err := fx.msgs.Append(ctx, env, fencing)

			if tc.wantReject {
				var appErr *klog.AppendError
				if !errors.As(err, &appErr) {
					t.Fatalf("expected *AppendError, got %T: %v", err, err)
				}
				if appErr.Reason != tc.wantReason {
					t.Fatalf("reason got=%q want=%q", appErr.Reason, tc.wantReason)
				}
				if n := messagesRowCount(t, fx.db); n != 0 {
					t.Errorf("messages row written despite reject: %d", n)
				}
				if n := outboxRowCount(t, fx.db); n != 0 {
					t.Errorf("view_sync_outbox row written despite reject: %d", n)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected accept, got err: %v", err)
			}
			if n := messagesRowCount(t, fx.db); n != 1 {
				t.Errorf("messages rowcount = %d, want 1", n)
			}
			if n := outboxRowCount(t, fx.db); n != 1 {
				t.Errorf("outbox rowcount = %d, want 1", n)
			}
		})
	}
}

// TestLedger_FencingEnforced mirrors TestMessages_FencingEnforced for
// Reserve + Commit. A stale daemon must not be able to reserve nor flip
// status.
func TestLedger_FencingEnforced(t *testing.T) {
	ctx := context.Background()
	fx := newFencedFixture(t, placement.FencingToken("tok-7"), placement.DaemonEpoch(3))

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

	// Reserve — stale fencing tuple → reject, no row.
	staleCtx := store.CtxWithFencing(ctx, placement.FencingToken("tok-6"), placement.DaemonEpoch(3))
	if _, err := fx.led.Reserve(staleCtx, entry); err == nil {
		t.Fatal("Reserve with stale token: expected error, got nil")
	} else if !store.IsFencingStale(err) {
		t.Errorf("expected FencingStaleError, got %v", err)
	}
	if n := ledgerRowCount(t, fx.db); n != 0 {
		t.Errorf("ledger rowcount after stale reserve = %d, want 0", n)
	}

	// Reserve — missing ctx → reject.
	if _, err := fx.led.Reserve(ctx, entry); err == nil {
		t.Fatal("Reserve without fencing ctx: expected error, got nil")
	} else if !store.IsFencingStale(err) {
		t.Errorf("expected FencingStaleError, got %v", err)
	}

	// Reserve — matching tuple → ok.
	goodCtx := store.CtxWithFencing(ctx, fx.token, fx.epoch)
	if _, err := fx.led.Reserve(goodCtx, entry); err != nil {
		t.Fatalf("Reserve match: %v", err)
	}
	if n := ledgerRowCount(t, fx.db); n != 1 {
		t.Errorf("ledger rowcount after good reserve = %d, want 1", n)
	}

	// Commit — stale daemon_epoch → reject.
	staleEpochCtx := store.CtxWithFencing(ctx, fx.token, placement.DaemonEpoch(99))
	if err := fx.led.Commit(staleEpochCtx, key, 2000); err == nil {
		t.Fatal("Commit stale epoch: expected error")
	} else if !store.IsFencingStale(err) {
		t.Errorf("expected FencingStaleError, got %v", err)
	}

	// Commit — match → ok + status flipped to committed.
	if err := fx.led.Commit(goodCtx, key, 2000); err != nil {
		t.Fatalf("Commit match: %v", err)
	}
	got, ok, err := fx.led.Find(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Find post-commit ok=%v err=%v", ok, err)
	}
	if got.Status != ledger.StatusCommitted {
		t.Errorf("post-commit status=%q want=%q", got.Status, ledger.StatusCommitted)
	}
}

// TestChannelLock_ValidateWriteTx covers the in-tx fencing gate in
// isolation. Mirrors store_test.go TestChannelLock but exercises the
// FIX-T6 tx-aware variant.
func TestChannelLock_ValidateWriteTx(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	lock := store.NewChannelLock(db)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    "ch-1",
		FencingToken: placement.FencingToken("tok-5"),
		OwnerEpoch:   placement.OwnerEpoch(1),
		DaemonID:     "daemon-A",
		DaemonEpoch:  placement.DaemonEpoch(2),
		AcquiredAt:   1000,
		RefreshedAt:  1000,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	runInTx := func(token placement.FencingToken, epoch placement.DaemonEpoch) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		return lock.ValidateWriteTx(ctx, tx, token, epoch)
	}

	cases := []struct {
		name    string
		token   placement.FencingToken
		epoch   placement.DaemonEpoch
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
				if !store.IsFencingStale(err) {
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
		db, err := store.OpenChannel(ctx, filepath.Join(dir, "empty.sqlite"), store.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		emptyLock := store.NewChannelLock(db)
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()
		err = emptyLock.ValidateWriteTx(ctx, tx, placement.FencingToken("tok-5"), placement.DaemonEpoch(2))
		if !store.IsFencingStale(err) {
			t.Errorf("missing row: want FencingStaleError, got %v", err)
		}
	})

	// Nil tx → error path.
	t.Run("nil tx", func(t *testing.T) {
		if err := lock.ValidateWriteTx(ctx, nil, placement.FencingToken("tok-5"), placement.DaemonEpoch(2)); err == nil {
			t.Fatal("nil tx: expected error")
		}
	})
}

// TestMessages_FencingDedupeUnchanged guards that the dedupe branch
// still works once fencing is enforced — a retry with the same envelope
// id should return Deduped=true (not a fencing reject).
func TestMessages_FencingDedupeUnchanged(t *testing.T) {
	fx := newFencedFixture(t, placement.FencingToken("tok-7"), placement.DaemonEpoch(3))
	ctx := context.Background()
	fencing := klog.FencingTuple{Token: fx.token, Epoch: fx.epoch}

	env := newFencedEnvelope("m-dedupe")
	res1, err := fx.msgs.Append(ctx, env, fencing)
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if res1.Deduped {
		t.Error("first append should not be deduped")
	}

	res2, err := fx.msgs.Append(ctx, env, fencing)
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if !res2.Deduped {
		t.Error("second append should be deduped")
	}
	if n := outboxRowCount(t, fx.db); n != 1 {
		t.Errorf("dedupe must not add outbox row, got %d", n)
	}
}
