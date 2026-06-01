package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/fencing"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// fencedFixture spins up a fresh channel sqlite and wires Messages + Ledger
// with a PURE fake fence (accepts only the seeded tuple) + a recording
// append observer. The framework-side ChannelLock / ViewSyncOutbox are
// tested in framework/multiuser/runtime/store; here the subject is the
// substrate fencing GATE: that Messages/Ledger consult the injected fence
// and only invoke the observer on a committed append.
type fencedFixture struct {
	db    *sql.DB
	msgs  *store.Messages
	led   *store.Ledger
	token fencing.FencingToken
	epoch fencing.DaemonEpoch
	enq   *int // observer EnqueueAppendTx call count
}

func newFencedFixture(t *testing.T, token fencing.FencingToken, epoch fencing.DaemonEpoch) *fencedFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	fence := fakeFence(token, epoch)
	enq := new(int)
	obs := store.AppendObserverFuncs{
		Enqueue: func(context.Context, *sql.Tx, *message.Envelope, int64) error { *enq++; return nil },
	}
	return &fencedFixture{
		db:    db,
		msgs:  store.NewMessagesWithObservers(db, fence, obs),
		led:   store.NewLedgerWithLock(db, fence),
		token: token,
		epoch: epoch,
		enq:   enq,
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
		Audience:   message.Audience{"agent:channel-agent"},
	}
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
// Every row exercises one branch of the fencing gate. The append observer
// must NOT be invoked on a rejected (un-committed) append.
func TestMessages_FencingEnforced(t *testing.T) {
	cases := []struct {
		name       string
		token      fencing.FencingToken
		epoch      fencing.DaemonEpoch
		supply     bool
		wantReject bool
		wantReason message.HarnessRejectReason
	}{
		{
			name:   "match — accept",
			token:  fencing.FencingToken("tok-7"),
			epoch:  fencing.DaemonEpoch(3),
			supply: true,
		},
		{
			name:       "stale token — caller below lock",
			token:      fencing.FencingToken("tok-6"), // lock=7
			epoch:      fencing.DaemonEpoch(3),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "stale token — caller above lock (newer worker view)",
			token:      fencing.FencingToken("tok-8"),
			epoch:      fencing.DaemonEpoch(3),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "daemon_epoch mismatch — stale daemon",
			token:      fencing.FencingToken("tok-7"),
			epoch:      fencing.DaemonEpoch(99),
			supply:     true,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
		{
			name:       "missing fencing — bare append rejected",
			token:      fencing.FencingToken("tok-7"),
			epoch:      fencing.DaemonEpoch(3),
			supply:     false,
			wantReject: true,
			wantReason: message.HarnessWorkerFencingStale,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFencedFixture(t, fencing.FencingToken("tok-7"), fencing.DaemonEpoch(3))
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
				if *fx.enq != 0 {
					t.Errorf("append observer invoked despite reject: %d", *fx.enq)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected accept, got err: %v", err)
			}
			if n := messagesRowCount(t, fx.db); n != 1 {
				t.Errorf("messages rowcount = %d, want 1", n)
			}
			if *fx.enq != 1 {
				t.Errorf("append observer call count = %d, want 1", *fx.enq)
			}
		})
	}
}

// TestLedger_FencingEnforced mirrors TestMessages_FencingEnforced for
// Reserve + Commit. A stale daemon must not be able to reserve nor flip
// status.
func TestLedger_FencingEnforced(t *testing.T) {
	ctx := context.Background()
	fx := newFencedFixture(t, fencing.FencingToken("tok-7"), fencing.DaemonEpoch(3))

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
	staleCtx := store.CtxWithFencing(ctx, fencing.FencingToken("tok-6"), fencing.DaemonEpoch(3))
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
	staleEpochCtx := store.CtxWithFencing(ctx, fx.token, fencing.DaemonEpoch(99))
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

// TestMessages_FencingDedupeUnchanged guards that the dedupe branch
// still works once fencing is enforced — a retry with the same envelope
// id should return Deduped=true (not a fencing reject) and must NOT
// re-invoke the append observer.
func TestMessages_FencingDedupeUnchanged(t *testing.T) {
	fx := newFencedFixture(t, fencing.FencingToken("tok-7"), fencing.DaemonEpoch(3))
	ctx := context.Background()
	tuple := klog.FencingTuple{Token: fx.token, Epoch: fx.epoch}

	env := newFencedEnvelope("m-dedupe")
	res1, err := fx.msgs.Append(ctx, env, tuple)
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if res1.Deduped {
		t.Error("first append should not be deduped")
	}

	res2, err := fx.msgs.Append(ctx, env, tuple)
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if !res2.Deduped {
		t.Error("second append should be deduped")
	}
	if *fx.enq != 1 {
		t.Errorf("dedupe must not re-invoke observer, got %d calls", *fx.enq)
	}
}
