package platform_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// newSweepingHome assembles a platform.Home whose closure reconciler sweeps on a
// fast ticker, so the AUTOMATIC level backstop (not a manually-driven sweep) can
// be observed within a test.
func newSweepingHome(t *testing.T, every time.Duration) *platform.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "sweep.sqlite")
	ch, err := platform.Open(platform.HomeConfig{
		ChannelID:         closureTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: every,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// terminalExists reports whether a kind=response receiver_unavailable terminal
// for parentID is present in truth right now (no polling — a point-in-time read).
func terminalExists(t *testing.T, ch *platform.Home, parentID message.ID) bool {
	t.Helper()
	rows, err := ch.View().ReadAfterSeq(context.Background(), 0, 1000)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	for _, row := range rows {
		if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == parentID {
			return true
		}
	}
	return false
}

// countTerminals counts kind=response rows whose parent is parentID (idempotency
// assertion: the UNIQUE index must keep this at exactly one across re-scans).
func countTerminals(t *testing.T, ch *platform.Home, parentID message.ID) int {
	t.Helper()
	rows, err := ch.View().ReadAfterSeq(context.Background(), 0, 1000)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	n := 0
	for _, row := range rows {
		if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == parentID {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Test 1 (C7 新测① · 已注销者在飞请求被关): an open request whose receiver has been
// DEREGISTERED (closed forever) is closed by the reconciler level sweep with
// receiver_unavailable — the monotone-fact backstop for a lost dereg edge.
// ---------------------------------------------------------------------------
func TestReconciler_LevelSweep_ClosesDeregisteredReceiver(t *testing.T) {
	// The receiver is admitted, then Removed (deregistered) while holding an
	// inbound open request. Under the C7 拔根 semantics closure is owed ONLY on the
	// monotone dereg fact (not liveness): a registered-but-absent member is left to
	// the deadline reaper, a deregistered one — which can never answer — is closed
	// here. The sweep is automatic (fast ticker) — the test drives NOTHING by hand,
	// proving the backstop is wired into Open, not just callable.
	ch := newSweepingHome(t, 50*time.Millisecond)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	registerActor(t, ch, &workerID, actor.KindAgent)

	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)

	// Deregister the receiver — now closed forever (its callers can never be
	// answered). Remove does not close inbound requests; the reconciler must.
	if err := ch.Remove(context.Background(), workerID); err != nil {
		t.Fatalf("Remove worker: %v", err)
	}

	// No manual sweep: wait for the automatic ticker sweep to close the orphan.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if terminalExists(t, ch, reqID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("automatic level sweep did NOT close the orphan open request of a deregistered receiver")
}

// ---------------------------------------------------------------------------
// Test 2 (C7 新测③ · 注销+边沿竞态无双终态): idempotency — repeated reconciler sweeps
// (and, in production, a dereg death edge racing them) produce no duplicate
// terminal. Both the level scan and the death edge funnel every closure through
// behavior.Respond → the ux_terminal_response_per_request UNIQUE index, so a
// second attempt collides and is rejected — exactly one terminal survives.
// ---------------------------------------------------------------------------
func TestReconciler_Idempotent_NoDuplicateTerminal(t *testing.T) {
	// A very fast ticker sweeps MANY times over the orphan's lifetime. The first
	// sweep closes the deregistered receiver's request; every subsequent sweep
	// re-attempts the same terminal and collides with the UNIQUE index — so exactly
	// one terminal must remain no matter how many times the level sweep runs.
	ch := newSweepingHome(t, 10*time.Millisecond)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	registerActor(t, ch, &workerID, actor.KindAgent)

	// Open request, then deregister the receiver → closed forever, closure owed.
	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)
	if err := ch.Remove(context.Background(), workerID); err != nil {
		t.Fatalf("Remove worker: %v", err)
	}

	// Wait for the first sweep to close it.
	deadline := time.Now().Add(5 * time.Second)
	for !terminalExists(t, ch, reqID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !terminalExists(t, ch, reqID) {
		t.Fatal("level sweep did not close the open request")
	}

	// Let the fast ticker sweep many more times, then assert exactly one terminal.
	time.Sleep(200 * time.Millisecond)
	if n := countTerminals(t, ch, reqID); n != 1 {
		t.Fatalf("idempotency broken: %d terminals for one request, want exactly 1", n)
	}
}
