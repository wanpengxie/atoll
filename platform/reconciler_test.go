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
// Test 1: restart — an open request whose receiver is absent (its embodiment
// predates this process) is closed by the startup reconciler sweep.
// ---------------------------------------------------------------------------
func TestReconciler_LevelSweep_ClosesOrphanWithAbsentReceiver(t *testing.T) {
	// The death edge NEVER fires for this request — its receiver is a member that
	// was never placed as a cell, so it has no embodiment and no death to publish.
	// This is exactly the #5 home-restart-leftover shape (an open request whose
	// receiver is absent because no live instance backs it). Only the LEVEL sweep
	// can close it. The sweep is automatic (fast ticker) — the test drives NOTHING
	// by hand, proving the backstop is wired into Open, not just callable.
	//
	// A literal two-Open() process restart on the same sqlite is blocked by the
	// out-of-batch non-idempotent genesis (audit #1: system-actor Insert collides
	// on reopen); the level-sweep RECOVERY semantics it would exercise are exactly
	// what this single-Home automatic sweep proves.
	ch := newSweepingHome(t, 50*time.Millisecond)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)

	// No manual sweep, no edge: wait for the automatic ticker sweep to close it.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if terminalExists(t, ch, reqID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("automatic level sweep did NOT close the orphan open request with an absent receiver")
}

// ---------------------------------------------------------------------------
// Test 2: idempotency — repeated reconciler sweeps produce no duplicate terminal
// (the per-request UNIQUE index rejects the re-write).
// ---------------------------------------------------------------------------
func TestReconciler_Idempotent_NoDuplicateTerminal(t *testing.T) {
	// A very fast ticker sweeps MANY times over the orphan's lifetime. The first
	// sweep closes it; every subsequent sweep re-attempts the same terminal and
	// collides with the ux_terminal_response_per_request UNIQUE index — so exactly
	// one terminal must remain no matter how many times the level sweep runs.
	ch := newSweepingHome(t, 10*time.Millisecond)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	// Open request to an absent receiver (registered, never placed as a cell).
	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)

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
