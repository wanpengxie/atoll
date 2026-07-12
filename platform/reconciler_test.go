package platform_test

import (
	"context"
	"path/filepath"
	"sync"
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
// Test 2 (C7 新测③ · 注销+边沿竞态无双终态): the two closure AUTHORS — the death
// edge (OnDown → downCh → consumeDown → closeFor) and the level scan
// (channel.Reconcile) — race concurrently on the SAME closed-forever fact. Both
// funnel every terminal through behavior → the ux_terminal_response_per_request
// UNIQUE index, so however they interleave, exactly one terminal survives.
//
// The prior version only re-ran the level sweep and proved level-vs-level
// idempotency; it never generated a death edge, so the cross-author arbitration
// this格 owes went unverified. Here a barrier releases many edge-path AND
// level-path authors at once (best exercised under -race), then we assert a
// single terminal.
// ---------------------------------------------------------------------------
func TestReconciler_DeregEdgeRace_NoDuplicateTerminal(t *testing.T) {
	// A long interval so the background ticker's own sweep never fires during the
	// storm — the ONLY authors are the ones this test releases at the barrier, so a
	// duplicate terminal could come only from edge-vs-level (or edge-vs-edge) racing,
	// which is exactly what格③ must rule out.
	ch := newSweepingHome(t, time.Hour)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	// Worker is a LIVE cell so its embodiment is real; Remove despawns AND deregs it.
	workerPen := spawnWithPen(t, ch, &workerID, actor.KindAgent)
	_ = workerPen

	// Open request, then Remove the receiver → closed-forever fact holds and the
	// inbound request is still open (Remove never closes inbound requests). A
	// terminal may already exist here if Remove's own despawn edge landed after the
	// dereg committed — that is fine; the assertion is a COUNT, so a pre-existing
	// terminal only makes the race's collision surface larger.
	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)
	if err := ch.Remove(context.Background(), workerID); err != nil {
		t.Fatalf("Remove worker: %v", err)
	}

	// Release many edge-path and level-path authors simultaneously. Each edge post
	// travels the real OnDown→consumeDown→closeFor path; each Reconcile is the real
	// level scan. Both re-derive gone==true for workerID and attempt the terminal.
	const authors = 8
	const rounds = 25
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < authors; i++ {
		wg.Add(1)
		edge := i%2 == 0
		go func(edge bool) {
			defer wg.Done()
			<-start
			for r := 0; r < rounds; r++ {
				if edge {
					platform.DriveDownEdgeForTest(ch, workerID)
				} else {
					platform.ReconcileClosureForTest(ch)
				}
			}
		}(edge)
	}
	close(start)
	wg.Wait()

	// Edge posts are drained asynchronously by consumeDown; wait for a terminal to
	// exist, then let the queue settle, then assert exactly one.
	deadline := time.Now().Add(5 * time.Second)
	for !terminalExists(t, ch, reqID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !terminalExists(t, ch, reqID) {
		t.Fatal("neither the death edge nor the level scan closed the deregistered receiver's request")
	}
	// A final level scan drains any straggler edge-path write still in flight, then
	// re-attempts once more — the last chance to expose a duplicate.
	time.Sleep(200 * time.Millisecond)
	platform.ReconcileClosureForTest(ch)
	if n := countTerminals(t, ch, reqID); n != 1 {
		t.Fatalf("dereg+edge race broke UNIQUE arbitration: %d terminals for one request, want exactly 1", n)
	}
}
