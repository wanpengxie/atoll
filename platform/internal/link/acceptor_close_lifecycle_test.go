package link

// Acceptor.Close as an admission gate and a bounded collector:
//
//   - Close is the admission cut AND the join point — an already-admitted
//     Serve is waited out, and no Serve is admitted afterwards;
//   - a control worker that never returns is ACCOUNTED, not joined: shutdown
//     stays bounded and the abandon shows up on the session snapshot.
//
// Note on shape: the old `Acceptor.Leaked` counter no longer exists on this
// branch. The leak account it used to carry now lives where the canonical
// four-state ledger keeps it — SessionSnapshot.Abandoned, written by
// completeSeal — so that is what the bounded-close test reads.

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// An admitted Serve is in-flight work: Close must wait for it to finish
// before returning, and must refuse every Serve that arrives afterwards.
//
// The wait is proved without racing the HTTP handler's own bookkeeping:
// completeSeal is the LAST thing the in-flight Serve's runLink does, so a row
// already in state closed at the moment Close returns cannot have been
// written after Close returned. One wedged control worker makes the wait
// long enough to be measurable as well.
func TestCloseWaitsForAdmittedServeThenRefusesNewServe(t *testing.T) {
	storage := newSessRaceStorage()
	rig := newAcceptorRig(t, acceptorRigConfig{
		settlementWindow: 800 * time.Millisecond,
		joinWindow:       300 * time.Millisecond,
		storage:          storage,
	})
	t.Cleanup(storage.releaseAll)

	daemon := dialRawDaemon(t, rig.wsURL(), true)
	active := rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
	sessRaceWedgeControlWorker(t, daemon, storage)

	start := time.Now()
	if err := rig.acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	elapsed := time.Since(start)

	if got := sessRaceSnapshot(rig.acc, active.Generation); got.State != SessionClosed {
		t.Fatalf("Close returned while the admitted Serve was still running: %+v", got)
	}
	if elapsed < 700*time.Millisecond {
		t.Fatalf("Close returned in %v; it did not wait out the in-flight session's settlement window", elapsed)
	}
	if elapsed > 20*time.Second {
		t.Fatalf("Close took %v; the join is meant to be bounded", elapsed)
	}

	// Admission is cut: a plain request is refused at the gate with an
	// honest status, and no websocket can be upgraded any more.
	response, err := http.Get(rig.srv.URL)
	if err != nil {
		t.Fatalf("post-close request: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("post-close body: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("post-close status=%d want 503", response.StatusCode)
	}
	if !strings.Contains(string(body), "link acceptor closed") {
		t.Fatalf("post-close body=%q want the closed-acceptor attribution", strings.TrimSpace(string(body)))
	}
	if conn, _, err := websocket.DefaultDialer.Dial(rig.wsURL(), nil); err == nil {
		_ = conn.Close()
		t.Fatal("a carrier was upgraded after the acceptor closed")
	}

	// Close is idempotent and still returns promptly.
	second := time.Now()
	if err := rig.acc.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if again := time.Since(second); again > 5*time.Second {
		t.Fatalf("second Close took %v; it must observe the completed close", again)
	}
}

// A control worker that never returns must not drag Close: the seal
// collection bounds itself with its own settlement/join windows, accounts the
// worker as abandoned, and shutdown completes. Nothing here joins the wedged
// goroutine — it is still parked when Close returns, which is exactly the
// point (§6 有界关闭但不伪造健康: an abandon is accounted, never disguised as
// a clean join).
func TestWedgedControlWorkerDoesNotDragCloseAndIsAccounted(t *testing.T) {
	storage := newSessRaceStorage()
	rig := newAcceptorRig(t, acceptorRigConfig{
		settlementWindow: 200 * time.Millisecond,
		joinWindow:       200 * time.Millisecond,
		storage:          storage,
	})
	t.Cleanup(storage.releaseAll)

	daemon := dialRawDaemon(t, rig.wsURL(), true)
	active := rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
	sessRaceWedgeControlWorker(t, daemon, storage)

	start := time.Now()
	if err := rig.acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		t.Fatalf("Close took %v; a wedged control worker dragged shutdown past its own windows", elapsed)
	}

	// The worker is still parked — nothing below joins it, and `storage.release`
	// stays open until the cleanup registered above runs. What that abandon has
	// to look like on the ledger is the assertion; the parking itself is the
	// fixture, not a claim.
	closed := sessRaceSnapshot(rig.acc, active.Generation)
	if closed.State != SessionClosed {
		t.Fatalf("session did not reach closed after Close: %+v", closed)
	}
	if closed.Abandoned < 1 {
		t.Fatalf("abandoned=%d; the wedged control worker was not accounted", closed.Abandoned)
	}
	if rig.acc.IsAttached("daemon-1") {
		t.Fatal("a bounded-abandon shutdown left the daemon accounted as attached")
	}
}
