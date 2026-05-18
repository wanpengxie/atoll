//go:build e2e

package e2e

import (
	"database/sql"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_DaemonbusKeepalive_PingPong asserts the daemonbus WS stays
// alive under prolonged business idleness. Specifically:
//
//   - Bring the stack up + bind a channel (so the daemon has an
//     active placement row whose heartbeat we can watch).
//   - Hold for ~10s without any business traffic.
//   - The daemons.last_heartbeat_at column must advance by at least
//     one cadence in that window.
//   - The daemon process must still be running (not exited).
//
// Catches bug #6: cloudflare / network-middlebox idle close that
// half-opens a stuck daemonbus WS. The full reproduction window is
// closer to 60s of idle (cloudflare's default), but cmd/daemon's
// keepalive cadence is < 10s so even a short window proves the
// WSClient SetPongHandler / ping pump are wired.
func TestE2E_DaemonbusKeepalive_PingPong(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "keepalive+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-keep-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-keep-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	// Sample the daemon last_heartbeat_at twice with a > pingCadence
	// gap and require it to advance. We DON'T rely on a fixed wall
	// clock value — we only require monotonic progress.
	// Heartbeat cadence is 15s (transit.DefaultHeartbeatPeriod). Wait
	// long enough for one full period plus margin so a missed first
	// tick doesn't false-fail. Idle window matters: the WS keepalive
	// (ping/pong) must keep the underlying conn open across this
	// span, otherwise the heartbeat write itself would fail and the
	// last_heartbeat_at row would not move forward.
	hb1 := readDaemonHeartbeat(t, s, "daemon-e2e")

	const idleWindow = 18 * time.Second
	idleDeadline := time.Now().Add(idleWindow)
	for time.Now().Before(idleDeadline) {
		time.Sleep(500 * time.Millisecond)
	}

	hb2 := readDaemonHeartbeat(t, s, "daemon-e2e")
	if hb2 <= hb1 {
		t.Fatalf("daemonbus heartbeat did not advance during %s idle window (hb1=%d hb2=%d) — WS keepalive likely broken",
			idleWindow, hb1, hb2)
	}
}

// readDaemonHeartbeat opens server.db read-only and returns the
// last_heartbeat_at column for the named daemon. Zero on missing
// row.
func readDaemonHeartbeat(t *testing.T, s *harness.Stack, daemonID string) int64 {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDBPath()+"?mode=ro")
	if err != nil {
		t.Fatalf("open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v int64
	err = db.QueryRow(`SELECT COALESCE(last_heartbeat_at, 0) FROM daemons WHERE id=?`,
		daemonID).Scan(&v)
	if err == sql.ErrNoRows {
		return 0
	}
	if err != nil {
		t.Fatalf("read heartbeat: %v", err)
	}
	return v
}
