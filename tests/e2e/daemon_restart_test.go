//go:build e2e

package e2e

import (
	"database/sql"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_DaemonRestart_Reconnect proves the daemon can crash + come
// back without losing the placement binding. Concretely:
//
//  1. Start stack, register, bind a channel, POST one message.
//  2. SIGINT the daemon, wait for clean exit.
//  3. Restart with the same --data-dir + --daemon-id.
//  4. Within 30s the daemons row last_heartbeat_at advances past
//     its pre-restart value AND placements row remains active.
//  5. POST a second message — must succeed (no 524 / no "daemon not
//     connected" 503).
//
// Catches:
//   - lost in-memory placement state on daemon restart
//   - server-side placement reclaim handshake regressions
//   - WSClient reconnect path silently dropping write_message
func TestE2E_DaemonRestart_Reconnect(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "restart+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-restart-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-restart-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	first := s.PostMessage(chID, "human.text", "before-restart", "")
	if first.MessageID == "" {
		t.Fatalf("pre-restart post failed: %+v", first)
	}

	hbBefore := readDaemonHeartbeat(t, s, "daemon-e2e")

	s.RestartDaemon()

	// Wait up to 30s for the daemon to push a fresh heartbeat past
	// hbBefore. RegisterDaemon happens in waitDaemonRegistered, but
	// the first heartbeat tick (immediate) writes a NEW timestamp
	// strictly greater than hbBefore.
	deadline := time.Now().Add(30 * time.Second)
	var hbAfter int64
	for time.Now().Before(deadline) {
		hbAfter = readDaemonHeartbeat(t, s, "daemon-e2e")
		if hbAfter > hbBefore {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if hbAfter <= hbBefore {
		t.Fatalf("daemon heartbeat did not advance within 30s of restart (before=%d after=%d)",
			hbBefore, hbAfter)
	}

	// Placement row for the channel should be back to active. Use
	// the server's view of the placements table directly so we don't
	// depend on any per-test API.
	if !waitPlacementActive(t, s, chID, 15*time.Second) {
		t.Fatalf("placement for channel %s never returned to active state", chID)
	}

	second := s.PostMessage(chID, "human.text", "after-restart", "")
	if second.MessageID == "" {
		t.Fatalf("post-restart post failed: %+v", second)
	}
	if !second.Accepted {
		t.Fatalf("post-restart accepted=%v want true: %+v", second.Accepted, second)
	}
}

func waitPlacementActive(t *testing.T, s *harness.Stack, channelID string, timeout time.Duration) bool {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDBPath()+"?mode=ro")
	if err != nil {
		t.Fatalf("open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var state string
		err := db.QueryRow(`SELECT state FROM channel_placements WHERE channel_id=?`,
			channelID).Scan(&state)
		if err == nil && state == "active" {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
