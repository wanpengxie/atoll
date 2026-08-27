package e2e

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setupDaemonEcho provisions the standard daemon lane: mint+attach a device,
// declare an echo actor placed on it, start the daemon, and wait until the
// actor is present. Returns the daemon process, the echo actor id, the minted
// device key, and the daemon log path.
func setupDaemonEcho(t *testing.T, h *harness, ws *wsClient, declID string) (*proc, string, string, string) {
	t.Helper()
	registrar := findRegistrar(t, ws)
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": "e2e-resilience"})
	deviceID := stringField(t, device, "id")
	deviceKey := stringField(t, device, "key")
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID,
		"device_id":  deviceID,
	})
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		// A declaration's name is the word the member it seats is called by, so
		// it obeys the name law; the sentence goes in description.
		"id": declID, "name": "e2e-resilience-echo", "class": "echo",
		"description": "Echo seat for the resilience runs.",
		"config":      map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": declID})
	echoID := stringField(t, introduced, "member")

	daemonLog := filepath.Join(h.root, "logs", "daemon.log")
	daemon := startProc(t, "daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", deviceKey,
		"--name", "e2e-resilience",
		"--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, ws, echoID, true, daemon, daemonLog)
	return daemon, echoID, deviceKey, daemonLog
}

// The carrier link (daemon's /compute websocket) dying is a fact of life the
// daemon must absorb by itself: the server goes away and comes back, the
// daemon process never restarts, and its hosted actor must return to service
// without any operator action. Only a black-box test can hold "the same OS
// process reconnects" — in-process tests cannot lose a real TCP link.
func TestDaemonReconnectsAfterCarrierLinkLoss(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	daemon, echoID, _, daemonLog := setupDaemonEcho(t, h, ws, "e2e-reconnect-echo")

	// Sever the link by replacing the server process. The daemon stays up.
	h.restartServer()
	if daemon.exited() {
		t.Fatalf("daemon exited when the carrier link dropped\n%s", tailLog(daemonLog, 100))
	}

	_, recovered := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	waitActorPresence(t, recovered, echoID, true, daemon, daemonLog)
	reply := recovered.request(c0ChannelID, "echo.say", echoID, map[string]any{"text": "after-link-loss"})
	if reply["text"] != "after-link-loss" {
		t.Fatalf("echo after reconnect=%v", reply)
	}
}

// Credentials cross this system in three shapes — a user password (register
// and a rejected login), and a minted device key (daemon bootstrap flag).
// None of them may land in either process's log. The root install password is
// the one deliberate exception (installation prints it once, by ruling).
func TestCredentialsNeverLandInProcessLogs(t *testing.T) {
	h := newHarness(t)
	api := newAPIClient(t, h.base)

	const userPassword = "e2e-user-secret-1f0a"
	const wrongPassword = "e2e-wrong-secret-9c3d"
	api.register("", "resilience@example.com", userPassword)
	api.request(http.MethodPost, "/api/identity/login",
		map[string]string{"email": "resilience@example.com", "password": wrongPassword},
		http.StatusUnauthorized)

	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	_, echoID, deviceKey, _ := setupDaemonEcho(t, h, ws, "e2e-credlog-echo")
	ws.request(c0ChannelID, "echo.say", echoID, map[string]any{"text": "traffic"})

	// Both processes have now handled every credential shape at least once.
	secrets := map[string]string{
		"registered password": userPassword,
		"rejected password":   wrongPassword,
		"device key":          deviceKey,
		// The install-time root password used to be printed here on purpose,
		// and that exception is withdrawn: the node's log is the worst home for
		// it (default permissions, tens of megabytes, tailed and pasted), and
		// copying the secret there defeated every other precaution taken with
		// it. It is now shown once on the installer's terminal and nowhere else.
		"install root password": rootPassword,
	}
	logs, err := filepath.Glob(filepath.Join(h.root, "logs", "*.log"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("no process logs found: %v", err)
	}
	// The positive control has to be something the node really writes and
	// nothing else does. It used to be the root password itself, which was
	// convenient precisely because that was a bug; with the secret gone, the
	// install line it sat on serves the same purpose and costs nothing.
	const installMarker = "atoll installed"
	sawInstallLine := false
	for _, path := range logs {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, secret := range secrets {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("%s leaked into %s", name, filepath.Base(path))
			}
		}
		sawInstallLine = sawInstallLine || strings.Contains(string(raw), installMarker)
	}
	// Without a control, "no secret was found" is also what reading the wrong
	// file looks like, and the green above would prove nothing.
	if !sawInstallLine {
		t.Fatalf("scanner never saw %q; it is reading the wrong logs", installMarker)
	}
}

// Both processes dying out of order is the ugliest crash shape: server first,
// then the daemon while the server is already gone, then revival server-first
// with a bare daemon start (identity persisted in its home). The system must
// converge back to a serving actor with zero manual repair.
func TestInterleavedKillNineConverges(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	daemon, echoID, _, _ := setupDaemonEcho(t, h, ws, "e2e-interleave-echo")

	h.server.kill9(t)
	daemon.kill9(t)

	h.startServer()
	_, revived := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	daemonLog := filepath.Join(h.root, "logs", "daemon-revived.log")
	daemon = startProc(t, "daemon-revived", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, revived, echoID, true, daemon, daemonLog)

	reply := revived.request(c0ChannelID, "echo.say", echoID, map[string]any{"text": "after-interleaved-kill"})
	if reply["text"] != "after-interleaved-kill" {
		t.Fatalf("echo after interleaved kill=%v", reply)
	}
}
