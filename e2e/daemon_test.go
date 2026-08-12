package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestDaemonComputeLinkServesAssignedActor(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, ws)

	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "e2e-daemon"})
	deviceID := stringField(t, device, "id")
	deviceKey := stringField(t, device, "key")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{
		"channel_id": c0ChannelID,
		"device_id":  deviceID,
	})

	const declarationID = "e2e-remote-echo"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declarationID, "name": "E2E remote echo", "class": "echo",
		"config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{
		"kind": "tool", "decl_id": declarationID,
	})
	echoID := stringField(t, introduced, "instance_id")

	daemonLog := filepath.Join(h.root, "logs", "daemon.log")
	daemon := startProc(t, "daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", deviceKey,
		"--name", "e2e-daemon",
		"--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	waitActorPresence(t, ws, echoID, true, daemon, daemonLog)

	reply := ws.request(c0ChannelID, "echo.say", echoID, map[string]any{"text": "through-compute"})
	if reply["text"] != "through-compute" {
		t.Fatalf("remote echo reply=%v", reply)
	}

	// A later bare start must resume the identity persisted by the first
	// --server/--key registration. This is the production daemon restart path.
	daemon.kill9(t)
	waitActorPresence(t, ws, echoID, false, nil, daemonLog)
	daemonRestartLog := filepath.Join(h.root, "logs", "daemon-restart.log")
	daemon = startProc(t, "daemon-restart", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonRestartLog)
	waitActorPresence(t, ws, echoID, true, daemon, daemonRestartLog)
	reply = ws.request(c0ChannelID, "echo.say", echoID, map[string]any{"text": "after-daemon-kill-9"})
	if reply["text"] != "after-daemon-kill-9" {
		t.Fatalf("restarted remote echo reply=%v", reply)
	}
}

func waitActorPresence(t *testing.T, ws *wsClient, actorID string, want bool, daemon *proc, logPath string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if daemon != nil && daemon.exited() {
			t.Fatalf("daemon exited while waiting for actor presence=%v\n%s", want, tailLog(logPath, 100))
		}
		catalog := ws.request(c0ChannelID, "actor.list", systemActor, map[string]any{})
		present := false
		rows, _ := catalog["actors"].([]any)
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			present = present || row["id"] == actorID && row["present"] == true
		}
		if present == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("remote actor %s presence=%v, want %v\ndaemon log:\n%s", actorID, present, want, tailLog(logPath, 100))
		}
		time.Sleep(200 * time.Millisecond)
	}
}
