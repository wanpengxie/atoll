package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// seatDevice mints a device seat from the shared template. An empty host means
// "no preference" and exercises the channel's default.
func seatDevice(t *testing.T, ws *wsClient, decl, host string) string {
	t.Helper()
	payload := map[string]any{"decl_id": decl}
	if host != "" {
		payload["desired_host"] = host
	}
	return stringField(t, ws.request(c0ChannelID, "system.member.create", systemActor, payload), "member")
}

// Two daemons at once is the shape the device class exists for: the node's own
// local device plus a machine that dialled in, each carrying its own shell and
// files into the same channel, neither able to touch the other's disk and
// neither taken down by the other's failure.
//
// None of it had ever run. The device class derived its own actor id instead of
// filling the planned seat, so a daemon host refused to build it and every
// seating stayed permanently absent — the actor body's unit tests passed the
// whole time because nothing could reach the body.
//
// The node here is a real `atoll up`, not a bare server with a hand-claimed
// local device key: the local device has to be the one an operator actually
// gets, running inside the node process, or "two daemons coexist" is being
// proved against a fixture rather than against the product.
func TestTwoDaemonsEachRunTheirOwnDevice(t *testing.T) {
	h := newNodeHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	const remoteName = "e2e-second-daemon"
	remote := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": remoteName})
	remoteID, remoteKey := stringField(t, remote, "id"), stringField(t, remote, "key")
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": remoteID,
	})

	const decl = "e2e-device-seat"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": decl, "name": decl, "class": "device",
		"description": "Device seat for the two-daemon run.",
		"visibility":  "private",
	})

	// One template, two seats, two machines — the id is the seat's, so the same
	// recipe can be placed twice without the two colliding.
	//
	// The local seat names no host on purpose: with two devices attached, the
	// channel's default must be the node's own device. Before that rule existed
	// the default was the first device by id, and a minted uuid always sorts
	// before the literal "local-device" — so this seat would have landed on the
	// dialled-in machine and this test would be silently testing one daemon
	// twice.
	localSeat := seatDevice(t, ws, decl, "")
	remoteSeat := seatDevice(t, ws, decl, remoteID)
	if localSeat == remoteSeat {
		t.Fatalf("both seats share the id %q", localSeat)
	}

	// Only the second daemon is started here; the first one is already running
	// inside the node process.
	remoteHome := filepath.Join(h.root, "daemon-remote")
	remoteLog := filepath.Join(h.root, "logs", "daemon-remote.log")
	remoteDaemon := startProc(t, remoteName, filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", remoteKey, "--name", remoteName, "--home", remoteHome,
	}, h.env, filepath.Join(h.root, "work"), remoteLog)

	// The regression's signature was presence that never arrives while the log
	// repeats "class derived a different id", so this is the assertion that
	// matters most.
	waitActorPresence(t, ws, localSeat, true, h.server, h.server.logPath)
	waitActorPresence(t, ws, remoteSeat, true, remoteDaemon, remoteLog)

	// Each seat runs a real bash, on its own machine, answering into the ledger.
	for _, seat := range []struct{ name, actor string }{{"local", localSeat}, {"remote", remoteSeat}} {
		result := ws.request(c0ChannelID, "device.exec", seat.actor, map[string]any{
			"command": "printf 'ran on the " + seat.name + " device'",
		})
		if code, ok := result["exit_code"].(float64); !ok || code != 0 {
			t.Fatalf("%s device.exec exit_code=%v: %v", seat.name, result["exit_code"], result)
		}
		if stdout, _ := result["stdout"].(string); stdout != "ran on the "+seat.name+" device" {
			t.Fatalf("%s device.exec stdout=%q", seat.name, stdout)
		}
	}

	// Separation is the claim worth proving: a file written through one seat
	// must be invisible to the other, because they are different disks. A shared
	// workspace would make both seats a single machine wearing two names.
	ws.request(c0ChannelID, "device.file.write", localSeat, map[string]any{
		"path": "marker.txt", "content": "local device\n",
	})
	ws.request(c0ChannelID, "device.file.write", remoteSeat, map[string]any{
		"path": "marker.txt", "content": "second daemon\n",
	})
	for _, seat := range []struct{ actor, want string }{
		{localSeat, "local device"},
		{remoteSeat, "second daemon"},
	} {
		read := ws.request(c0ChannelID, "device.file.read", seat.actor, map[string]any{"path": "marker.txt"})
		if content, _ := read["content"].(string); !strings.Contains(content, seat.want) {
			t.Fatalf("seat %s read %v, want %q — the two seats share a workspace", seat.actor, read, seat.want)
		}
	}

	// And the disks really are two directories under two daemon homes, each
	// addressed by its own device id — the node's own device home under the node
	// root, the dialled-in one under its own.
	for _, path := range []struct{ file, want string }{
		{filepath.Join(h.nodeDir, "device", "daemons", "local-device", "channels", "c0", "marker.txt"), "local device"},
		{filepath.Join(remoteHome, "daemons", remoteID, "channels", "c0", "marker.txt"), "second daemon"},
	} {
		content, err := os.ReadFile(path.file)
		if err != nil {
			t.Fatalf("workspace file %s: %v", path.file, err)
		}
		if !strings.Contains(string(content), path.want) {
			t.Fatalf("%s holds %q, want %q", path.file, content, path.want)
		}
	}

	// A path leaving the workspace is refused rather than served: the file words
	// are confined even though device.exec deliberately is not.
	if _, refused, err := ws.tryRequest(c0ChannelID, "device.file.read", remoteSeat, map[string]any{
		"path": "../../../../etc/hostname",
	}); err == nil {
		t.Fatalf("device.file.read escaped the workspace: %v", refused)
	}

	// Independence: losing the dialled-in machine must not disturb the local
	// one. This is the property an operator is really buying when they place a
	// member on a laptop — the laptop closing its lid cannot take the node's own
	// agents down with it.
	remoteDaemon.kill9(t)
	waitActorPresence(t, ws, remoteSeat, false, h.server, h.server.logPath)
	survivor := ws.request(c0ChannelID, "device.exec", localSeat, map[string]any{
		"command": "printf 'still here'",
	})
	if stdout, _ := survivor["stdout"].(string); stdout != "still here" {
		t.Fatalf("the local device stopped answering after the other daemon died: %v", survivor)
	}
}
