package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An actor that lives in a channel directory names files the way its own
// process sees them: an absolute path on its own machine. Nothing upstream can
// complete that name — the sender would have to know which device it sits on,
// and a receiver cannot know the sender's machine at all — so the access door
// completes it, using the one thing only the device knows: where it actually
// put the channel directory.
//
// This is the only place that fact can be proven. Everything else about
// normalization is decided in accessdoor and tested there against a stated
// root; what cannot be faked is that a real daemon reports its real root over
// a real lane and the door's answer lands on the real file.
//
// The caller has to be an actor PLACED on the device. A browser is not one: it
// runs on no machine of the channel, so a local path names nothing for it —
// the first live run of this feature returned exactly that refusal, correctly.
func TestAnActorOnADeviceNamesFilesByItsOwnAbsolutePath(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	const deviceName = "e2e-path-daemon"
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": deviceName})
	deviceID := stringField(t, device, "id")
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": deviceID,
	})
	daemonLog := filepath.Join(h.root, "logs", "path-daemon.log")
	daemon := startProc(t, "path-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", deviceName, "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	// The layout rule lives in compute; this test asserts the door agrees with
	// the disk, so it spells the path the way the daemon builds it.
	channelRoot := filepath.Join(h.daemonHome, "daemons", deviceID, "channels", "c0")

	const scriptDecl = "e2e-path-script"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": scriptDecl, "name": scriptDecl, "class": "script",
		"config":     map[string]any{"tool_id": string(systemActor), "tool_type": "system.member.list"},
		"visibility": "private",
	})
	scriptIntro := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": scriptDecl, "desired_host": deviceID})
	scriptID := stringField(t, scriptIntro, "member")
	waitActorPresence(t, ws, scriptID, true, daemon, daemonLog)

	// The compartment only exists once the lane is up, which presence just
	// proved, so the directory the daemon reports is on disk by now.
	if err := os.MkdirAll(filepath.Join(channelRoot, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	const want = "bytes the actor should find by local path"
	if err := os.WriteFile(filepath.Join(channelRoot, "docs", "report.md"), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}

	// The script agent's verification path reads whatever resource_id it is
	// given, straight through sys.Resource().Open — so this is an actor on the
	// device asking the door for a file by its own absolute path.
	read := func(id string) (string, map[string]any, error) {
		return ws.tryRequest(c0ChannelID, "agent.ask", scriptID, map[string]any{
			"text": "read by path", "attachments": []map[string]any{{"address": id, "name": "report.md"}},
		})
	}

	local := filepath.Join(channelRoot, "docs", "report.md")
	_, reply, err := read(local)
	if err != nil {
		t.Fatalf("absolute local path was refused: %v (%v)\n%s", err, reply, tailLog(daemonLog, 60))
	}
	if content := fmt.Sprint(reply["text"]); !strings.Contains(content, want) {
		t.Fatalf("read by local path returned %v, want the file's bytes", reply)
	}

	// The same file under its address must be the same file. If the two forms
	// ever diverge, one of them is addressing something nobody meant.
	_, viaAddress, err := read("daemon://" + deviceName + "/c0/docs/report.md")
	if err != nil {
		t.Fatalf("address form was refused: %v (%v)", err, viaAddress)
	}
	if fmt.Sprint(viaAddress["text"]) != fmt.Sprint(reply["text"]) {
		t.Fatalf("local path and address disagree: %v vs %v", reply, viaAddress)
	}

	// A path outside the channel directory is refused by name, and the refusal
	// carries the boundary. Told only "no", a model discovers the line by
	// probing — confidently, and at length.
	_, outside, err := read("/etc/passwd")
	if err == nil {
		t.Fatalf("/etc/passwd was accepted: %v", outside)
	}
	detail := fmt.Sprint(outside["detail"])
	if !strings.Contains(detail, channelRoot) {
		t.Fatalf("refusal does not name the channel directory: %q", detail)
	}
}
