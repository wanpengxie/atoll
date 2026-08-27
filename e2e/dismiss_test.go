package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Dropping somebody else's waiting task is answered BY the actor holding it.
// That is not a detour around cancellation — it is the only shape the closure
// model has: a terminal comes from the receiver, from the caller closing its
// own account, or from the substrate, and a bystander is none of those. So the
// truthful act is to ask the holder to answer, and it does.
func TestDismissIsAnsweredByTheHolder(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	const deviceName = "e2e-dismiss-daemon"
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": deviceName})
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": stringField(t, device, "id"),
	})
	daemonLog := filepath.Join(h.root, "logs", "dismiss-daemon.log")
	daemon := startProc(t, "dismiss-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", deviceName, "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	const decl = "e2e-dismiss-agent"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": decl, "name": decl, "class": "script",
		"config":     map[string]any{"tool_id": systemActor, "tool_type": "system.member.list"},
		"visibility": "private",
	})
	seated := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": decl})
	agentID := stringField(t, seated, "member")

	waitActorPresence(t, ws, agentID, true, daemon, daemonLog)

	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})

	// An agent runs one turn at a time, so a burst leaves a tail waiting.
	// Holding it would not do: a new message deliberately releases a hold, so
	// the only honest way to observe queued work is to actually make some.
	// dismiss is a command — it runs on arrival rather than queueing behind
	// the very work it is about — so the tail is still waiting when it lands.
	var waiting string
	for i := 0; i < 24; i++ {
		waiting = ws.submit(c0ChannelID, "agent.ask", "request", []string{agentID},
			map[string]any{"text": fmt.Sprintf("排队 %d", i)})
	}
	if waiting == "" {
		t.Fatal("the asks were not accepted")
	}

	ws.submit(c0ChannelID, "agent.dismiss", "request", []string{agentID}, map[string]any{"target": waiting})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "response" || envelope["parent_id"] != waiting {
				continue
			}
			body, _ := envelope["payload"].(map[string]any)
			if body["status"] != "failed" {
				continue
			}
			// The answer comes from the actor that owed it, not from the
			// substrate and not from the person who asked.
			sender, _ := envelope["sender"].(map[string]any)
			if sender["id"] != agentID {
				t.Fatalf("the terminal was authored by %v, want the actor holding the task", sender["id"])
			}
			if body["error_code"] != "dismissed" {
				t.Fatalf("terminal=%v, want it to say the task was dismissed", body)
			}
			return
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatal("the waiting task was never answered")
}
