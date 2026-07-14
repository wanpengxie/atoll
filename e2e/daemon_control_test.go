package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestDaemonBinaryCanonicalControl is the zero-legacy black-box seam: channel
// composition changes enter only as authenticated subject frames, while the
// real daemon binary pulls that plan, builds the remote actor, serves work and
// reconnects after a hard process death.
func TestDaemonBinaryCanonicalControl(t *testing.T) {
	binDir := requireE2EBin(t)
	serverBin := filepath.Join(binDir, "atoll-server")
	daemonBin := filepath.Join(binDir, "atoll-daemon")
	if _, err := os.Stat(daemonBin); err != nil {
		t.Fatalf("binary missing: %v", err)
	}

	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "daemonwd", "channels", "daemon-ws", "home", "logs")
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	serverLog := filepath.Join(dirs["logs"], "server.log")
	var daemonLog string
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("server log tail:\n%s", tailLog(serverLog, 80))
			if daemonLog != "" {
				t.Logf("daemon log tail:\n%s", tailLog(daemonLog, 80))
			}
		}
	})
	server := startProc(t, "canonical-server", serverBin, []string{
		"-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-db", dbPath,
		"-channel-db-dir", dirs["channels"],
		"-init",
	}, dirs["serverwd"], serverLog, env)
	waitHealthz(t, base, server, 30*time.Second)

	api := newAPIClient(t, base)
	api.must("POST", "/api/identity/register", map[string]any{
		"email": "canonical@example.com", "password": "secret123", "display_name": "Canonical",
	}, http.StatusCreated)
	workspace := api.must("POST", "/api/workspaces", map[string]any{"name": "canonical-ws"}, http.StatusCreated)
	workspaceID, _ := workspace["id"].(string)
	channelRow := api.must("POST", "/api/workspaces/"+workspaceID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	channelID, _ := channelRow["id"].(string)
	daemonRow := api.must("POST", "/api/channels/"+channelID+"/daemons", map[string]any{"name": "canonical-box"}, http.StatusCreated)
	daemonID, _ := daemonRow["id"].(string)
	apiKey, _ := daemonRow["api_key"].(string)
	if channelID == "" || daemonID == "" || apiKey == "" {
		t.Fatalf("incomplete channel/daemon identity: channel=%q daemon=%q key=%t", channelID, daemonID, apiKey != "")
	}

	ws := dialWS(t, base, api.cookieHeader(), channelID, 0)
	defer ws.close()
	echoDecl := api.must("POST", "/api/actor-decls", map[string]any{"name": "echo-tool", "class": "echo"}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	echoResult := canonicalControl(t, ws, "channel.introduce_actor", map[string]any{
		"decl_id": echoDeclID, "placement": "server",
	})
	echoID, _ := echoResult["instance_id"].(string)
	if echoID == "" {
		t.Fatalf("introduce echo returned no instance_id: %v", echoResult)
	}

	assistantDecl := api.must("POST", "/api/actor-decls", map[string]any{
		"name": "assistant", "class": "script", "config": map[string]any{"tool_id": echoID},
	}, http.StatusCreated)
	assistantDeclID, _ := assistantDecl["id"].(string)
	assistantResult := canonicalControl(t, ws, "channel.introduce_actor", map[string]any{
		"decl_id": assistantDeclID, "placement": "daemon", "desired_host": daemonID, "make_default": true,
	})
	assistantID, _ := assistantResult["instance_id"].(string)
	if assistantID == "" {
		t.Fatalf("introduce assistant returned no instance_id: %v", assistantResult)
	}
	canonicalControl(t, ws, "channel.set_default_agent", map[string]any{"instance_id": assistantID})

	startDaemon := func(generation int) *proc {
		daemonLog = filepath.Join(dirs["logs"], fmt.Sprintf("daemon-%d.log", generation))
		return startProc(t, fmt.Sprintf("canonical-daemon#%d", generation), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, channelID),
			"-key", apiKey,
			"-name", "canonical-box",
			"-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], daemonLog, env)
	}
	daemon := startDaemon(1)
	waitDaemonOnline(t, api, channelID, daemonID)

	payload1 := json.RawMessage(`{"text":"canonical one"}`)
	_, terminal1 := submitAndAwaitTerminal(t, ws, "loop.chat", payload1, 120*time.Second)
	resourceID := assertCanonicalChat(t, terminal1, payload1)
	verifyCanonicalResource(t, ws, resourceID, payload1)

	canonicalControl(t, ws, "channel.restart_actor", map[string]any{"instance_id": assistantID})
	payload2 := json.RawMessage(`{"text":"after restart"}`)
	_, terminal2 := submitAndAwaitTerminal(t, ws, "loop.chat", payload2, 120*time.Second)
	_ = assertCanonicalChat(t, terminal2, payload2)

	daemon.kill9(t)
	daemon = startDaemon(2)
	waitDaemonOnline(t, api, channelID, daemonID)
	payload3 := json.RawMessage(`{"text":"after daemon crash"}`)
	_, terminal3 := submitAndAwaitTerminal(t, ws, "loop.chat", payload3, 180*time.Second)
	_ = assertCanonicalChat(t, terminal3, payload3)
	verifyCanonicalResource(t, ws, resourceID, payload1)

	canonicalControl(t, ws, "channel.remove_actor", map[string]any{"instance_id": assistantID})
	daemon.kill9(t)
	server.kill9(t)
}

func canonicalControl(t *testing.T, ws *wsClient, messageType string, payload map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	requestID := frameSubmit(t, ws, map[string]any{
		"msg_type": messageType,
		"kind":     "request",
		"audience": []string{"system"},
		"payload":  json.RawMessage(raw),
	})
	terminal, ok := ws.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == requestID && terminalStatus(env) != ""
	}, 30*time.Second)
	if !ok {
		t.Fatalf("%s produced no terminal", messageType)
	}
	if terminalStatus(terminal) != "completed" {
		t.Fatalf("%s failed: %v", messageType, envelopePayload(terminal))
	}
	return envelopePayload(terminal)
}

func waitDaemonOnline(t *testing.T, api *apiClient, channelID, daemonID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, body := api.do("GET", "/api/channels/"+channelID+"/daemons", nil)
		rows, _ := body["daemons"].([]any)
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			if row["id"] == daemonID && row["online"] == true {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("daemon %s did not attach to channel %s", daemonID, channelID)
}

func assertCanonicalChat(t *testing.T, terminal map[string]any, sent json.RawMessage) string {
	t.Helper()
	payload := envelopePayload(terminal)
	if payload["ok"] != true {
		t.Fatalf("chat terminal not ok: %v", payload)
	}
	var want map[string]any
	if err := json.Unmarshal(sent, &want); err != nil {
		t.Fatal(err)
	}
	echoed, _ := payload["echoed"].(map[string]any)
	if !reflect.DeepEqual(echoed, want) {
		t.Fatalf("echoed=%v want=%v", echoed, want)
	}
	resourceID, _ := payload["resource_id"].(string)
	if resourceID == "" {
		t.Fatalf("chat terminal carries no resource_id: %v", payload)
	}
	return resourceID
}

func verifyCanonicalResource(t *testing.T, ws *wsClient, resourceID string, original json.RawMessage) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"resource_id": resourceID})
	_, terminal := submitAndAwaitTerminal(t, ws, "loop.verify", payload, 120*time.Second)
	result := envelopePayload(terminal)
	if result["exists"] != true || result["content"] != string(original) {
		t.Fatalf("verify %s=%v, want byte-exact %q", resourceID, result, string(original))
	}
}
