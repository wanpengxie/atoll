package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type deviceE2EChannel struct {
	id        string
	assistant string
	ws        *wsClient
}

type deviceE2EHarness struct {
	api       *apiClient
	base      string
	daemonID  string
	apiKey    string
	dirs      map[string]string
	daemonBin string
	channels  []deviceE2EChannel
}

func newDeviceE2EHarness(t *testing.T, channelCount int) *deviceE2EHarness {
	t.Helper()
	binDir := requireE2EBin(t)
	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "daemonwd", "channels", "daemon-ws", "home", "logs")
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	serverLog := filepath.Join(dirs["logs"], "server.log")
	server := startProc(t, "device-server", filepath.Join(binDir, "atoll-server"), []string{
		"-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-db", filepath.Join(root, "app.db"),
		"-channel-db-dir", dirs["channels"],
		"-init",
	}, dirs["serverwd"], serverLog, scrubbedEnv(dirs["home"]))
	waitHealthz(t, base, server, 30*time.Second)

	api := newAPIClient(t, base)
	api.must("POST", "/api/identity/register", map[string]any{
		"email": "device@example.com", "password": "secret123", "display_name": "Device",
	}, http.StatusCreated)
	daemonRow := api.must("POST", "/api/daemons", map[string]any{"name": "device-box"}, http.StatusCreated)
	h := &deviceE2EHarness{
		api: api, base: base, dirs: dirs, daemonBin: filepath.Join(binDir, "atoll-daemon"),
		daemonID: stringField(t, daemonRow, "id"), apiKey: stringField(t, daemonRow, "api_key"),
	}
	for i := 0; i < channelCount; i++ {
		channelRow := api.must("POST", "/api/channels", map[string]any{
			"name": fmt.Sprintf("device-%d", i),
		}, http.StatusCreated)
		chID := stringField(t, channelRow, "id")
		api.must("POST", "/api/channels/"+chID+"/daemons",
			map[string]any{"daemon_id": h.daemonID}, http.StatusOK)
		ws := dialWS(t, base, api.cookieHeader(), chID, 0)

		echoDecl := api.must("POST", "/api/actor-decls", map[string]any{
			"name": fmt.Sprintf("echo-%d", i), "class": "echo", "visibility": "public",
		}, http.StatusCreated)
		echoResult := canonicalControl(t, ws, "channel.introduce_actor", map[string]any{
			"decl_id": stringField(t, echoDecl, "id"), "placement": "server",
		})
		assistantDecl := api.must("POST", "/api/actor-decls", map[string]any{
			"name": fmt.Sprintf("assistant-%d", i), "class": "script", "visibility": "public",
			"config": map[string]any{"tool_id": stringField(t, echoResult, "instance_id")},
		}, http.StatusCreated)
		assistantResult := canonicalControl(t, ws, "channel.introduce_actor", map[string]any{
			"decl_id":   stringField(t, assistantDecl, "id"),
			"placement": "daemon", "desired_host": h.daemonID, "make_default": true,
		})
		assistant := stringField(t, assistantResult, "instance_id")
		canonicalControl(t, ws, "channel.set_default_agent", map[string]any{"instance_id": assistant})
		h.channels = append(h.channels, deviceE2EChannel{id: chID, assistant: assistant, ws: ws})
	}
	return h
}

func stringField(t *testing.T, row map[string]any, name string) string {
	t.Helper()
	value, _ := row[name].(string)
	if value == "" {
		t.Fatalf("missing %s in %v", name, row)
	}
	return value
}

func (h *deviceE2EHarness) startDaemon(t *testing.T, generation int) *proc {
	t.Helper()
	logPath := filepath.Join(h.dirs["logs"], fmt.Sprintf("device-daemon-%d.log", generation))
	return startProc(t, fmt.Sprintf("device-daemon#%d", generation), h.daemonBin, []string{
		"-server", strings.Replace(h.base, "http://", "ws://", 1) + "/compute",
		"-key", h.apiKey,
		"-name", "device-box",
		"-workspace", h.dirs["daemon-ws"],
	}, h.dirs["daemonwd"], logPath, scrubbedEnv(h.dirs["home"]))
}

func (h *deviceE2EHarness) waitReady(t *testing.T) {
	t.Helper()
	for _, ch := range h.channels {
		waitDaemonOnline(t, h.api, ch.id, h.daemonID)
	}
}

func chatChannel(t *testing.T, ch deviceE2EChannel, text string) {
	t.Helper()
	payload := json.RawMessage(fmt.Sprintf(`{"text":%q}`, text))
	_, terminal := submitAndAwaitTerminal(t, ch.ws, "loop.chat", payload, 120*time.Second)
	_ = assertCanonicalChat(t, terminal, payload)
}

func TestDaemonOneCarrierServesTwoChannels(t *testing.T) {
	h := newDeviceE2EHarness(t, 2)
	daemon := h.startDaemon(t, 1)
	h.waitReady(t)
	chatChannel(t, h.channels[0], "channel A")
	chatChannel(t, h.channels[1], "channel B")
	if daemon.exited() {
		t.Fatal("shared device process exited while both compartments were serving")
	}
}

func TestDaemonDetachOneChannelDoesNotAffectOther(t *testing.T) {
	h := newDeviceE2EHarness(t, 2)
	daemon := h.startDaemon(t, 1)
	h.waitReady(t)
	chatChannel(t, h.channels[1], "B before A detach")
	h.api.must("DELETE", "/api/channels/"+h.channels[0].id+"/daemons/"+h.daemonID,
		nil, http.StatusOK)
	waitDaemonDetached(t, h.api, h.channels[0].id, h.daemonID)
	waitDaemonOnline(t, h.api, h.channels[1].id, h.daemonID)
	chatChannel(t, h.channels[1], "B after A detach")
	if daemon.exited() {
		t.Fatal("detaching A terminated the carrier serving B")
	}
}

// TestDaemonCarrierReconnectRestoresCompartments is the zero-legacy black-box seam: channel
// composition changes enter only as authenticated subject frames, while the
// real daemon binary pulls that plan, builds the remote actor, serves work and
// reconnects after a hard process death.
func TestDaemonCarrierReconnectRestoresCompartments(t *testing.T) {
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
	channelRow := api.must("POST", "/api/channels", map[string]any{"name": "home"}, http.StatusCreated)
	channelID, _ := channelRow["id"].(string)
	// Daemon identity is realm truth; its channel binding is a separate SysOp
	// committed inside the channel membrane.
	daemonRow := api.must("POST", "/api/daemons", map[string]any{"name": "canonical-box"}, http.StatusCreated)
	daemonID, _ := daemonRow["id"].(string)
	apiKey, _ := daemonRow["api_key"].(string)
	if channelID == "" || daemonID == "" || apiKey == "" {
		t.Fatalf("incomplete channel/daemon identity: channel=%q daemon=%q key=%t", channelID, daemonID, apiKey != "")
	}
	api.must("POST", "/api/channels/"+channelID+"/daemons", map[string]any{"daemon_id": daemonID}, http.StatusOK)

	ws := dialWS(t, base, api.cookieHeader(), channelID, 0)
	defer ws.close()
	echoDecl := api.must("POST", "/api/actor-decls", map[string]any{
		"name": "echo-tool", "class": "echo", "visibility": "public",
	}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	echoResult := canonicalControl(t, ws, "channel.introduce_actor", map[string]any{
		"decl_id": echoDeclID, "placement": "server",
	})
	echoID, _ := echoResult["instance_id"].(string)
	if echoID == "" {
		t.Fatalf("introduce echo returned no instance_id: %v", echoResult)
	}

	assistantDecl := api.must("POST", "/api/actor-decls", map[string]any{
		"name": "assistant", "class": "script", "visibility": "public",
		"config": map[string]any{"tool_id": echoID},
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
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute", port),
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

func TestDaemonQueryCredentialRejectedWithoutLoggingSecret(t *testing.T) {
	binDir := requireE2EBin(t)
	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "channels", "home", "logs")
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	logPath := filepath.Join(dirs["logs"], "server.log")
	server := startProc(t, "query-auth-server", filepath.Join(binDir, "atoll-server"), []string{
		"-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-db", filepath.Join(root, "app.db"),
		"-channel-db-dir", dirs["channels"],
		"-init",
	}, dirs["serverwd"], logPath, scrubbedEnv(dirs["home"]))
	waitHealthz(t, base, server, 30*time.Second)

	const secret = "query-secret-must-never-enter-logs"
	response, err := http.Get(base + "/compute?key=" + secret)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("query credential status=%d, want 401", response.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), secret) {
		t.Fatal("query credential bytes leaked into server logs")
	}
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
	t.Fatalf("daemon %s did not become ready for channel %s", daemonID, channelID)
}

func waitDaemonDetached(t *testing.T, api *apiClient, channelID, daemonID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, body := api.do("GET", "/api/channels/"+channelID+"/daemons", nil)
		rows, _ := body["daemons"].([]any)
		found := false
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			found = found || row["id"] == daemonID
		}
		if !found {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("daemon %s remained bound to channel %s", daemonID, channelID)
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
