package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestCodexDriverMultiTurnMemory is the opt-in, real-binary smoke lane. It
// exercises `atoll up` provisioning (including the provisioned Codex default),
// two-turn thread memory, restart/resume, stop, terminate and lazy recovery.
// Mock protocol tests remain the deterministic CI lane; this test deliberately
// requires both an explicit opt-in and a locally authenticated Codex install.
func TestCodexDriverMultiTurnMemory(t *testing.T) {
	if os.Getenv("ATOLL_CODEX_E2E") != "1" {
		t.Skip("ATOLL_CODEX_E2E=1 not set")
	}
	binDir := requireE2EBin(t)
	atollBin := filepath.Join(binDir, "atoll")
	if _, err := os.Stat(atollBin); err != nil {
		t.Fatalf("atoll binary missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(binDir, "atoll-server")); err != nil {
		t.Fatalf("atoll-server binary missing: %v", err)
	}

	root := t.TempDir()
	home := filepath.Join(root, "isolated-home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	copyCodexAuthForSmoke(t, home)
	env := scrubbedEnv(home)
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		env = append(env, "OPENAI_API_KEY="+key)
	}
	env = append(env, "CODEX_HOME="+filepath.Join(home, ".codex"))

	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	nodeDir := filepath.Join(root, "node")
	logPath := filepath.Join(root, "atoll.log")
	p := startProc(t, "codex-atoll", atollBin, []string{
		"up", "--dir", nodeDir, "--addr", fmt.Sprintf("127.0.0.1:%d", port),
	}, root, logPath, env)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("atoll log tail:\n%s", tailLog(logPath, 120))
		}
	})
	waitHealthz(t, base, p, 45*time.Second)

	tokenPath := filepath.Join(nodeDir, "server", "atoll-token")
	token := waitTextFile(t, tokenPath, 15*time.Second)
	api := newAPIClient(t, base)
	api.bearer = token
	channels := api.must("GET", "/api/channels", nil, http.StatusOK)
	var homeID string
	rows, ok := channels["channels"].([]any)
	if !ok {
		t.Fatalf("invalid channel list: %v", channels)
	}
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		if row["name"] == "home" {
			homeID, _ = row["id"].(string)
		}
	}
	if homeID == "" {
		t.Fatalf("provisioned home missing: %v", channels)
	}
	ws := dialWSBearer(t, base, token, homeID)

	marker := fmt.Sprintf("ATOLL-%d", time.Now().UnixNano())
	_, first := submitAndAwaitTerminal(t, ws, "user.text", json.RawMessage(fmt.Sprintf(`{"text":%q}`, "Remember this exact marker: "+marker+". Reply with the marker only.")), 3*time.Minute)
	if !strings.Contains(payloadField(first, "text"), marker) {
		t.Fatalf("first Codex answer did not echo marker: %v", first["payload"])
	}
	_, second := submitAndAwaitTerminal(t, ws, "user.text", json.RawMessage(`{"text":"What exact marker did I ask you to remember? Reply with it only."}`), 3*time.Minute)
	if !strings.Contains(payloadField(second, "text"), marker) {
		t.Fatalf("second turn lost thread memory: %v", second["payload"])
	}

	for _, control := range []string{"agent.restart", "agent.stop", "agent.terminate"} {
		_, terminal := submitAndAwaitTerminal(t, ws, control, json.RawMessage(`{}`), 2*time.Minute)
		if terminalStatus(terminal) != "completed" {
			t.Fatalf("%s terminal=%v", control, terminal)
		}
	}
	_, recovered := submitAndAwaitTerminal(t, ws, "user.text", json.RawMessage(`{"text":"Reply exactly: recovered"}`), 3*time.Minute)
	if !strings.Contains(strings.ToLower(payloadField(recovered, "text")), "recovered") {
		t.Fatalf("lazy recovery answer=%v", recovered["payload"])
	}
}

func dialWSBearer(t *testing.T, base, token, chID string) *wsClient {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+token)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(base, "http")+"/ws", hdr)
	if err != nil {
		t.Fatalf("dial bearer ws: %v", err)
	}
	c := &wsClient{t: t, conn: conn, chID: chID, tail: make(chan map[string]any, 16384), acks: make(chan map[string]any, 256), done: make(chan struct{})}
	t.Cleanup(c.close)
	go c.readLoop()
	ref := "attach-" + chID
	if err := conn.WriteJSON(frame("attach", ref, map[string]any{"since": map[string]int64{chID: 0}})); err != nil {
		t.Fatal(err)
	}
	if rec, ok := c.awaitRef(ref, 10*time.Second); !ok || rec["frame_type"] != "receipt" {
		t.Fatalf("attach not accepted: %v", rec)
	}
	return c
}

func waitTextFile(t *testing.T, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(raw)) != "" {
			return strings.TrimSpace(string(raw))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("file did not become readable: %s", path)
	return ""
}

func copyCodexAuthForSmoke(t *testing.T, isolatedHome string) {
	t.Helper()
	sourceHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(isolatedHome, ".codex")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	copied := false
	for _, name := range []string{"auth.json", "config.toml"} {
		raw, err := os.ReadFile(filepath.Join(sourceHome, ".codex", name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("read Codex %s: %v", name, err)
		}
		mode := os.FileMode(0o600)
		if name == "config.toml" {
			mode = 0o600
		}
		if err := os.WriteFile(filepath.Join(dst, name), raw, mode); err != nil {
			t.Fatalf("copy Codex %s: %v", name, err)
		}
		if name == "auth.json" {
			copied = true
		}
	}
	if !copied && os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("no ~/.codex/auth.json or OPENAI_API_KEY available for real Codex smoke")
	}
}
