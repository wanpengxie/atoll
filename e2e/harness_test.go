// Package e2e is the portal-dialect system harness. It imports no Atoll
// package and drives only the shipped binaries, HTTP, websocket frames, and
// process signals.
//
// The retired Codex-driver scenarios are intentionally not inherited: they
// exercised the removed /api channel/declaration/control resources and real
// provider behaviour, neither of which is a portal contract now. Message,
// identity, and device journeys use the production membrane and registrar
// paths rather than hidden database reads.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const (
	rootEmail    = "root@atoll.local"
	rootPassword = "e2e-root-password"
	c0ChannelID  = "c0"
	systemActor  = "system"
)

var e2eBinDir string

func TestMain(m *testing.M) {
	if configured := os.Getenv("ATOLL_E2E_BIN"); configured != "" {
		e2eBinDir = configured
		os.Exit(m.Run())
	}
	root, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "atoll-e2e-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	e2eBinDir = dir
	for _, target := range []struct {
		name string
		pkg  string
	}{{"atoll-server", "./cmd/server"}, {"atoll-daemon", "./cmd/daemon"}} {
		cmd := exec.Command("go", "build", "-race", "-o", filepath.Join(dir, target.name), target.pkg)
		cmd.Dir = root
		cmd.Env = os.Environ()
		if out, buildErr := cmd.CombinedOutput(); buildErr != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", target.name, buildErr, out)
			_ = os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("e2e: repository root not found from %s", dir)
		}
		dir = parent
	}
}

type proc struct {
	name    string
	cmd     *exec.Cmd
	logPath string
	done    chan struct{}
	once    sync.Once
}

func startProc(t *testing.T, name, binary string, args, env []string, dir, logPath string) *proc {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("%s: open log: %v", name, err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("%s: start: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, logPath: logPath, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(p.done)
	}()
	t.Cleanup(p.reclaim)
	return p
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *proc) kill9(t *testing.T) {
	t.Helper()
	p.reclaim()
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: process group was not reclaimed", p.name)
	}
}

func (p *proc) reclaim() {
	p.once.Do(func() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
		}
	})
}

func tailLog(path string, lines int) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "(log unavailable: " + err.Error() + ")"
	}
	parts := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

type harness struct {
	t          *testing.T
	root       string
	serverHome string
	daemonHome string
	base       string
	port       int
	env        []string
	server     *proc
	serverGen  int
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	childHome := filepath.Join(root, "home")
	for _, dir := range []string{childHome, filepath.Join(root, "work"), filepath.Join(root, "logs")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	port := freePort(t)
	h := &harness{
		t: t, root: root,
		serverHome: filepath.Join(root, "server"),
		daemonHome: filepath.Join(root, "daemon"),
		base:       fmt.Sprintf("http://127.0.0.1:%d", port),
		port:       port,
		env:        []string{"PATH=" + os.Getenv("PATH"), "HOME=" + childHome},
	}
	h.startServer()
	return h
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate loopback port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func (h *harness) startServer() {
	h.t.Helper()
	h.serverGen++
	logPath := filepath.Join(h.root, "logs", fmt.Sprintf("server-%d.log", h.serverGen))
	h.server = startProc(h.t, fmt.Sprintf("server-%d", h.serverGen), filepath.Join(e2eBinDir, "atoll-server"), []string{
		"--home", h.serverHome,
		"--addr", fmt.Sprintf("127.0.0.1:%d", h.port),
		"--root-password", rootPassword,
		"--open-registration", // e2e registers users; nodes are closed by default
	}, h.env, filepath.Join(h.root, "work"), logPath)
	if err := waitHealth(h.base, h.server, 40*time.Second); err != nil {
		h.t.Fatalf("%v\nserver log:\n%s", err, tailLog(logPath, 100))
	}
}

func (h *harness) restartServer() {
	h.t.Helper()
	h.server.kill9(h.t)
	h.startServer()
}

func waitHealth(base string, p *proc, timeout time.Duration) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("%s exited before healthz", p.name)
		}
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s did not become healthy within %s", p.name, timeout)
}

type apiClient struct {
	t    *testing.T
	base string
	http *http.Client
}

func newAPIClient(t *testing.T, base string) *apiClient {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &apiClient{t: t, base: base, http: &http.Client{Jar: jar, Timeout: 10 * time.Second}}
}

func (a *apiClient) request(method, path string, body any, want int) map[string]any {
	a.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			a.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, a.base+path, reader)
	if err != nil {
		a.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.http.Do(req)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var value map[string]any
	_ = json.Unmarshal(raw, &value)
	if resp.StatusCode != want {
		a.t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, resp.StatusCode, want, raw)
	}
	return value
}

func (a *apiClient) cookieHeader() string {
	u, _ := url.Parse(a.base)
	var out []string
	for _, cookie := range a.http.Jar.Cookies(u) {
		out = append(out, cookie.Name+"="+cookie.Value)
	}
	return strings.Join(out, "; ")
}

func (a *apiClient) register(id, email, password string) map[string]any {
	return a.request(http.MethodPost, "/api/identity/register", map[string]any{
		"id": id, "email": email, "password": password, "display_name": "E2E User",
	}, http.StatusCreated)
}

func (a *apiClient) login(email, password string) map[string]any {
	return a.request(http.MethodPost, "/api/identity/login", map[string]string{
		"email": email, "password": password,
	}, http.StatusOK)
}

type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	acks chan map[string]any
	feed chan map[string]any
	done chan struct{}
	once sync.Once
}

func dialWS(t *testing.T, base, cookie string, since map[string]int64) *wsClient {
	t.Helper()
	headers := http.Header{}
	if cookie != "" {
		headers.Set("Cookie", cookie)
	}
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial %s: status=%d err=%v", wsURL, status, err)
	}
	client := &wsClient{
		t: t, conn: conn,
		acks: make(chan map[string]any, 128),
		feed: make(chan map[string]any, 2048),
		done: make(chan struct{}),
	}
	go client.readLoop()
	t.Cleanup(client.close)
	if err := conn.WriteJSON(wireFrame("attach", "attach", map[string]any{"since": since})); err != nil {
		t.Fatal(err)
	}
	ack := client.awaitAck("attach", 10*time.Second)
	if ack["frame_type"] != "receipt" {
		t.Fatalf("attach rejected: %v", ack)
	}
	payload, _ := ack["payload"].(map[string]any)
	if payload["contract_version"] == "" {
		t.Fatalf("attach receipt omitted contract version: %v", ack)
	}
	return client
}

func wireFrame(frameType, ref string, payload any) map[string]any {
	frame := map[string]any{"v": 2, "frame_type": frameType}
	if ref != "" {
		frame["ref"] = ref
	}
	if payload != nil {
		frame["payload"] = payload
	}
	return frame
}

func (c *wsClient) readLoop() {
	defer close(c.done)
	for {
		var frame map[string]any
		if err := c.conn.ReadJSON(&frame); err != nil {
			return
		}
		switch frame["frame_type"] {
		case "receipt", "error":
			c.acks <- frame
		case "feed":
			if payload, ok := frame["payload"].(map[string]any); ok {
				c.feed <- payload
			}
		}
	}
}

func (c *wsClient) close() { c.once.Do(func() { _ = c.conn.Close() }) }

func (c *wsClient) awaitAck(ref string, timeout time.Duration) map[string]any {
	c.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case frame := <-c.acks:
			if frame["ref"] == ref {
				return frame
			}
		case <-c.done:
			c.t.Fatalf("websocket closed while awaiting ref %s", ref)
		case <-timer.C:
			c.t.Fatalf("no websocket ack for ref %s within %s", ref, timeout)
		}
	}
}

func (c *wsClient) awaitEnvelope(match func(map[string]any) bool, timeout time.Duration) map[string]any {
	c.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case item := <-c.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope != nil && match(envelope) {
				return envelope
			}
		case <-c.done:
			c.t.Fatal("websocket closed while awaiting feed")
		case <-timer.C:
			c.t.Fatalf("no matching feed envelope within %s", timeout)
		}
	}
}

var wireRef int

func (c *wsClient) submit(channelID, msgType, kind string, audience []string, payload any) string {
	c.t.Helper()
	wireRef++
	ref := fmt.Sprintf("submit-%d", wireRef)
	body := map[string]any{
		"channel_id": channelID,
		"msg_type":   msgType,
		"kind":       kind,
		"visibility": "public",
		"payload":    payload,
	}
	if len(audience) > 0 {
		body["audience"] = audience
	}
	if err := c.conn.WriteJSON(wireFrame("submit", ref, body)); err != nil {
		c.t.Fatal(err)
	}
	ack := c.awaitAck(ref, 10*time.Second)
	if ack["frame_type"] == "error" {
		c.t.Fatalf("submit %s rejected: %v", msgType, ack["payload"])
	}
	receipt, _ := ack["payload"].(map[string]any)
	id, _ := receipt["message_id"].(string)
	if id == "" {
		c.t.Fatalf("submit receipt omitted message_id: %v", ack)
	}
	return id
}

func (c *wsClient) request(channelID, msgType, audience string, payload any) map[string]any {
	c.t.Helper()
	_, body, err := c.tryRequest(channelID, msgType, audience, payload)
	if err != nil {
		c.t.Fatalf("request %s failed: %v", msgType, err)
	}
	return body
}

func (c *wsClient) resource(payload map[string]any) map[string]any {
	c.t.Helper()
	out, err := c.tryResource(payload)
	if err != nil {
		c.t.Fatal(err)
	}
	return out
}

func (c *wsClient) tryResource(payload map[string]any) (map[string]any, error) {
	wireRef++
	ref := fmt.Sprintf("resource-%d", wireRef)
	if err := c.conn.WriteJSON(wireFrame("resource", ref, payload)); err != nil {
		return nil, err
	}
	ack := c.awaitAck(ref, 10*time.Second)
	if ack["frame_type"] == "error" {
		return nil, fmt.Errorf("resource request rejected: %v", ack["payload"])
	}
	out, _ := ack["payload"].(map[string]any)
	if out["status"] != "ok" {
		return out, fmt.Errorf("resource outcome=%v", out)
	}
	return out, nil
}

func (c *wsClient) requestWithID(channelID, msgType, audience string, payload any) (string, map[string]any) {
	c.t.Helper()
	id, body, err := c.tryRequest(channelID, msgType, audience, payload)
	if err != nil {
		c.t.Fatalf("request %s failed: %v", msgType, err)
	}
	return id, body
}

func (c *wsClient) tryRequest(channelID, msgType, audience string, payload any) (string, map[string]any, error) {
	c.t.Helper()
	wireRef++
	ref := fmt.Sprintf("submit-%d", wireRef)
	body := map[string]any{
		"channel_id": channelID,
		"msg_type":   msgType,
		"kind":       "request",
		"visibility": "public",
		"payload":    payload,
		"audience":   []string{audience},
	}
	if err := c.conn.WriteJSON(wireFrame("submit", ref, body)); err != nil {
		return "", nil, err
	}
	ack := c.awaitAck(ref, 10*time.Second)
	if ack["frame_type"] == "error" {
		return "", nil, fmt.Errorf("submit rejected: %v", ack["payload"])
	}
	receipt, _ := ack["payload"].(map[string]any)
	id, _ := receipt["message_id"].(string)
	if id == "" {
		return "", nil, fmt.Errorf("submit receipt omitted message_id: %v", ack)
	}
	terminal := c.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" || envelope["parent_id"] != id {
			return false
		}
		body, _ := envelope["payload"].(map[string]any)
		return body["status"] == "completed" || body["status"] == "failed"
	}, 30*time.Second)
	terminalBody, _ := terminal["payload"].(map[string]any)
	if terminalBody["status"] != "completed" {
		return id, terminalBody, fmt.Errorf("terminal=%v", terminalBody)
	}
	return id, terminalBody, nil
}

func rootClient(t *testing.T, h *harness, since map[string]int64) (*apiClient, *wsClient) {
	t.Helper()
	api := newAPIClient(t, h.base)
	login := api.login(rootEmail, rootPassword)
	if login["id"] != "root" {
		t.Fatalf("root login=%v", login)
	}
	return api, dialWS(t, h.base, api.cookieHeader(), since)
}

func findRegistrar(t *testing.T, ws *wsClient) string {
	t.Helper()
	// The protocol deliberately exposes the registrar by its two-segment
	// address; the engine resolves it to the sole active three-segment member.
	return "system:registrar"
}

// awaitDoor waits for a newly created channel's intrinsic system door to serve.
func awaitDoor(t *testing.T, ws *wsClient, channelID string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		_, _, err := ws.tryRequest(channelID, "system.member.list", systemActor, map[string]any{})
		if err == nil {
			return systemActor
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	// 把最后一次的真实原因带出来：只报"没等到"恒不足以定位——CI 上红了两天，
	// 而日志里除了这句什么都没有。
	t.Fatalf("channel %s never served its system door; last error: %v", channelID, last)
	return ""
}

// attachDevice binds a device to a channel. Binding is requested from inside
// the target channel through that channel's own door: the registrar in
// c0 cannot bind on another channel's behalf.
func attachDevice(t *testing.T, ws *wsClient, channelID, deviceID string) {
	t.Helper()
	door := awaitDoor(t, ws, channelID)
	registrarRequest(t, ws, channelID, door, "system.device.attach", map[string]any{
		"channel_id": channelID, "device_id": deviceID,
	})
}

// Registrar replies expose exactly one value field. channelID and target let
// the same helper cover direct c0 calls and membrane-routed child calls.
func registrarRequest(t *testing.T, ws *wsClient, channelID, target, word string, payload any) map[string]any {
	t.Helper()
	reply := ws.request(channelID, word, target, payload)
	value, _ := reply["value"].(map[string]any)
	if value == nil {
		t.Fatalf("registrar %s reply omitted value: %v", word, reply)
	}
	return value
}

func stringField(t *testing.T, row map[string]any, field string) string {
	t.Helper()
	value, _ := row[field].(string)
	if value == "" {
		t.Fatalf("%s missing from %v", field, row)
	}
	return value
}

// rootActorID returns root's complete three-segment actor id in a channel.
//
// system.channel.create requires initial_actor_ids and恒不接受两段地址: the
// seat carried into the new channel has to name an actual current member, and
// an incarnation is part of that name. Tests therefore have to look it up
// rather than spell it.
func rootActorID(t *testing.T, ws *wsClient, channelID string) string {
	t.Helper()
	catalog := ws.request(channelID, "system.member.list", systemActor, map[string]any{})
	rows, _ := catalog["actors"].([]any)
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		id, _ := row["id"].(string)
		if strings.HasPrefix(id, "human:root:") {
			return id
		}
	}
	t.Fatalf("root has no actor in channel %s: %v", channelID, catalog)
	return ""
}

// createChannelWithRoot creates a child channel and carries root into it.
//
// Two shapes exist and they are not interchangeable. The DOOR takes
// initial_actor_ids (plain current-channel actor ids) and resolves them; the
// REGISTRAR takes an already-resolved plan, initial_seats, because by the time
// a request reaches it the source channel is no longer available to look
// anything up in. Tests speak to both, so this helper covers the registrar
// path and callers that go through a channel door pass initial_actor_ids.
//
// Carrying root is not incidental: every one of these tests then speaks to the
// new channel's door as root, and a channel恒不认得非成员 (forbidden: no
// eligibility for channel).
func createChannelWithRoot(t *testing.T, ws *wsClient, parentID, registrar, name string) map[string]any {
	t.Helper()
	return registrarRequest(t, ws, parentID, registrar, "system.channel.create", map[string]any{
		"name": name,
		"initial_seats": []any{map[string]any{
			"source_actor_id": rootActorID(t, ws, parentID),
			"kind":            "human",
			"principal":       "root",
		}},
	})
}

// waitActorPresence polls system.member.list until actorID's presence matches want,
// failing early if the daemon expected to host it has already exited.
func waitActorPresence(t *testing.T, ws *wsClient, actorID string, want bool, daemon *proc, logPath string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if daemon != nil && daemon.exited() {
			t.Fatalf("daemon exited while waiting for actor presence=%v\n%s", want, tailLog(logPath, 100))
		}
		catalog := ws.request(c0ChannelID, "system.member.list", systemActor, map[string]any{})
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
