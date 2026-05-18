//go:build e2e

// Package harness spins up a fresh coagent stack (server + daemon)
// as subprocesses for end-to-end smoke tests.
//
// Build tag: `e2e` — these tests do not run under the default
// `go test ./...`; the make target `e2e-smoke` enables the tag.
//
// The harness is deliberately a thin wrapper around real binaries
// rather than reaching into Go-level fixtures: every wiring bug the
// owner has hit recently (write deadline, ack frame_id pairing,
// WS ping/pong, response shape) was invisible to in-process unit
// tests because those tests never exercise the actual subprocess
// boundary. The harness pays the cost of spawning + polling so the
// suite catches a class of integration bugs that single-binary
// unit tests cannot.
package harness

import (
	"bytes"
	"context"
	"database/sql"
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
	_ "modernc.org/sqlite"
)

const (
	// Default poll cadence + total timeout for Eventually helpers.
	defaultPollInterval = 50 * time.Millisecond
	defaultPollTimeout  = 10 * time.Second

	// daemonSecret / sessionSecret / deviceSecret / humanSecret are
	// fixed test sentinels. Real secrets are loaded from environment
	// in production; here we want determinism + reproducible failures.
	daemonSecret  = "e2e-daemon-secret-fixed-for-tests"
	sessionSecret = "e2e-session-secret-fixed-for-tests"
	deviceSecret  = "e2e-device-secret-fixed-for-tests"
	humanSecret   = "e2e-human-secret-fixed-for-tests"

	// daemonID — the test stack uses a single daemon registered under
	// this stable identifier. Multi-daemon scenarios will need to
	// extend the harness; the M1 smoke pass uses one.
	daemonID = "daemon-e2e"
)

// Stack owns the running server + daemon processes for the lifetime
// of one test. Tests obtain it via Start, then exercise it via the
// helper methods + Client(). Stop tears everything down.
type Stack struct {
	t   *testing.T
	ctx context.Context

	RepoRoot    string
	WorkDir     string
	ServerDB    string
	DataDir     string
	ChannelsDir string

	ServerPort int
	ServerURL  string
	WSURL      string

	server *exec.Cmd
	daemon *exec.Cmd

	serverStdout *teeBuf
	serverStderr *teeBuf
	daemonStdout *teeBuf
	daemonStderr *teeBuf

	client    *http.Client
	once      sync.Once
	stoppedCh chan struct{}

	// Bookkeeping so subsequent helpers can derive paths /
	// authenticated requests without re-passing IDs.
	mu          sync.Mutex
	currentUser registeredUser
	workspaces  map[string]string // name → id
	channels    map[string]string // name → id
}

// registeredUser is the result of a Register + Login pair.
type registeredUser struct {
	ID    string
	Email string
}

// Options tunes optional harness behaviour. All fields are optional;
// zero-value Options yields the default smoke configuration.
type Options struct {
	// Verbose=true streams server / daemon stdout+stderr to the test
	// log on stop. Default false (only logged on failure via t.Logf in
	// Stop's defer). Set via E2E_VERBOSE=1.
	Verbose bool
}

// Start launches a fresh stack with random ports + tmp data dir. The
// returned Stack is registered with t.Cleanup so the test does not
// need to defer Stop explicitly. Fatal if any subprocess fails to
// reach a usable state inside the start-up budget.
func Start(t *testing.T, opts Options) *Stack {
	t.Helper()

	if os.Getenv("E2E_VERBOSE") == "1" {
		opts.Verbose = true
	}

	repoRoot := mustRepoRoot(t)
	serverBin := filepath.Join(repoRoot, "bin", "coagent-server")
	daemonBin := filepath.Join(repoRoot, "bin", "coagent-daemon")
	workerBin := filepath.Join(repoRoot, "bin", "coagent-worker")
	for _, p := range []string{serverBin, daemonBin, workerBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("harness: binary missing at %s — run `make build-go` first (%v)", p, err)
		}
	}

	work := t.TempDir()
	serverDB := filepath.Join(work, "server.db")
	dataDir := filepath.Join(work, "daemon-data")
	channelsDir := filepath.Join(dataDir, "channels")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("harness: mkdir data: %v", err)
	}

	serverPort := mustFreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s := &Stack{
		t:           t,
		ctx:         ctx,
		RepoRoot:    repoRoot,
		WorkDir:     work,
		ServerDB:    serverDB,
		DataDir:     dataDir,
		ChannelsDir: channelsDir,
		ServerPort:  serverPort,
		ServerURL:   fmt.Sprintf("http://127.0.0.1:%d", serverPort),
		WSURL:       fmt.Sprintf("ws://127.0.0.1:%d/ws", serverPort),
		workspaces:  make(map[string]string),
		channels:    make(map[string]string),
		stoppedCh:   make(chan struct{}),
	}
	s.client = buildClient(t)

	s.startServer(serverBin)
	s.waitHealthy()
	s.startDaemon(daemonBin, workerBin)
	s.waitDaemonRegistered()

	t.Cleanup(func() { s.Stop() })
	return s
}

// Stop sends SIGINT to both processes and waits for clean exit.
// Idempotent — safe to call from t.Cleanup even after an earlier
// explicit Stop. The first call captures any wait() error; subsequent
// calls are no-ops.
func (s *Stack) Stop() {
	s.once.Do(func() {
		// Send SIGINT to both. Server has 10s shutdown budget, daemon
		// drains its dispatcher in supervisor.Shutdown — give each
		// 15s before SIGKILL.
		for _, c := range []*exec.Cmd{s.daemon, s.server} {
			if c == nil || c.Process == nil {
				continue
			}
			_ = c.Process.Signal(syscall.SIGINT)
		}

		shutdownDeadline := time.Now().Add(15 * time.Second)
		for _, c := range []*exec.Cmd{s.daemon, s.server} {
			if c == nil {
				continue
			}
			done := make(chan error, 1)
			go func(cc *exec.Cmd) { done <- cc.Wait() }(c)
			select {
			case <-done:
			case <-time.After(time.Until(shutdownDeadline)):
				_ = c.Process.Kill()
				<-done
			}
		}
		close(s.stoppedCh)

		// Surface logs on test failure so triage doesn't require
		// re-running with E2E_VERBOSE=1.
		if s.t.Failed() {
			s.t.Logf("=== server stdout ===\n%s", s.serverStdout.String())
			s.t.Logf("=== server stderr ===\n%s", s.serverStderr.String())
			s.t.Logf("=== daemon stdout ===\n%s", s.daemonStdout.String())
			s.t.Logf("=== daemon stderr ===\n%s", s.daemonStderr.String())
		}
	})
}

// Client returns the cookie-jar-backed http.Client. Cookies set by
// Login are reused for subsequent requests automatically.
func (s *Stack) Client() *http.Client { return s.client }

// ChannelSqlitePath returns the absolute path to a channel's local
// daemon-side sqlite file. Test assertions on stored envelopes hit
// this file directly with database/sql.
func (s *Stack) ChannelSqlitePath(channelID string) string {
	return filepath.Join(s.ChannelsDir, channelID, "channel.sqlite")
}

// ServerDBPath returns the server-side sqlite path (placements,
// view_cache_messages, daemons, identity).
func (s *Stack) ServerDBPath() string { return s.ServerDB }

// ----------------------------------------------------------------------
// startup helpers
// ----------------------------------------------------------------------

func (s *Stack) startServer(bin string) {
	args := []string{
		"--addr", fmt.Sprintf("127.0.0.1:%d", s.ServerPort),
		"--db", s.ServerDB,
		"--allow-dev-secrets",
	}
	cmd := exec.CommandContext(s.ctx, bin, args...)
	cmd.Env = append(os.Environ(),
		"COAGENT_SESSION_SECRET="+sessionSecret,
		"COAGENT_DAEMON_SECRET="+daemonSecret,
		"COAGENT_DEVICE_SECRET="+deviceSecret,
		"COAGENT_HUMAN_SECRET="+humanSecret,
		"COAGENT_GIN_MODE=release",
		"COAGENT_LOG_LEVEL=info",
	)
	s.serverStdout = &teeBuf{}
	s.serverStderr = &teeBuf{}
	cmd.Stdout = s.serverStdout
	cmd.Stderr = s.serverStderr
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("harness: server start: %v", err)
	}
	s.server = cmd
}

func (s *Stack) startDaemon(daemonBin, workerBin string) {
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/daemonbus", s.ServerPort)
	args := []string{
		"--data-dir", s.DataDir,
		"--daemon-id", daemonID,
		"--server-url", wsURL,
		"--key", daemonSecret,
		"--human-caller-secret", humanSecret,
		"--worker-bin", workerBin,
		"--worker-provider", "mock",
		"--replay-window-ms", "300000",
	}
	cmd := exec.CommandContext(s.ctx, daemonBin, args...)
	cmd.Env = append(os.Environ(),
		"COAGENT_DATA_DIR="+s.DataDir,
		"COAGENT_LOG_LEVEL=info",
		// Force every spawned mock worker into single-shot mode so the
		// "exactly 1 agent.text per trigger with next_action=done"
		// contract holds without per-test plumbing.
		"COAGENT_MOCK_SINGLE_SHOT=1",
		"COAGENT_MOCK_REPLY_TEXT=pong",
	)
	s.daemonStdout = &teeBuf{}
	s.daemonStderr = &teeBuf{}
	cmd.Stdout = s.daemonStdout
	cmd.Stderr = s.daemonStderr
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("harness: daemon start: %v", err)
	}
	s.daemon = cmd
}

func (s *Stack) waitHealthy() {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if s.server.ProcessState != nil && s.server.ProcessState.Exited() {
			s.t.Fatalf("harness: server exited early: %s", s.serverStderr.String())
		}
		resp, err := s.client.Get(s.ServerURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatalf("harness: server /healthz never green within 15s\nstderr:\n%s", s.serverStderr.String())
}

// waitDaemonRegistered polls server.db `daemons` table until the
// expected daemonID row exists with key_hash filled in. That row is
// only created when the daemon's WS handshake succeeds, so it is the
// cheapest "daemon is up and connected" proxy.
func (s *Stack) waitDaemonRegistered() {
	deadline := time.Now().Add(20 * time.Second)
	db, err := sql.Open("sqlite", s.ServerDB)
	if err != nil {
		s.t.Fatalf("harness: open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	for time.Now().Before(deadline) {
		if s.daemon.ProcessState != nil && s.daemon.ProcessState.Exited() {
			s.t.Fatalf("harness: daemon exited early\nstdout:\n%s\nstderr:\n%s",
				s.daemonStdout.String(), s.daemonStderr.String())
		}
		var got string
		err := db.QueryRowContext(s.ctx,
			`SELECT id FROM daemons WHERE id=? AND key_hash != ''`, daemonID).Scan(&got)
		if err == nil && got == daemonID {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	s.t.Fatalf("harness: daemon never registered within 20s\nstdout:\n%s\nstderr:\n%s",
		s.daemonStdout.String(), s.daemonStderr.String())
}

// ----------------------------------------------------------------------
// API helpers — each returns (decoded body, http.Response). Fatal on
// transport / status mismatch.
// ----------------------------------------------------------------------

// Register + Login a fresh user. Returns the user_id. The cookie jar
// holds the resulting session token automatically.
func (s *Stack) RegisterAndLogin(email, password string) string {
	s.t.Helper()
	type regResp struct {
		ID string `json:"id"`
	}
	var rr regResp
	s.do("POST", "/api/identity/register", map[string]any{
		"email":    email,
		"password": password,
	}, http.StatusCreated, &rr)
	// Login sets the cookie.
	var loginResp struct {
		User struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	s.do("POST", "/api/identity/login", map[string]any{
		"email":    email,
		"password": password,
	}, http.StatusOK, &loginResp)

	s.mu.Lock()
	s.currentUser = registeredUser{ID: loginResp.User.ID, Email: email}
	s.mu.Unlock()
	return loginResp.User.ID
}

// CreateWorkspace creates a workspace owned by the current user.
// Returns workspace id.
func (s *Stack) CreateWorkspace(name string) string {
	s.t.Helper()
	var resp struct {
		ID string `json:"id"`
	}
	s.do("POST", "/api/workspaces", map[string]any{"name": name},
		http.StatusCreated, &resp)
	s.mu.Lock()
	s.workspaces[name] = resp.ID
	s.mu.Unlock()
	return resp.ID
}

// CreateChannel creates a channel of the given type in workspaceID.
// Returns channel id.
func (s *Stack) CreateChannel(workspaceID, name, channelType string) string {
	s.t.Helper()
	var resp struct {
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
	}
	s.do("POST", "/api/workspaces/"+workspaceID+"/channels",
		map[string]any{"name": name, "type": channelType},
		http.StatusCreated, &resp)
	s.mu.Lock()
	s.channels[name] = resp.Channel.ID
	s.mu.Unlock()
	return resp.Channel.ID
}

// BindChannel attaches a channel to the harness daemon. The handler
// returns 202 once the placement reservation is committed (the
// daemon-side create_channel ack is async). Wait for the channel
// sqlite file to appear before issuing writes.
func (s *Stack) BindChannel(workspaceID, channelID string) {
	s.t.Helper()
	s.do("POST", "/api/workspaces/"+workspaceID+"/channels/"+channelID+"/bind",
		map[string]any{"daemon_id": daemonID}, http.StatusAccepted, nil)

	// Block until the channel sqlite materialises on the daemon side
	// — that's the observable signal that create_channel finished.
	Eventually(s.t, "channel sqlite materialised", 10*time.Second, func() bool {
		_, err := os.Stat(s.ChannelSqlitePath(channelID))
		return err == nil
	})
}

// PostMessageResponse is the decoded /messages POST body.
type PostMessageResponse struct {
	FrameID       string `json:"frame_id"`
	DaemonAckID   string `json:"daemon_ack_id"`
	MessageID     string `json:"message_id"`
	Seq           int64  `json:"seq"`
	CorrelationID string `json:"correlation_id"`
	Accepted      bool   `json:"accepted"`
	Deduped       bool   `json:"deduped"`
	RejectReason  string `json:"reject_reason"`
}

// PostMessage submits a human-authored text message. envType is
// usually "human.text"; kindHint is forwarded as the optional `kind`
// field (empty = let server pick the L1 default).
func (s *Stack) PostMessage(channelID, envType, text, kindHint string) PostMessageResponse {
	s.t.Helper()
	payload, _ := json.Marshal(map[string]string{"text": text})
	body := map[string]any{
		"type":    envType,
		"payload": json.RawMessage(payload),
	}
	if kindHint != "" {
		body["kind"] = kindHint
	}
	var resp PostMessageResponse
	s.do("POST", "/api/channels/"+channelID+"/messages", body, http.StatusOK, &resp)
	return resp
}

// GetMessages returns the raw JSON body so tests can assert the
// envelope shape directly (the contract drift in bug #4 was hidden
// by typed unmarshal). Tests can json.Unmarshal a second time into
// their own shape when needed.
func (s *Stack) GetMessages(channelID string) []byte {
	s.t.Helper()
	return s.doRaw("GET", "/api/channels/"+channelID+"/messages", nil, http.StatusOK)
}

// DialPushWS connects to /ws as the currently logged-in user. The
// returned conn must be Close()'d by the caller.
//
// http.Header treats multiple "Cookie" entries as separate values
// (RFC 7230 list semantics); browsers/servers expect a SINGLE
// Cookie header with "; "-joined name=value pairs. cookiejar already
// stores the right set, so we serialize it manually.
func (s *Stack) DialPushWS() *websocket.Conn {
	s.t.Helper()
	// Build an http-equivalent URL to pull cookies from the jar.
	httpURL, _ := url.Parse(s.ServerURL)
	cookies := s.client.Jar.Cookies(httpURL)
	header := http.Header{}
	if len(cookies) > 0 {
		parts := make([]string, 0, len(cookies))
		for _, c := range cookies {
			parts = append(parts, c.Name+"="+c.Value)
		}
		header.Set("Cookie", strings.Join(parts, "; "))
	}
	ws, _, err := websocket.DefaultDialer.DialContext(s.ctx, s.WSURL, header)
	if err != nil {
		s.t.Fatalf("harness: ws dial: %v", err)
	}
	return ws
}

// RestartDaemon kills the daemon process and starts a fresh one with
// the same data dir + daemon-id. Tests rely on this to assert the
// reconnect path keeps placements healthy.
func (s *Stack) RestartDaemon() {
	s.t.Helper()
	if s.daemon == nil || s.daemon.Process == nil {
		s.t.Fatalf("harness: RestartDaemon called with no running daemon")
	}
	_ = s.daemon.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- s.daemon.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = s.daemon.Process.Kill()
		<-done
	}
	daemonBin := filepath.Join(s.RepoRoot, "bin", "coagent-daemon")
	workerBin := filepath.Join(s.RepoRoot, "bin", "coagent-worker")
	s.startDaemon(daemonBin, workerBin)
	s.waitDaemonRegistered()
}

// ----------------------------------------------------------------------
// internal HTTP plumbing
// ----------------------------------------------------------------------

func (s *Stack) do(method, path string, body any, wantStatus int, decode any) {
	s.t.Helper()
	raw := s.doRaw(method, path, body, wantStatus)
	if decode != nil {
		if err := json.Unmarshal(raw, decode); err != nil {
			s.t.Fatalf("harness: decode %s %s: %v (body=%s)", method, path, err, string(raw))
		}
	}
}

func (s *Stack) doRaw(method, path string, body any, wantStatus int) []byte {
	s.t.Helper()
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			s.t.Fatalf("harness: encode %s %s: %v", method, path, err)
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(s.ctx, method, s.ServerURL+path, reader)
	if err != nil {
		s.t.Fatalf("harness: build %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.t.Fatalf("harness: do %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		s.t.Fatalf("harness: %s %s status=%d want=%d body=%s",
			method, path, resp.StatusCode, wantStatus, string(raw))
	}
	return raw
}

// ----------------------------------------------------------------------
// utility primitives
// ----------------------------------------------------------------------

func buildClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("harness: cookiejar: %v", err)
	}
	return &http.Client{
		Jar:     jar,
		Timeout: 15 * time.Second,
	}
}

func mustFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("harness: bind free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// mustRepoRoot returns the absolute path of the repo root by walking
// up from the test cwd until go.mod is found. Cached because every
// helper call would otherwise stat repeatedly.
func mustRepoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("harness: getwd: %v", err)
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("harness: no go.mod found above %s", cwd)
		}
		dir = parent
	}
}

// Eventually polls fn until it returns true or timeout elapses. On
// failure, the fatal message includes the supplied label so test
// failures are immediately legible.
func Eventually(t *testing.T, label string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(defaultPollInterval)
	}
	t.Fatalf("harness: condition %q never true within %s", label, timeout)
}

// EventuallyValue polls fn until it returns (value, true). The
// value is returned for assertion. Same failure mode as Eventually.
func EventuallyValue[T any](t *testing.T, label string, timeout time.Duration, fn func() (T, bool)) T {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := fn(); ok {
			return v
		}
		time.Sleep(defaultPollInterval)
	}
	var zero T
	t.Fatalf("harness: condition %q never true within %s", label, timeout)
	return zero
}

// ----------------------------------------------------------------------
// channel sqlite assertion helpers
// ----------------------------------------------------------------------

// StoredMessage mirrors the subset of columns tests assert on.
type StoredMessage struct {
	Seq        int64
	ID         string
	Type       string
	Kind       string
	SenderKind string
	SenderID   string
	Payload    json.RawMessage
}

// OpenChannelDB opens a read-only handle to the per-channel sqlite.
// Caller is responsible for closing.
func (s *Stack) OpenChannelDB(channelID string) *sql.DB {
	s.t.Helper()
	path := s.ChannelSqlitePath(channelID)
	// mode=ro keeps the harness honest: tests assert state, never
	// mutate. file: prefix is required by modernc.org/sqlite when
	// using URI query params.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		s.t.Fatalf("harness: open channel sqlite %s: %v", path, err)
	}
	return db
}

// ListChannelMessages returns every row of the messages table in
// seq ASC order. Empty channel => empty slice (not nil).
func (s *Stack) ListChannelMessages(channelID string) []StoredMessage {
	s.t.Helper()
	db := s.OpenChannelDB(channelID)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(s.ctx, `
		SELECT seq, id, type, kind, sender_kind, sender_id, payload
		FROM messages ORDER BY seq ASC`)
	if err != nil {
		s.t.Fatalf("harness: list messages: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]StoredMessage, 0)
	for rows.Next() {
		var m StoredMessage
		var payload string
		if err := rows.Scan(&m.Seq, &m.ID, &m.Type, &m.Kind,
			&m.SenderKind, &m.SenderID, &payload); err != nil {
			s.t.Fatalf("harness: scan: %v", err)
		}
		m.Payload = json.RawMessage(payload)
		out = append(out, m)
	}
	return out
}

// ----------------------------------------------------------------------
// process output capture
// ----------------------------------------------------------------------

// teeBuf is a goroutine-safe bytes.Buffer wrapper used as Stdout /
// Stderr for the child processes. It lets the harness dump logs
// after a failure without racing the still-running writer goroutine.
type teeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (t *teeBuf) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.Write(p)
}

func (t *teeBuf) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}

// ContainsLine returns true if any captured line contains substr.
// Used by smoke tests to assert structured log breadcrumbs the
// harness already grep-targets (e.g. mock_bridge: domain_prompt_loaded).
func (t *teeBuf) ContainsLine(substr string) bool {
	return strings.Contains(t.String(), substr)
}

// DaemonStderrContains exposes the daemon stderr buffer to tests.
func (s *Stack) DaemonStderrContains(substr string) bool {
	return s.daemonStderr.ContainsLine(substr)
}

// DaemonStdoutContains exposes the daemon stdout buffer to tests.
func (s *Stack) DaemonStdoutContains(substr string) bool {
	return s.daemonStdout.ContainsLine(substr)
}
