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

	"github.com/google/uuid"
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

	opts Options

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

	// extraDaemons covers tests that spawn additional daemons beyond the
	// primary one (multi-daemon reclaim case). Map key is the daemon id.
	extraDaemons map[string]*daemonProc
}

// daemonProc bundles the per-daemon process + log buffers so multi-
// daemon tests can read either side's stderr on assertion failure.
type daemonProc struct {
	id      string
	dataDir string
	cmd     *exec.Cmd
	stdout  *teeBuf
	stderr  *teeBuf
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

	// DeviceAllowedOrigins forwards `--devicebus-allowed-origins` to the
	// server. Tests that need /devicebus WS handshakes (mock extension)
	// must pre-declare the chrome-extension://test-ext-id origin here.
	// Empty (the default) keeps the deny-all production posture.
	DeviceAllowedOrigins []string

	// FastReconcile shrinks the server-side placement reconcile knobs so
	// stale-eviction tests don't have to wait the 90s production
	// timeout. Wires --heartbeat-timeout=2s --reconcile-grace=1s
	// --create-timeout=5s when true. Default false (production timings).
	FastReconcile bool

	// SkipPrimaryDaemon=true starts the server alone — used by tests that
	// drive multiple daemons themselves via StartDaemon. The default
	// stack always includes a single primary daemon.
	SkipPrimaryDaemon bool

	// SharedDataDir overrides the per-stack tmpdir for daemon data. Used
	// by tests that want a deliberate ordering (kill daemon A → boot
	// daemon B against same data dir) so the boot reclaim path reads
	// the existing channel locks. Empty (default) = isolated tmpdir.
	SharedDataDir string

	// ExtraDaemonEnv is appended to the daemon subprocess env. Lets a
	// test enable e.g. COAGENT_MOCK_SCRIPT=xhs-publish without forking
	// the harness — entries replace any default in lower precedence.
	ExtraDaemonEnv []string
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
	dataDir := opts.SharedDataDir
	if dataDir == "" {
		dataDir = filepath.Join(work, "daemon-data")
	}
	channelsDir := filepath.Join(dataDir, "channels")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("harness: mkdir data: %v", err)
	}

	serverPort := mustFreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s := &Stack{
		t:            t,
		ctx:          ctx,
		opts:         opts,
		RepoRoot:     repoRoot,
		WorkDir:      work,
		ServerDB:     serverDB,
		DataDir:      dataDir,
		ChannelsDir:  channelsDir,
		ServerPort:   serverPort,
		ServerURL:    fmt.Sprintf("http://127.0.0.1:%d", serverPort),
		WSURL:        fmt.Sprintf("ws://127.0.0.1:%d/ws", serverPort),
		workspaces:   make(map[string]string),
		channels:     make(map[string]string),
		stoppedCh:    make(chan struct{}),
		extraDaemons: map[string]*daemonProc{},
	}
	s.client = buildClient(t)

	s.startServer(serverBin)
	s.waitHealthy()
	if !opts.SkipPrimaryDaemon {
		s.startDaemon(daemonBin, workerBin)
		s.waitDaemonRegistered(daemonID)
	}

	t.Cleanup(func() { s.Stop() })
	return s
}

// Stop sends SIGINT to both processes and waits for clean exit.
// Idempotent — safe to call from t.Cleanup even after an earlier
// explicit Stop. The first call captures any wait() error; subsequent
// calls are no-ops.
func (s *Stack) Stop() {
	s.once.Do(func() {
		// Collect every daemon process (primary + extras) and the server
		// into a single shutdown order: daemons first (so they get their
		// disconnect frames out before the server tears down WS readers),
		// then the server.
		var all []*exec.Cmd
		if s.daemon != nil {
			all = append(all, s.daemon)
		}
		s.mu.Lock()
		for _, dp := range s.extraDaemons {
			if dp != nil && dp.cmd != nil {
				all = append(all, dp.cmd)
			}
		}
		s.mu.Unlock()
		if s.server != nil {
			all = append(all, s.server)
		}

		// Send SIGINT to all. Server has 10s shutdown budget, daemon
		// drains its dispatcher in supervisor.Shutdown — give each
		// 15s before SIGKILL.
		for _, c := range all {
			if c == nil || c.Process == nil {
				continue
			}
			_ = c.Process.Signal(syscall.SIGINT)
		}

		shutdownDeadline := time.Now().Add(15 * time.Second)
		for _, c := range all {
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
			s.t.Logf("=== server stdout ===\n%s", safeBuf(s.serverStdout))
			s.t.Logf("=== server stderr ===\n%s", safeBuf(s.serverStderr))
			s.t.Logf("=== daemon stdout ===\n%s", safeBuf(s.daemonStdout))
			s.t.Logf("=== daemon stderr ===\n%s", safeBuf(s.daemonStderr))
			s.mu.Lock()
			for id, dp := range s.extraDaemons {
				s.t.Logf("=== daemon(%s) stdout ===\n%s", id, safeBuf(dp.stdout))
				s.t.Logf("=== daemon(%s) stderr ===\n%s", id, safeBuf(dp.stderr))
			}
			s.mu.Unlock()
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
	if len(s.opts.DeviceAllowedOrigins) > 0 {
		args = append(args, "--devicebus-allowed-origins",
			strings.Join(s.opts.DeviceAllowedOrigins, ","))
	}
	if s.opts.FastReconcile {
		args = append(args,
			"--heartbeat-timeout=2s",
			"--reconcile-grace=1s",
			"--create-timeout=5s",
		)
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
	s.daemonStdout, s.daemonStderr, s.daemon = s.spawnDaemon(daemonBin, workerBin, daemonID, s.DataDir, s.opts.ExtraDaemonEnv)
}

// spawnDaemon launches a coagent-daemon subprocess against the harness
// server. id is the --daemon-id; dataDir is the per-daemon data root
// (may be shared across daemons for the multi-daemon reclaim case);
// extraEnv is appended after the base env so callers may override the
// mock-bridge mode per-daemon.
func (s *Stack) spawnDaemon(daemonBin, workerBin, id, dataDir string, extraEnv []string) (*teeBuf, *teeBuf, *exec.Cmd) {
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/daemonbus", s.ServerPort)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		s.t.Fatalf("harness: mkdir daemon data %s: %v", dataDir, err)
	}
	args := []string{
		"--data-dir", dataDir,
		"--daemon-id", id,
		"--server-url", wsURL,
		"--key", daemonSecret,
		"--human-caller-secret", humanSecret,
		"--worker-bin", workerBin,
		"--worker-provider", "mock",
		"--replay-window-ms", "300000",
	}
	cmd := exec.CommandContext(s.ctx, daemonBin, args...)
	env := append(os.Environ(),
		"COAGENT_DATA_DIR="+dataDir,
		"COAGENT_LOG_LEVEL=info",
		// Force every spawned mock worker into single-shot mode so the
		// "exactly 1 agent.text per trigger with next_action=done"
		// contract holds without per-test plumbing.
		"COAGENT_MOCK_SINGLE_SHOT=1",
		"COAGENT_MOCK_REPLY_TEXT=pong",
	)
	env = append(env, extraEnv...)
	cmd.Env = env
	stdout := &teeBuf{}
	stderr := &teeBuf{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("harness: daemon %s start: %v", id, err)
	}
	return stdout, stderr, cmd
}

// StartExtraDaemon launches a second (or N-th) coagent-daemon against
// the same server with the supplied id. The data dir is dataDir or, when
// empty, a fresh per-daemon tmpdir under WorkDir. Returns the daemon id
// so callers can pass it to RestartExtraDaemon / StopExtraDaemon. Wait
// for it to register via WaitDaemonRegistered(id).
//
// When dataDir == s.DataDir the second daemon shares the channels/
// volume with the primary daemon — which is exactly what multi-daemon
// reclaim requires (the protocol assumes the channel sqlite files are
// available to the reclaiming daemon).
func (s *Stack) StartExtraDaemon(id, dataDir string) {
	s.t.Helper()
	daemonBin := filepath.Join(s.RepoRoot, "bin", "coagent-daemon")
	workerBin := filepath.Join(s.RepoRoot, "bin", "coagent-worker")
	if dataDir == "" {
		dataDir = filepath.Join(s.WorkDir, "daemon-"+id)
	}
	stdout, stderr, cmd := s.spawnDaemon(daemonBin, workerBin, id, dataDir, nil)
	s.mu.Lock()
	s.extraDaemons[id] = &daemonProc{id: id, dataDir: dataDir, cmd: cmd, stdout: stdout, stderr: stderr}
	s.mu.Unlock()
	s.waitDaemonRegistered(id)
}

// StopExtraDaemon SIGINTs a previously-spawned extra daemon and removes
// it from bookkeeping. Test cleanup calls Stop anyway, so this is only
// needed when the case wants a deliberate kill ordering (e.g. crash
// daemon-A before daemon-B picks up).
func (s *Stack) StopExtraDaemon(id string, force bool) {
	s.t.Helper()
	s.mu.Lock()
	dp, ok := s.extraDaemons[id]
	if ok {
		delete(s.extraDaemons, id)
	}
	s.mu.Unlock()
	if !ok || dp.cmd == nil || dp.cmd.Process == nil {
		return
	}
	sig := syscall.SIGINT
	if force {
		sig = syscall.SIGKILL
	}
	_ = dp.cmd.Process.Signal(sig)
	done := make(chan error, 1)
	go func() { done <- dp.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = dp.cmd.Process.Kill()
		<-done
	}
}

// CrashPrimaryDaemon force-kills the primary daemon (SIGKILL) so the
// server-side placement reconciler must transition its placements to
// stale via the heartbeat timeout instead of receiving a graceful
// disconnect. Used by multi-daemon reclaim tests where we want the
// stale-eviction path, not the clean-shutdown path.
func (s *Stack) CrashPrimaryDaemon() {
	s.t.Helper()
	if s.daemon == nil || s.daemon.Process == nil {
		return
	}
	_ = s.daemon.Process.Kill()
	done := make(chan error, 1)
	go func() { done <- s.daemon.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	s.daemon = nil
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
func (s *Stack) waitDaemonRegistered(id string) {
	deadline := time.Now().Add(20 * time.Second)
	db, err := sql.Open("sqlite", s.ServerDB)
	if err != nil {
		s.t.Fatalf("harness: open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	for time.Now().Before(deadline) {
		// Identify which daemon process to watch: primary uses the well-
		// known `daemonID`, extras live in extraDaemons.
		var proc *exec.Cmd
		var stdout, stderr *teeBuf
		if id == daemonID {
			proc = s.daemon
			stdout, stderr = s.daemonStdout, s.daemonStderr
		} else {
			s.mu.Lock()
			dp := s.extraDaemons[id]
			s.mu.Unlock()
			if dp != nil {
				proc = dp.cmd
				stdout, stderr = dp.stdout, dp.stderr
			}
		}
		if proc != nil && proc.ProcessState != nil && proc.ProcessState.Exited() {
			s.t.Fatalf("harness: daemon %s exited early\nstdout:\n%s\nstderr:\n%s",
				id, safeBuf(stdout), safeBuf(stderr))
		}
		var got string
		err := db.QueryRowContext(s.ctx,
			`SELECT id FROM daemons WHERE id=? AND key_hash != ''`, id).Scan(&got)
		if err == nil && got == id {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	s.t.Fatalf("harness: daemon %s never registered within 20s", id)
}

func safeBuf(t *teeBuf) string {
	if t == nil {
		return ""
	}
	return t.String()
}

// ----------------------------------------------------------------------
// API helpers — each returns (decoded body, http.Response). Fatal on
// transport / status mismatch.
// ----------------------------------------------------------------------

// Register + Login a fresh user. Returns the user_id. The cookie jar
// holds the resulting session token automatically.
//
// Per impl-layer3 §4.3.2, /api/identity/register returns 202 with an
// opaque body (no user fields) regardless of whether the email was new
// or already registered — see R4-9 / R5-13. The real user_id therefore
// comes from the subsequent /login call, not the register response.
func (s *Stack) RegisterAndLogin(email, password string) string {
	s.t.Helper()
	s.do("POST", "/api/identity/register", map[string]any{
		"email":    email,
		"password": password,
	}, http.StatusAccepted, nil)
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
//
// R4-3: gateway now requires caller-supplied envelope.id (L3 §1.8.1).
// We fill a fresh uuid per call so individual e2e cases don't have to
// fabricate ids. Tests that exercise L1 §2.3 idempotent-retry semantics
// use PostMessageWithID below to thread their own id.
func (s *Stack) PostMessage(channelID, envType, text, kindHint string) PostMessageResponse {
	return s.PostMessageWithID(channelID, uuid.NewString(), envType, text, kindHint)
}

// PostMessageWithID is the explicit-id variant. Tests that exercise
// L1 §2.3 idempotent-retry / id-duplicate-conflict semantics use this
// to reuse an id across submissions.
func (s *Stack) PostMessageWithID(channelID, messageID, envType, text, kindHint string) PostMessageResponse {
	s.t.Helper()
	payload, _ := json.Marshal(map[string]string{"text": text})
	body := map[string]any{
		"id":      messageID,
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

// IssueDeviceSessionResponse mirrors POST /api/channels/:chID/devices.
type IssueDeviceSessionResponse struct {
	DeviceSessionID string `json:"device_session_id"`
	Token           string `json:"token"`
	ExpiresAt       int64  `json:"expires_at"`
}

// IssueDeviceSession calls the gateway's device session issue endpoint
// and returns the freshly minted session_id + raw token.  Wraps the four
// step bind: (1) caller already knows the channel + daemon, (2) caller
// uses the returned token to open a /devicebus WS handshake.
func (s *Stack) IssueDeviceSession(channelID, deviceID, daemonIDArg string) IssueDeviceSessionResponse {
	s.t.Helper()
	if daemonIDArg == "" {
		daemonIDArg = daemonID
	}
	var resp IssueDeviceSessionResponse
	s.do("POST", "/api/channels/"+channelID+"/devices", map[string]any{
		"device_id":   deviceID,
		"device_type": "xhs",
		"daemon_id":   daemonIDArg,
	}, http.StatusCreated, &resp)
	return resp
}

// PlacementRow captures the columns multi-daemon reclaim tests assert on.
type PlacementRow struct {
	ChannelID       string
	DaemonID        string
	State           string
	OwnerEpoch      int64
	ConnectionEpoch int64
	LastHeartbeatAt int64
}

// GetPlacement reads the current channel_placements row directly from
// server.db. Returns false when no row exists for that channel. Tests
// poll this to watch the active → stale → active(daemon-B) transitions
// during multi-daemon reclaim.
func (s *Stack) GetPlacement(channelID string) (PlacementRow, bool) {
	s.t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDB+"?mode=ro")
	if err != nil {
		s.t.Fatalf("harness: open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var row PlacementRow
	err = db.QueryRowContext(s.ctx, `
		SELECT channel_id, daemon_id, state,
		       owner_epoch,
		       COALESCE(daemon_connection_epoch, 0),
		       COALESCE(last_heartbeat_at, 0)
		FROM channel_placements WHERE channel_id=?`, channelID).Scan(
		&row.ChannelID, &row.DaemonID, &row.State,
		&row.OwnerEpoch, &row.ConnectionEpoch, &row.LastHeartbeatAt,
	)
	if err == sql.ErrNoRows {
		return PlacementRow{}, false
	}
	if err != nil {
		s.t.Fatalf("harness: placement lookup: %v", err)
	}
	return row, true
}

// DeviceSessionRow captures the columns device session bind tests
// assert on. token_hash is included so tests can verify it equals the
// HMAC over the raw token they received from IssueDeviceSession.
type DeviceSessionRow struct {
	ID        string
	DeviceID  string
	ChannelID string
	DaemonID  string
	State     string
	TokenHash string
	ExpiresAt int64
	CreatedAt int64
}

// GetDeviceSession reads the device_sessions row directly. Returns false
// when no row exists. Used by case 2 to assert state=ready / active and
// that the row carries the right channel + daemon ids.
func (s *Stack) GetDeviceSession(sessionID string) (DeviceSessionRow, bool) {
	s.t.Helper()
	db, err := sql.Open("sqlite", "file:"+s.ServerDB+"?mode=ro")
	if err != nil {
		s.t.Fatalf("harness: open server.db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var row DeviceSessionRow
	err = db.QueryRowContext(s.ctx, `
		SELECT device_session_id, device_id, channel_id, daemon_id,
		       state, token_hash, expires_at, created_at
		FROM device_sessions WHERE device_session_id=?`, sessionID).Scan(
		&row.ID, &row.DeviceID, &row.ChannelID, &row.DaemonID,
		&row.State, &row.TokenHash, &row.ExpiresAt, &row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return DeviceSessionRow{}, false
	}
	if err != nil {
		s.t.Fatalf("harness: device session lookup: %v", err)
	}
	return row, true
}

// RestartServer SIGINTs the server process and starts a fresh one on the
// same port + db path. Used by the view-sync gap drain test — daemon
// must reconnect cleanly and replay any unacked view-sync outbox rows.
func (s *Stack) RestartServer() {
	s.t.Helper()
	if s.server == nil || s.server.Process == nil {
		s.t.Fatalf("harness: RestartServer called with no running server")
	}
	_ = s.server.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- s.server.Wait() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = s.server.Process.Kill()
		<-done
	}
	serverBin := filepath.Join(s.RepoRoot, "bin", "coagent-server")
	s.startServer(serverBin)
	s.waitHealthy()
	// daemonbus reconnect supervisor handles redial; wait for the
	// daemon row to repopulate as a proxy for "WS handshake complete".
	if s.daemon != nil {
		s.waitDaemonRegistered(daemonID)
	}
}

// DataDirFor returns the on-disk path for an arbitrary daemon's data
// root. Used by tests that need to assert files exist under either the
// primary or an extra daemon's tree.
func (s *Stack) DataDirFor(id string) string {
	s.t.Helper()
	if id == daemonID {
		return s.DataDir
	}
	s.mu.Lock()
	dp, ok := s.extraDaemons[id]
	s.mu.Unlock()
	if !ok {
		s.t.Fatalf("harness: unknown daemon %q", id)
	}
	return dp.dataDir
}

// ChannelSqlitePathFor returns the channel sqlite path under the named
// daemon's data root. Useful for the multi-daemon reclaim case where
// each daemon owns its own channels dir.
func (s *Stack) ChannelSqlitePathFor(daemonName, channelID string) string {
	return filepath.Join(s.DataDirFor(daemonName), "channels", channelID, "channel.sqlite")
}

// ServerURLBase returns the http base url (no trailing slash).
func (s *Stack) ServerURLBase() string { return s.ServerURL }

// DevicebusWSURL composes the wss/ws URL for the /devicebus endpoint
// with the session_id + token query params.
func (s *Stack) DevicebusWSURL(sessionID, token string) string {
	return fmt.Sprintf("ws://127.0.0.1:%d/devicebus?session_id=%s&token=%s",
		s.ServerPort, url.QueryEscape(sessionID), url.QueryEscape(token))
}

// RestartDaemon kills the daemon process and starts a fresh one with
// the same data dir + daemon-id. Tests rely on this to assert the
// reconnect path keeps placements healthy.
//
// When the primary daemon was already torn down (e.g. via
// CrashPrimaryDaemon) the SIGINT step is skipped — the cold-start path
// is what we want to exercise next.
func (s *Stack) RestartDaemon() {
	s.t.Helper()
	if s.daemon != nil && s.daemon.Process != nil {
		_ = s.daemon.Process.Signal(syscall.SIGINT)
		done := make(chan error, 1)
		go func() { done <- s.daemon.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = s.daemon.Process.Kill()
			<-done
		}
	}
	daemonBin := filepath.Join(s.RepoRoot, "bin", "coagent-daemon")
	workerBin := filepath.Join(s.RepoRoot, "bin", "coagent-worker")
	s.startDaemon(daemonBin, workerBin)
	s.waitDaemonRegistered(daemonID)
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
	Visibility string
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
		SELECT seq, id, type, kind, sender_kind, sender_id, visibility, payload
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
			&m.SenderKind, &m.SenderID, &m.Visibility, &payload); err != nil {
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
