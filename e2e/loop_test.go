// Package e2e is the C1 minimal-loop harness (c1-minimal-loop-build-spec.md §4):
// a BLACK-BOX driver of two real OS processes (atoll-server + atoll-daemon)
// walking one message's six-leg journey — 起/入/转/做/回/验 — over real HTTP, a
// real gateway ws, a real yamux link, and two kill -9 restarts.
//
// Black-box law (spec red line 1): this package imports ZERO atoll packages. It
// speaks exactly four languages — ① /api HTTP ② /ws frames ③ binary CLI flags
// ④ process signals. Any assertion that would need an internal import is a
// contract-surface gap to report, never to bridge here.
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
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMain(m *testing.M) { os.Exit(m.Run()) }

// ---------------------------------------------------------------------------
// Process management: start / kill -9 / idempotent reclaim
// ---------------------------------------------------------------------------

// proc wraps one child process. Each Start registers an idempotent cleanup that
// SIGKILLs the process GROUP and Waits (zombie reclaim) — the same reclaim an
// explicit kill9 mid-test uses, so failure paths leave zero orphans (requirement
// 验收线 1: "进程收干净" includes failure paths).
type proc struct {
	name    string
	cmd     *exec.Cmd
	logPath string

	mu     sync.Mutex
	waited bool
	done   chan struct{} // closed once Wait returns
}

// startProc launches bin with args, its own process group, a scrubbed env, and
// stdout+stderr teed to logPath. dir is Cmd.Dir (the server loads cwd's .env —
// pointing Dir at a tempdir keeps dev-machine creds out, spec red line 3).
func startProc(t *testing.T, name, bin string, args []string, dir, logPath string, env []string) *proc {
	t.Helper()
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("%s: open log: %v", name, err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logf.Close()
		t.Fatalf("%s: start: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, logPath: logPath, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		logf.Close()
		close(p.done)
	}()
	t.Cleanup(p.reclaim) // idempotent; also runs after an explicit kill9
	return p
}

// exited reports whether the process has already terminated.
func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// kill9 SIGKILLs the process group (never a graceful signal — the 验 leg is a
// crash, not a shutdown) and asserts the reclaim actually landed.
func (p *proc) kill9(t *testing.T) {
	t.Helper()
	p.reclaim()
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatalf("%s: not reclaimed after SIGKILL", p.name)
	}
}

// reclaim is the idempotent terminate-and-wait: safe to call any number of
// times, from kill9 and from t.Cleanup both. The wait is BOUNDED — an
// unkillable/wedged child must not pin the whole test to its global timeout
// (kill9's own assert then reports it loudly).
func (p *proc) reclaim() {
	p.mu.Lock()
	if p.waited {
		p.mu.Unlock()
		return
	}
	p.waited = true
	p.mu.Unlock()
	// Negative pid = the whole process group (children included). ESRCH = the
	// group is already gone (process exited on its own) — the bounded wait below
	// still reaps it via the Wait goroutine.
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
	}
}

// tailLog returns the last n lines of the process log for failure diagnostics.
func tailLog(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "(no log: " + err.Error() + ")"
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// scrubbedEnv is the child env: PATH + a tempdir HOME only. No inherited
// KIMI_*/API creds can reach the child (spec red line 3 — a leaked credential
// would let boost run a REAL LLM and steal the route).
func scrubbedEnv(home string) []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home}
}

// freePort asks the kernel for a free TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// ---------------------------------------------------------------------------
// HTTP API client (cookie-jar session)
// ---------------------------------------------------------------------------

type apiClient struct {
	t    *testing.T
	base string
	hc   *http.Client
}

func newAPIClient(t *testing.T, base string) *apiClient {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	return &apiClient{t: t, base: base, hc: &http.Client{Jar: jar, Timeout: 15 * time.Second}}
}

// do runs one request and decodes the JSON body (nil body on empty response).
func (a *apiClient) do(method, path string, body any) (int, map[string]any) {
	a.t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			a.t.Fatalf("marshal %s %s body: %v", method, path, err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, a.base+path, rdr)
	if err != nil {
		a.t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.hc.Do(req)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m
}

// must runs do and asserts one of the wanted statuses.
func (a *apiClient) must(method, path string, body any, wantStatus ...int) map[string]any {
	a.t.Helper()
	st, m := a.do(method, path, body)
	for _, w := range wantStatus {
		if st == w {
			return m
		}
	}
	a.t.Fatalf("%s %s: status %d (want %v), body %v", method, path, st, wantStatus, m)
	return nil
}

// mustRetry5xx runs do, retrying transient verdicts (5xx — e.g. introduce
// hitting the humancell mint window returns 500/503) until deadline. This is
// the spec's retry discipline: 5xx is the race-window class, everything else is
// a real failure.
func (a *apiClient) mustRetry5xx(method, path string, body any, deadline time.Duration, wantStatus ...int) map[string]any {
	a.t.Helper()
	end := time.Now().Add(deadline)
	for {
		st, m := a.do(method, path, body)
		for _, w := range wantStatus {
			if st == w {
				return m
			}
		}
		if st < 500 || time.Now().After(end) {
			a.t.Fatalf("%s %s: status %d (want %v), body %v", method, path, st, wantStatus, m)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// cookieHeader renders the jar's cookies for the ws handshake.
func (a *apiClient) cookieHeader() string {
	u, _ := url.Parse(a.base)
	var parts []string
	for _, c := range a.hc.Jar.Cookies(u) {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}

// waitHealthzErr is the readiness barrier: poll /healthz until 200, watching
// for an early process exit (the server migrates the DB + loads channels before
// it listens — skipping this gate is flake食谱). Error (not fatal) so the
// initial-start path can treat a bind clash as "pick another port and retry".
func waitHealthzErr(base string, p *proc, timeout time.Duration) error {
	hc := &http.Client{Timeout: 2 * time.Second}
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if p.exited() {
			return fmt.Errorf("%s exited before becoming healthy", p.name)
		}
		resp, err := hc.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s not healthy within %s", p.name, timeout)
}

func waitHealthz(t *testing.T, base string, p *proc, timeout time.Duration) {
	t.Helper()
	if err := waitHealthzErr(base, p, timeout); err != nil {
		t.Fatalf("%v; log tail:\n%s", err, tailLog(p.logPath, 50))
	}
}

// ---------------------------------------------------------------------------
// Gateway ws client: single reader, ref-matched acks, buffered tail
// ---------------------------------------------------------------------------

// wsClient follows the spec's ws discipline: ONE reader goroutine fans frames
// by type into tail and ack/error channels; requests are matched by ref (never
// "read the next frame and call it the ack" — subscribe backfills history and
// tail shares the outbound pump with acks).
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	tail chan map[string]any
	acks chan map[string]any
}

func dialWS(t *testing.T, base, cookie, chID string, sinceSeq int64) *wsClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	hdr := http.Header{}
	hdr.Set("Cookie", cookie)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel_id": chID, "since_seq": sinceSeq}); err != nil {
		t.Fatalf("subscribe frame: %v", err)
	}
	c := &wsClient{t: t, conn: conn,
		tail: make(chan map[string]any, 16384),
		acks: make(chan map[string]any, 256),
	}
	t.Cleanup(c.close)
	go c.readLoop()
	return c
}

func (c *wsClient) close() { _ = c.conn.Close() }

func (c *wsClient) readLoop() {
	for {
		var m map[string]any
		if err := c.conn.ReadJSON(&m); err != nil {
			return
		}
		switch m["type"] {
		case "message":
			c.tail <- m
		case "ack", "error":
			c.acks <- m
		}
	}
}

func (c *wsClient) send(m map[string]any) error { return c.conn.WriteJSON(m) }

// awaitRef returns the ack/error frame whose ref matches (skipping strays).
func (c *wsClient) awaitRef(ref string, timeout time.Duration) (map[string]any, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case m := <-c.acks:
			if m["ref"] == ref {
				return m, true
			}
		case <-deadline:
			return nil, false
		}
	}
}

// awaitTail returns the first tail envelope matching pred.
func (c *wsClient) awaitTail(pred func(env map[string]any) bool, timeout time.Duration) (map[string]any, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case m := <-c.tail:
			env, _ := m["envelope"].(map[string]any)
			if env != nil && pred(env) {
				return env, true
			}
		case <-deadline:
			return nil, false
		}
	}
}

// ---------------------------------------------------------------------------
// Loop verbs over the ws: chat / verify with the retry discipline
// ---------------------------------------------------------------------------

// retryableFrameErr: submit-window races (assistant cell attaching, home
// re-opening) surface as these frame error codes — retry with a fresh attempt.
func retryableFrameErr(code string) bool {
	switch code {
	case "unavailable", "closed", "receiver_unavailable":
		return true
	}
	return false
}

// retryableTerminal: a failed terminal whose reason is a liveness race (the
// daemon-hosted receiver not attached yet / request expired while nobody
// served) — retry a fresh request; any other failure is a real bug.
//
// Known blind spot (双线审 P2, on record): a liveness race INSIDE the
// assistant's own tool call surfaces as tool_call_failed — a hard fail here.
// Unreachable today (echo is server-placed; loadChannels precedes listen +
// the healthz gate), but a daemon-placed tool would make it a flake source —
// widen this set then.
func retryableTerminal(reason string) bool {
	switch reason {
	case "receiver_unavailable", "unanswered_timeout":
		return true
	}
	return false
}

var refCounter int

// submitAndAwaitTerminal sends one message frame (msg_type + raw payload) and
// waits for its terminal response envelope, retrying竞态类 verdicts with fresh
// attempts until deadline. Each attempt's wait stays under the request TTL.
func submitAndAwaitTerminal(t *testing.T, ws *wsClient, msgType string, payload json.RawMessage, deadline time.Duration) (msgID string, terminal map[string]any) {
	t.Helper()
	end := time.Now().Add(deadline)
	for attempt := 1; ; attempt++ {
		if time.Now().After(end) {
			t.Fatalf("%s: no completed terminal within %s", msgType, deadline)
		}
		// ONE budget per attempt, ack + terminal both inside it, strictly under
		// the 30s request TTL (spec §4 retry discipline: "单次 attempt 等待 <
		// 30s") — the terminal wait spends whatever the ack wait left over.
		attemptEnd := time.Now().Add(28 * time.Second)
		refCounter++
		ref := fmt.Sprintf("%s-%d", msgType, refCounter)
		if err := ws.send(map[string]any{
			"type": "message", "ref": ref, "msg_type": msgType, "payload": payload,
		}); err != nil {
			t.Fatalf("%s: ws send: %v", msgType, err)
		}
		ack, ok := ws.awaitRef(ref, 10*time.Second)
		if !ok {
			t.Fatalf("%s: no ack/error frame for ref %s within 10s", msgType, ref)
		}
		if ack["type"] == "error" {
			code, _ := ack["error"].(string)
			if retryableFrameErr(code) {
				time.Sleep(time.Second)
				continue
			}
			t.Fatalf("%s: frame error %q (detail %v)", msgType, code, ack["detail"])
		}
		id, _ := ack["message_id"].(string)
		if id == "" {
			t.Fatalf("%s: ack carries no message_id: %v", msgType, ack)
		}
		// The ack receipt is {message_id, seq} — the commit seq is part of the
		// L2 acceptance shape, so its absence/zero is a contract break.
		if seq, ok := ack["seq"].(float64); !ok || seq <= 0 {
			t.Fatalf("%s: ack carries no positive seq: %v", msgType, ack)
		}
		// Await THIS request's terminal (a failed terminal carries
		// payload.status=failed + reason).
		env, got := ws.awaitTail(func(env map[string]any) bool {
			return env["kind"] == "response" && env["parent_id"] == id && terminalStatus(env) != ""
		}, time.Until(attemptEnd))
		if !got {
			// The request may still be open (nobody served in the window) —
			// treat as a race-class retry; its own deadline closes it durably.
			continue
		}
		if terminalStatus(env) == "completed" {
			return id, env
		}
		reason := payloadField(env, "reason")
		if retryableTerminal(reason) {
			time.Sleep(time.Second)
			continue
		}
		t.Fatalf("%s: failed terminal, reason %q, payload %v", msgType, reason, env["payload"])
	}
}

// terminalStatus extracts payload.status if terminal ("" otherwise).
func terminalStatus(env map[string]any) string {
	p := envelopePayload(env)
	s, _ := p["status"].(string)
	if s == "completed" || s == "failed" {
		return s
	}
	return ""
}

func envelopePayload(env map[string]any) map[string]any {
	raw, ok := env["payload"]
	if !ok || raw == nil {
		return map[string]any{}
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return m
}

func payloadField(env map[string]any, key string) string {
	v, _ := envelopePayload(env)[key].(string)
	return v
}

// ---------------------------------------------------------------------------
// The six-leg journey
// ---------------------------------------------------------------------------

func TestLoop(t *testing.T) {
	binDir := os.Getenv("ATOLL_E2E_BIN")
	if binDir == "" {
		t.Skip("ATOLL_E2E_BIN not set; run via `make e2e-loop`")
	}
	serverBin := filepath.Join(binDir, "atoll-server")
	daemonBin := filepath.Join(binDir, "atoll-daemon")
	for _, b := range []string{serverBin, daemonBin} {
		if _, err := os.Stat(b); err != nil {
			t.Fatalf("binary missing: %v", err)
		}
	}

	// Fully-isolated world: server cwd (no .env), app db, channel dbs, daemon
	// workspace root, child HOME, logs.
	root := t.TempDir()
	dirs := map[string]string{}
	for _, d := range []string{"serverwd", "daemonwd", "channels", "daemon-ws", "home", "logs"} {
		dirs[d] = filepath.Join(root, d)
		if err := os.MkdirAll(dirs[d], 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])
	var port int
	var base string

	// On failure, dump both process logs' tails so the broken leg is locatable
	// from the output alone (requirement 验收线 5, agent-first).
	var serverLog, daemonLog string
	t.Cleanup(func() {
		if t.Failed() {
			if serverLog != "" {
				t.Logf("server log tail:\n%s", tailLog(serverLog, 50))
			}
			if daemonLog != "" {
				t.Logf("daemon log tail:\n%s", tailLog(daemonLog, 50))
			}
		}
	})

	serverGen := 0
	startServerProc := func() *proc {
		serverGen++
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("server-%d.log", serverGen))
		return startProc(t, fmt.Sprintf("server#%d", serverGen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], serverLog, env)
	}

	// ---- L1 起 -----------------------------------------------------------
	// Initial start: the probe→listen window can lose the port to another
	// process — an early exit before healthy retries on a FRESH port (spec §4:
	// "bind 失败换口重试"). The restart leg below reuses the settled port (the
	// daemon's -server URL is welded to it).
	var server *proc
	for attempt := 1; ; attempt++ {
		port = freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		server = startServerProc()
		err := waitHealthzErr(base, server, 30*time.Second)
		if err == nil {
			break
		}
		if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		}
		t.Fatalf("%v; log tail:\n%s", err, tailLog(serverLog, 50))
	}
	api := newAPIClient(t, base)

	reg := api.must("POST", "/api/identity/register",
		map[string]any{"email": "loop@example.com", "password": "secret123", "display_name": "Loop"},
		http.StatusCreated)
	userID, _ := reg["id"].(string)

	ws1 := api.must("POST", "/api/workspaces", map[string]any{"name": "loop-ws"}, http.StatusCreated)
	wsID, _ := ws1["id"].(string)

	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	// create-and-attach (MUST be this endpoint: bare POST /api/daemons issues a
	// key but no daemon_channels binding — the daemon would 403 forever).
	dm := api.must("POST", "/api/channels/"+chID+"/daemons", map[string]any{"name": "loop-box"}, http.StatusCreated)
	daemonID, _ := dm["id"].(string)
	apiKey, _ := dm["api_key"].(string)
	if daemonID == "" || apiKey == "" {
		t.Fatalf("create-and-attach daemon: %v", dm)
	}

	// Two declarations through the world layer: the echo tool + the scripted
	// assistant configured to call it.
	echoDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "echo-tool", "class": "echo"}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	// Introduce the tool first so the assistant config receives its substrate-minted
	// instance id rather than reconstructing one from the declaration principal.
	echoIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": echoDeclID, "placement": "server"},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	echoID, _ := echoIntro["instance_id"].(string)
	asstDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "assistant", "class": "script",
			"config": map[string]any{"tool_id": echoID}},
		http.StatusCreated)
	asstDeclID, _ := asstDecl["id"].(string)
	asstIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": asstDeclID, "placement": "daemon",
			"desired_host": daemonID, "make_default": true},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	assistantID, _ := asstIntro["instance_id"].(string)

	startDaemon := func(gen int) *proc {
		daemonLog = filepath.Join(dirs["logs"], fmt.Sprintf("daemon-%d.log", gen))
		return startProc(t, fmt.Sprintf("daemon#%d", gen), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, chID),
			"-key", apiKey,
			"-name", "loop-box",
			"-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], daemonLog, env)
	}
	daemon := startDaemon(1)

	// L1 assertion ①: default_agent converges to the assistant.
	pollUntil(t, "default_agent points at assistant", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID, nil)
		return m["default_agent"] == assistantID
	})

	// L1 assertion ②: 户籍 is EXACTLY five members — {system, creator,
	// agent:boost, echo, assistant} (membrane law: nothing extra slipped in as a
	// side effect, nothing missing).
	boostID, _ := ch["default_agent"].(string)
	var humanID string
	pollUntil(t, "creator principal is represented by one active human", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
		rows, _ := m["actors"].([]any)
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			if row["kind"] == "human" && row["principal"] == userID {
				humanID, _ = row["id"].(string)
				return humanID != ""
			}
		}
		return false
	})
	wantMembers := []string{"system", humanID, boostID, echoID, assistantID}
	sort.Strings(wantMembers)
	pollUntil(t, "membership is exactly the five expected", 60*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
		rows, _ := m["actors"].([]any)
		var got []string
		for _, r := range rows {
			row, _ := r.(map[string]any)
			id, _ := row["id"].(string)
			got = append(got, id)
		}
		sort.Strings(got)
		return reflect.DeepEqual(got, wantMembers)
	})

	// ---- L2 入 / L3 转 / L4 做 / L5 回 ------------------------------------
	cookie := api.cookieHeader()
	ws := dialWS(t, base, cookie, chID, 0)

	// Payload discipline for the byte-exact legs: the frames marshal via
	// encoding/json, which compacts and HTML-escapes — every payload here is
	// pre-compacted pure ASCII (no spare whitespace, no <>&) so the RawMessage
	// bytes survive the trip verbatim and content comparisons stay exact.
	chatPayload1 := json.RawMessage(`{"text":"hello loop one"}`)
	chat1ID, term1 := submitAndAwaitTerminal(t, ws, "loop.chat", chatPayload1, 120*time.Second)
	rid1 := assertChatReply(t, term1, chatPayload1)

	// L5 verify: the assistant reads the REAL bytes back off the daemon disk.
	verifyResource(t, ws, rid1, chatPayload1)

	// ---- L6 验① : kill -9 the daemon, restart on the same workspace root ---
	daemon.kill9(t)
	daemon = startDaemon(2)

	chatPayload2 := json.RawMessage(`{"text":"hello loop two"}`)
	_, term2 := submitAndAwaitTerminal(t, ws, "loop.chat", chatPayload2, 120*time.Second)
	_ = assertChatReply(t, term2, chatPayload2)

	// The PRE-crash file survives the daemon restart (storagehost rescans the
	// real disk — incarnation换代 without truth loss).
	verifyResource(t, ws, rid1, chatPayload1)

	// ---- L6 验② : kill -9 the server, restart on the same db + channel dir --
	ws.close()
	server.kill9(t)
	server = startServerProc()
	waitHealthz(t, base, server, 30*time.Second)

	// Fresh session (the old cookie row also survived in app.db, but re-login is
	// the honest client behaviour after a server death).
	api2 := newAPIClient(t, base)
	api2.must("POST", "/api/identity/login",
		map[string]any{"email": "loop@example.com", "password": "secret123"}, http.StatusOK)

	ws2 := dialWS(t, base, api2.cookieHeader(), chID, 0)

	// ①: the pre-crash conversation is STILL IN THE LOG — both the request and
	// its response envelopes replay from seq 0 (requirement: "之前的会话还在" is
	// proven by the old envelopes, not by a new chat succeeding).
	if _, ok := ws2.awaitTail(func(env map[string]any) bool {
		return env["id"] == chat1ID && env["kind"] == "request"
	}, 30*time.Second); !ok {
		t.Fatalf("server restart: pre-crash request envelope %s not replayed from seq 0", chat1ID)
	}
	if _, ok := ws2.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == chat1ID && terminalStatus(env) == "completed"
	}, 30*time.Second); !ok {
		t.Fatalf("server restart: pre-crash response for %s not replayed from seq 0", chat1ID)
	}

	// ②: a fresh chat walks the whole path again (daemon redials, reconcile
	// revives, routing + call + resource all live).
	chatPayload3 := json.RawMessage(`{"text":"hello loop three"}`)
	_, term3 := submitAndAwaitTerminal(t, ws2, "loop.chat", chatPayload3, 180*time.Second)
	_ = assertChatReply(t, term3, chatPayload3)

	// ③: the ORIGINAL file still reads back byte-exact across BOTH restarts.
	verifyResource(t, ws2, rid1, chatPayload1)

	// Success path also reclaims both processes explicitly (t.Cleanup would too;
	// doing it here keeps "进程收干净" an asserted step, not a teardown side
	// effect).
	daemon.kill9(t)
	server.kill9(t)
}

// assertChatReply pins the loop.chat completed terminal: ok=true, echoed ==
// the sent payload (protocol fields stripped), resource_id present. Returns the
// resource id.
func assertChatReply(t *testing.T, term map[string]any, sent json.RawMessage) string {
	t.Helper()
	p := envelopePayload(term)
	if p["ok"] != true {
		t.Fatalf("chat terminal not ok: %v", p)
	}
	var want map[string]any
	if err := json.Unmarshal(sent, &want); err != nil {
		t.Fatalf("unmarshal sent payload: %v", err)
	}
	echoed, _ := p["echoed"].(map[string]any)
	if !reflect.DeepEqual(echoed, want) {
		t.Fatalf("echoed = %v, want %v", echoed, want)
	}
	rid, _ := p["resource_id"].(string)
	if rid == "" {
		t.Fatalf("chat terminal carries no resource_id: %v", p)
	}
	return rid
}

// verifyResource drives loop.verify and asserts the daemon-disk bytes match the
// original payload exactly (size + content).
func verifyResource(t *testing.T, ws *wsClient, rid string, original json.RawMessage) {
	t.Helper()
	req, _ := json.Marshal(map[string]any{"resource_id": rid})
	_, term := submitAndAwaitTerminal(t, ws, "loop.verify", req, 120*time.Second)
	p := envelopePayload(term)
	if p["exists"] != true {
		t.Fatalf("verify %s: exists = %v (payload %v)", rid, p["exists"], p)
	}
	content, _ := p["content"].(string)
	if content != string(original) {
		t.Fatalf("verify %s: content = %q, want byte-exact %q", rid, content, string(original))
	}
	if size, _ := p["size"].(float64); int(size) != len(original) {
		t.Fatalf("verify %s: size = %v, want %d", rid, p["size"], len(original))
	}
}

// pollUntil polls cond until true or fails at deadline.
func pollUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
