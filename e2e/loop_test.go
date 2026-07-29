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
// Gateway ws client: single reader, ref-matched receipts, buffered feed tail
// ---------------------------------------------------------------------------

// wsClient speaks the standard gateway frame protocol (连接模型勘误期 v2 — 连接即人):
// every wire frame is {v(=2), frame_type, ref, payload}. There is NO binding_gen (the
// client-visible binding axis is retired) and the /ws URL names NO channel (a
// connection is an authenticated person + one pipe subscribing to ALL the person's合法
// 频道). ONE reader goroutine fans frames by type — feed frames (each carrying its own
// channel_id) into tail, receipt/error into acks — and requests are matched by
// TOP-LEVEL ref. The opening frame is a channel-blind attach handing over a multi-key
// 游标表 (since); its receipt is an empty报到 ack. Every UPSTREAM business frame carries
// a required channel_id (send stamps the session's primary channel when the caller
// doesn't name its own). Frames are hand-rolled maps: this package imports ZERO atoll
// packages (red line 1) — the frame shape is part of the /ws contract it speaks.
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	tail chan map[string]any // feed-frame payloads {channel_id, seq, envelope}
	acks chan map[string]any // receipt/error frames (whole frame, ref at top)
	done chan struct{}       // closed when the reader exits (server tore the session down)

	chID string // primary channel: the default channel_id stamped onto business frames
}

const frameVersion = 2

// frame builds one wire frame map (v2 envelope: no binding_gen).
func frame(frameType, ref string, payload any) map[string]any {
	m := map[string]any{"v": frameVersion, "frame_type": frameType}
	if ref != "" {
		m["ref"] = ref
	}
	if payload != nil {
		m["payload"] = payload
	}
	return m
}

// dialWS opens ONE connection whose primary channel is chID, seeding its游标表 with a
// single since key. Business frames sent through it default their channel_id to chID.
func dialWS(t *testing.T, base, cookie, chID string, sinceSeq int64) *wsClient {
	t.Helper()
	return dialWSMulti(t, base, cookie, chID, map[string]int64{chID: sinceSeq})
}

// dialWSMulti opens ONE channel-blind connection (连接即人 v2): the /ws URL carries no
// channel and the opening attach hands over a multi-key游标表 (since). primaryCh is the
// default channel_id stamped onto business frames that don't name their own — the
// multi-channel test passes channel_id explicitly per frame.
func dialWSMulti(t *testing.T, base, cookie, primaryCh string, since map[string]int64) *wsClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	hdr := http.Header{}
	hdr.Set("Cookie", cookie)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	c := &wsClient{t: t, conn: conn, chID: primaryCh,
		tail: make(chan map[string]any, 16384),
		acks: make(chan map[string]any, 256),
		done: make(chan struct{}),
	}
	t.Cleanup(c.close)
	go c.readLoop()
	// Opening attach: channel-blind report-in handing over the游标表 (since map). The
	// receipt is an empty报到 ack (attach no longer grants a binding_gen).
	ref := "attach-" + primaryCh
	if err := conn.WriteJSON(frame("attach", ref, map[string]any{"since": since})); err != nil {
		t.Fatalf("attach frame: %v", err)
	}
	rec, ok := c.awaitRef(ref, 10*time.Second)
	if !ok {
		t.Fatalf("no attach receipt within 10s")
	}
	if rec["frame_type"] != "receipt" {
		t.Fatalf("attach not accepted: %v", rec)
	}
	return c
}

func (c *wsClient) close() { _ = c.conn.Close() }

func (c *wsClient) readLoop() {
	defer close(c.done)
	for {
		var m map[string]any
		if err := c.conn.ReadJSON(&m); err != nil {
			return
		}
		switch m["frame_type"] {
		case "feed":
			if p, _ := m["payload"].(map[string]any); p != nil {
				c.tail <- p
			}
		case "receipt", "error":
			c.acks <- m
		}
	}
}

// send writes one upstream business frame (连接模型勘误期 v2). When the caller's payload
// is a map that doesn't name its own channel_id, it is stamped with the session's
// primary channel — every business frame carries a required channel_id.
func (c *wsClient) send(frameType, ref string, payload any) error {
	if m, ok := payload.(map[string]any); ok {
		if _, has := m["channel_id"]; !has && c.chID != "" {
			m["channel_id"] = c.chID
		}
	}
	return c.conn.WriteJSON(frame(frameType, ref, payload))
}

// awaitRef returns the receipt/error/notify frame whose TOP-LEVEL ref matches
// (skipping strays).
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

// awaitTail returns the first feed envelope matching pred (a feed frame's
// payload.envelope).
func (c *wsClient) awaitTail(pred func(env map[string]any) bool, timeout time.Duration) (map[string]any, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case fp := <-c.tail:
			env, _ := fp["envelope"].(map[string]any)
			if env != nil && pred(env) {
				return env, true
			}
		case <-deadline:
			return nil, false
		}
	}
}

// frameErrCode / frameErrDetail extract an error frame's flat code + detail (表①,
// 裁决8: code is always a single flat word).
func frameErrCode(m map[string]any) string {
	p, _ := m["payload"].(map[string]any)
	code, _ := p["code"].(string)
	return code
}

func frameErrDetail(m map[string]any) string {
	p, _ := m["payload"].(map[string]any)
	d, _ := p["detail"].(string)
	return d
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
		// A submit frame's payload = {msg_type, payload:<message body>} (no audience
		// → the gateway routing面 resolves the default_agent). The message body's
		// bytes survive verbatim (json.RawMessage).
		if err := ws.send("submit", ref, map[string]any{"msg_type": msgType, "payload": payload}); err != nil {
			t.Fatalf("%s: ws send: %v", msgType, err)
		}
		rec, ok := ws.awaitRef(ref, 10*time.Second)
		if !ok {
			t.Fatalf("%s: no receipt/error frame for ref %s within 10s", msgType, ref)
		}
		if rec["frame_type"] == "error" {
			code := frameErrCode(rec)
			if retryableFrameErr(code) {
				time.Sleep(time.Second)
				continue
			}
			t.Fatalf("%s: frame error %q (detail %q)", msgType, code, frameErrDetail(rec))
		}
		rp, _ := rec["payload"].(map[string]any)
		id, _ := rp["message_id"].(string)
		if id == "" {
			t.Fatalf("%s: receipt carries no message_id: %v", msgType, rec)
		}
		// The submit receipt is {message_id} and nothing else. A receipt says
		// "accepted, and this is its identity"; seq is the store's row position,
		// which the wire contract already forbade a client from using as a feed
		// cursor — leaving it the one field nobody could legally read. Its
		// PRESENCE is now the contract break.
		if _, leaked := rp["seq"]; leaked {
			t.Fatalf("%s: receipt still carries seq: %v", msgType, rec)
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
