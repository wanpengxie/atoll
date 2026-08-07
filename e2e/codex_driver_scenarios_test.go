package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// codexSmokeHarness owns one isolated, real Codex node. Every scenario gets a
// fresh node and CODEX_HOME; the developer's Codex installation is read only
// for auth/config bootstrap and is never used as the rollout store.
type codexSmokeHarness struct {
	t       *testing.T
	root    string
	home    string
	nodeDir string
	binDir  string
	env     []string
	port    int
	base    string
	logPath string
	proc    *proc
	token   string
	homeID  string
	ws      *wsClient
}

func newCodexSmokeHarness(t *testing.T) *codexSmokeHarness {
	t.Helper()
	if os.Getenv("ATOLL_CODEX_E2E") != "1" {
		t.Skip("ATOLL_CODEX_E2E=1 not set")
	}
	binDir := requireE2EBin(t)
	for _, name := range []string{"atoll", "atoll-server", "atoll-daemon"} {
		if _, err := os.Stat(filepath.Join(binDir, name)); err != nil {
			t.Fatalf("%s binary missing: %v", name, err)
		}
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
	h := &codexSmokeHarness{
		t: t, root: root, home: home, nodeDir: filepath.Join(root, "node"), binDir: binDir,
		env: env, port: port, base: fmt.Sprintf("http://127.0.0.1:%d", port), logPath: filepath.Join(root, "atoll.log"),
	}
	h.proc = startProc(t, "codex-atoll", filepath.Join(binDir, "atoll"), []string{
		"up", "--dir", h.nodeDir, "--addr", fmt.Sprintf("127.0.0.1:%d", port),
	}, root, h.logPath, env)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("atoll log tail:\n%s", tailLog(h.logPath, 160))
		}
	})
	waitHealthz(t, h.base, h.proc, 45*time.Second)
	h.connect()
	return h
}

func (h *codexSmokeHarness) connect() {
	h.t.Helper()
	h.token = waitTextFile(h.t, filepath.Join(h.nodeDir, "server", "atoll-token"), 15*time.Second)
	api := newAPIClient(h.t, h.base)
	api.bearer = h.token
	channels := api.must("GET", "/api/channels", nil, http.StatusOK)
	rows, ok := channels["channels"].([]any)
	if !ok {
		h.t.Fatalf("invalid channel list: %v", channels)
	}
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		if row["name"] == "home" {
			h.homeID, _ = row["id"].(string)
		}
	}
	if h.homeID == "" {
		h.t.Fatalf("provisioned home missing: %v", channels)
	}
	if h.ws != nil {
		h.ws.close()
	}
	h.ws = dialWSBearer(h.t, h.base, h.token, h.homeID)
}

// ask retries only the DOORSTEP — a submit refused because the agent is not
// live yet — and never re-submits once accepted. It deliberately does not use
// submitAndAwaitTerminal: that helper re-submits every 28s, which is right for
// the fast stub agent but wrong for a real Codex turn, where a resubmit lands
// as new content on the live turn and preempts the answer being waited for, so
// any turn slower than the attempt budget could never converge.
func (h *codexSmokeHarness) ask(text string, timeout time.Duration) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		id, refusal := submitOnce(h.t, h.ws, "user.text", rawText(text))
		if id != "" {
			terminal, _ := awaitTerminalAndRecord(h.t, h.ws, id, time.Until(deadline))
			return terminal
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("submit kept being refused (%s) until the deadline", refusal)
		}
		time.Sleep(time.Second)
	}
}

// submitOnce returns the accepted message id, or "" plus the refusal code when
// the door is shut (nobody live to receive). Any other error frame is fatal.
func submitOnce(t *testing.T, ws *wsClient, typ string, payload json.RawMessage) (string, string) {
	t.Helper()
	refCounter++
	ref := fmt.Sprintf("codex-once-%d", refCounter)
	if err := ws.send("submit", ref, map[string]any{"msg_type": typ, "payload": payload}); err != nil {
		t.Fatal(err)
	}
	rec, ok := ws.awaitRef(ref, 10*time.Second)
	if !ok {
		t.Fatalf("%s: no receipt within 10s", typ)
	}
	if rec["frame_type"] == "error" {
		code := frameErrCode(rec)
		if code == "routing_unavailable" || retryableFrameErr(code) {
			return "", code
		}
		t.Fatalf("%s: frame error %q (%s)", typ, code, frameErrDetail(rec))
	}
	p, _ := rec["payload"].(map[string]any)
	id, _ := p["message_id"].(string)
	if id == "" {
		t.Fatalf("%s: receipt has no message_id: %v", typ, rec)
	}
	return id, ""
}

func rawText(text string) json.RawMessage {
	raw, _ := json.Marshal(map[string]string{"text": text})
	return raw
}

func submitRaw(t *testing.T, ws *wsClient, typ string, payload json.RawMessage) string {
	t.Helper()
	refCounter++
	ref := fmt.Sprintf("codex-raw-%d", refCounter)
	if err := ws.send("submit", ref, map[string]any{"msg_type": typ, "payload": payload}); err != nil {
		t.Fatal(err)
	}
	receipt, ok := ws.awaitRef(ref, 10*time.Second)
	if !ok || receipt["frame_type"] != "receipt" {
		t.Fatalf("%s submit receipt=%v", typ, receipt)
	}
	payloadMap, _ := receipt["payload"].(map[string]any)
	id, _ := payloadMap["message_id"].(string)
	if id == "" {
		t.Fatalf("%s receipt has no message_id: %v", typ, receipt)
	}
	return id
}

// selfActorID returns the human's own subject actor id, read back from the
// channel log (provisioning already wrote a row from that subject). There is
// no actor-listing endpoint; the log is the authoritative record either way.
func (h *codexSmokeHarness) selfActorID() string {
	h.t.Helper()
	api := newAPIClient(h.t, h.base)
	api.bearer = h.token
	body := api.must("GET", "/api/channels/"+h.homeID+"/messages?limit=50", nil, http.StatusOK)
	rows, _ := body["messages"].([]any)
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		env, _ := row["envelope"].(map[string]any)
		sender, _ := env["sender"].(map[string]any)
		if id, _ := sender["id"].(string); strings.HasPrefix(id, "human:") {
			return id
		}
	}
	h.t.Fatalf("no human sender in the channel log: %v", body)
	return ""
}

// submitTo posts a request with an explicit audience, so it lands regardless
// of whether the channel's default agent is currently live.
func submitTo(t *testing.T, ws *wsClient, typ string, payload json.RawMessage, audience string) string {
	t.Helper()
	refCounter++
	ref := fmt.Sprintf("codex-to-%d", refCounter)
	if err := ws.send("submit", ref, map[string]any{"msg_type": typ, "payload": payload, "audience": []string{audience}}); err != nil {
		t.Fatal(err)
	}
	receipt, ok := ws.awaitRef(ref, 10*time.Second)
	if !ok || receipt["frame_type"] != "receipt" {
		t.Fatalf("%s addressed submit receipt=%v", typ, receipt)
	}
	payloadMap, _ := receipt["payload"].(map[string]any)
	id, _ := payloadMap["message_id"].(string)
	if id == "" {
		t.Fatalf("%s addressed submit receipt has no message_id: %v", typ, receipt)
	}
	return id
}

func awaitTerminalAndRecord(t *testing.T, ws *wsClient, id string, timeout time.Duration) (map[string]any, []map[string]any) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var seen []map[string]any
	for {
		select {
		case feed := <-ws.tail:
			env, _ := feed["envelope"].(map[string]any)
			if env == nil {
				continue
			}
			seen = append(seen, env)
			if env["kind"] == "response" && env["parent_id"] == id && terminalStatus(env) != "" {
				return env, seen
			}
		case <-timer.C:
			t.Fatalf("request %s has no terminal within %s", id, timeout)
		}
	}
}

func awaitActivity(t *testing.T, ws *wsClient, parentID, typ string, timeout time.Duration) map[string]any {
	t.Helper()
	// Everything seen for this request is kept, so a timeout can say WHY the
	// marker never came (usually: the request already reached a terminal) —
	// a bare "not observed" turns every such failure into a fresh investigation.
	var trail []string
	env, ok := ws.awaitTail(func(env map[string]any) bool {
		if env["parent_id"] == parentID {
			entry := fmt.Sprintf("%v/%v", env["kind"], env["type"])
			if status := payloadField(env, "status"); status != "" {
				entry += "(" + status + ")"
			}
			if code := payloadField(env, "error_code"); code != "" {
				entry += "[" + code + "]"
			}
			trail = append(trail, entry)
		}
		return env["kind"] == "event" && env["type"] == typ && env["parent_id"] == parentID
	}, timeout)
	if !ok {
		t.Fatalf("%s for %s not observed within %s; that request saw: %v", typ, parentID, timeout, trail)
	}
	return env
}

func TestCodexDriverRestartResumeAndMissingRollout(t *testing.T) {
	h := newCodexSmokeHarness(t)
	marker := fmt.Sprintf("RESUME-%d", time.Now().UnixNano())
	if got := payloadField(h.ask("Remember exactly "+marker+" and reply with it only.", 3*time.Minute), "text"); !strings.Contains(got, marker) {
		t.Fatalf("initial marker answer=%q", got)
	}
	_, restarted := submitAndAwaitTerminal(t, h.ws, "agent.restart", json.RawMessage(`{}`), 2*time.Minute)
	if terminalStatus(restarted) != "completed" {
		t.Fatalf("restart terminal=%v", restarted)
	}
	if got := payloadField(h.ask("Reply with the exact marker I asked you to remember.", 3*time.Minute), "text"); !strings.Contains(got, marker) {
		t.Fatalf("restart did not resume thread: %q", got)
	}
	_, _ = submitAndAwaitTerminal(t, h.ws, "agent.terminate", json.RawMessage(`{}`), 2*time.Minute)
	waitForCodexDescendants(t, h.proc.cmd.Process.Pid, false, 8*time.Second)
	for _, dir := range []string{"sessions", "archived_sessions"} {
		if err := os.RemoveAll(filepath.Join(h.home, ".codex", dir)); err != nil {
			t.Fatal(err)
		}
	}
	answer := h.ask("The prior rollout was deliberately removed. Reply exactly: fresh-session", 3*time.Minute)
	if !strings.Contains(strings.ToLower(payloadField(answer, "text")), "fresh-session") {
		t.Fatalf("invalid rollout did not reopen cleanly: %v", answer["payload"])
	}
}

func TestCodexDriverCatchUpAfterDaemonOffline(t *testing.T) {
	h := newCodexSmokeHarness(t)
	h.proc.kill9(t)
	h.ws.close()
	serverLog := filepath.Join(h.root, "server-only.log")
	server := startProc(t, "codex-server-only", filepath.Join(h.binDir, "atoll-server"), []string{
		"--home", filepath.Join(h.nodeDir, "server"), "--addr", fmt.Sprintf("127.0.0.1:%d", h.port),
	}, h.root, serverLog, scrubbedEnv(h.home))
	waitHealthz(t, h.base, server, 30*time.Second)
	h.connect()
	// The token is deliberately opaque: a marker named after the scenario
	// ("OFFLINE-123") invites an answer of the literal word, which would say
	// nothing about whether the row actually reached the model.
	marker := fmt.Sprintf("ZQ%dXK", time.Now().UnixNano())
	// Landing a row while the agent is away needs an explicit audience: a
	// no-audience request is refused at the door (routing_unavailable), and a
	// kind=event row is filtered out of the catch-up window by design
	// (sysactor.includeLogbookRow keeps only request/response). Addressing the
	// human's own subject lands an ordinary request row that catch-up sees.
	self := h.selfActorID()
	submitTo(t, h.ws, "user.text", rawText("Bookkeeping token for later: "+marker), self)
	daemonLog := filepath.Join(h.root, "daemon-rejoined.log")
	_ = startProc(t, "codex-daemon-rejoined", filepath.Join(h.binDir, "atoll-daemon"), []string{
		"--home", filepath.Join(h.nodeDir, "device"),
	}, h.root, daemonLog, h.env)
	answer := h.ask("A bookkeeping token starting with ZQ appears in the recent channel records. Reply with that token verbatim and nothing else.", 4*time.Minute)
	if !strings.Contains(payloadField(answer, "text"), marker) {
		t.Fatalf("catch-up lost the offline token %q: %v", marker, answer["payload"])
	}
}

func TestCodexDriverLogShape(t *testing.T) {
	h := newCodexSmokeHarness(t)
	id := submitRaw(t, h.ws, "user.text", rawText("Reply exactly: log-shape-ok"))
	terminal, seen := awaitTerminalAndRecord(t, h.ws, id, 3*time.Minute)
	if terminalStatus(terminal) != "completed" {
		t.Fatalf("terminal=%v", terminal)
	}
	// The log order is terminal-first, activity.turn.ended-after, so the
	// closing phase marker lands past the terminal we stopped recording at.
	seen = append(seen, awaitActivity(t, h.ws, id, "activity.turn.ended", 30*time.Second))
	started, ended := false, false
	for _, env := range seen {
		typ, _ := env["type"].(string)
		if strings.Contains(strings.ToLower(typ), "delta") {
			t.Fatalf("delta leaked to channel log: %v", env)
		}
		if typ != "activity.turn.started" && typ != "activity.turn.ended" {
			continue
		}
		if env["visibility"] != "public" || env["parent_id"] != id || env["correlation_id"] == "" {
			t.Fatalf("activity envelope shape=%v", env)
		}
		if audience, ok := env["audience"].([]any); !ok || len(audience) != 0 {
			t.Fatalf("activity audience is not []: %#v", env["audience"])
		}
		started = started || typ == "activity.turn.started"
		ended = ended || typ == "activity.turn.ended"
	}
	if !started || !ended {
		t.Fatalf("turn activity pair missing: started=%v ended=%v", started, ended)
	}
}

func TestCodexDriverBusyQueueBatch(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run the shell command `sleep 8`, then reply exactly: initial-finished"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	first := submitRaw(t, h.ws, "agent.queue", rawText("queued marker FIRST"))
	second := submitRaw(t, h.ws, "agent.queue", rawText("queued marker SECOND; reply with FIRST and SECOND"))
	firstTerminal, _ := awaitTerminalAndRecord(t, h.ws, first, 3*time.Minute)
	if envelopePayload(firstTerminal)["merged_into"] != second {
		t.Fatalf("first queued request was not merged into tail: %v", firstTerminal)
	}
	secondTerminal, _ := awaitTerminalAndRecord(t, h.ws, second, 3*time.Minute)
	text := payloadField(secondTerminal, "text")
	if !strings.Contains(text, "FIRST") || !strings.Contains(text, "SECOND") {
		t.Fatalf("batch tail answer=%q", text)
	}
}

func TestCodexDriverSteer(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 10`, then reply exactly: stale-answer"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	steer := submitRaw(t, h.ws, "agent.steer", rawText("Do not wait for the old answer. Reply exactly: steered-answer"))
	oldTerminal, _ := awaitTerminalAndRecord(t, h.ws, active, 2*time.Minute)
	if envelopePayload(oldTerminal)["preempted_by"] != steer {
		t.Fatalf("old owner was not preempted by steer: %v", oldTerminal)
	}
	steerTerminal, _ := awaitTerminalAndRecord(t, h.ws, steer, 3*time.Minute)
	if !strings.Contains(strings.ToLower(payloadField(steerTerminal, "text")), "steered-answer") {
		t.Fatalf("steer answer=%v", steerTerminal["payload"])
	}
}

// A long-final-answer lane deliberately does NOT live here. Whether a model
// reproduces 5 KB verbatim is the model's behaviour; the driver-side property
// (nothing on our path clips the final text) is pinned deterministically by
// codex.TestFinalAnswerSurvivesFarBeyondToolSummaryBound. Measured 2026-08-07:
// the real model stalls on that prompt — Codex's own rollout stops after one
// reasoning item — so this lane only ever measured the weather.

func TestCodexDriverStopRestartTerminate(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 10`, then reply exactly: should-not-land"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	_, stopped := submitAndAwaitTerminal(t, h.ws, "agent.stop", json.RawMessage(`{}`), 2*time.Minute)
	if terminalStatus(stopped) != "completed" {
		t.Fatalf("stop=%v", stopped)
	}
	for _, typ := range []string{"agent.restart", "agent.terminate"} {
		_, terminal := submitAndAwaitTerminal(t, h.ws, typ, json.RawMessage(`{}`), 2*time.Minute)
		if terminalStatus(terminal) != "completed" {
			t.Fatalf("%s=%v", typ, terminal)
		}
	}
	if got := payloadField(h.ask("Reply exactly: lazy-recovered", 3*time.Minute), "text"); !strings.Contains(strings.ToLower(got), "lazy-recovered") {
		t.Fatalf("lazy recovery=%q", got)
	}
}

func TestCodexDriverControlSmoke(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 8`, then reply exactly: control-old"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	_, interrupt := submitAndAwaitTerminal(t, h.ws, "agent.interrupt", json.RawMessage(`{}`), 2*time.Minute)
	if terminalStatus(interrupt) != "completed" {
		t.Fatalf("interrupt control=%v", interrupt)
	}
	_, restart := submitAndAwaitTerminal(t, h.ws, "agent.restart", json.RawMessage(`{}`), 2*time.Minute)
	if terminalStatus(restart) != "completed" {
		t.Fatalf("restart control=%v", restart)
	}
}

func TestCodexDriverProgressLog(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 8`, then reply exactly: progress-first"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	queued := submitRaw(t, h.ws, "agent.queue", rawText("Reply exactly: progress-second"))
	terminal, seen := awaitTerminalAndRecord(t, h.ws, queued, 4*time.Minute)
	if terminalStatus(terminal) != "completed" {
		t.Fatalf("queued terminal=%v", terminal)
	}
	statuses := map[string]bool{}
	for _, env := range seen {
		if env["kind"] == "response" && env["parent_id"] == queued {
			status, _ := envelopePayload(env)["status"].(string)
			statuses[status] = true
		}
	}
	if !statuses["queued"] || !statuses["processing"] || !statuses["completed"] {
		t.Fatalf("queued request progress=%v", statuses)
	}
}

func TestCodexDriverInterrupt(t *testing.T) {
	h := newCodexSmokeHarness(t)
	_, idle := submitAndAwaitTerminal(t, h.ws, "agent.interrupt", json.RawMessage(`{}`), time.Minute)
	if terminalStatus(idle) != "completed" {
		t.Fatalf("idle interrupt=%v", idle)
	}
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 10`, then reply exactly: interrupted-old"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	_, replacement := submitAndAwaitTerminal(t, h.ws, "agent.interrupt", rawText("Reply exactly: interrupt-reopened"), 3*time.Minute)
	if !strings.Contains(strings.ToLower(payloadField(replacement, "text")), "interrupt-reopened") {
		t.Fatalf("content interrupt did not reopen: %v", replacement["payload"])
	}
}

func TestCodexDriverProcessCrashRecovery(t *testing.T) {
	h := newCodexSmokeHarness(t)
	active := submitRaw(t, h.ws, "user.text", rawText("Run `sleep 20`, then reply exactly: crash-old"))
	awaitActivity(t, h.ws, active, "activity.turn.started", 2*time.Minute)
	pids := waitForCodexDescendants(t, h.proc.cmd.Process.Pid, true, 10*time.Second)
	if err := syscall.Kill(pids[0], syscall.SIGKILL); err != nil {
		t.Fatalf("kill Codex app-server %d: %v", pids[0], err)
	}
	terminal, _ := awaitTerminalAndRecord(t, h.ws, active, 2*time.Minute)
	if terminalStatus(terminal) != "failed" || payloadField(terminal, "error_code") != "provider_crash" {
		t.Fatalf("crashed turn terminal=%v", terminal)
	}
	recovered := h.ask("Reply exactly: crash-recovered", 3*time.Minute)
	if got := payloadField(recovered, "text"); !strings.Contains(strings.ToLower(got), "crash-recovered") {
		t.Fatalf("post-crash recovery terminal=%v", recovered)
	}
}

func TestCodexDriverHomeProvisionedDefault(t *testing.T) {
	h := newCodexSmokeHarness(t)
	if h.homeID == "" {
		t.Fatal("home channel was not provisioned")
	}
	answer := h.ask("Reply exactly: provisioned-default", 3*time.Minute)
	if !strings.Contains(strings.ToLower(payloadField(answer, "text")), "provisioned-default") {
		t.Fatalf("default route did not reach Codex: %v", answer["payload"])
	}
}

func waitForCodexDescendants(t *testing.T, rootPID int, want bool, timeout time.Duration) []int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pids := codexDescendants(rootPID)
		if (len(pids) > 0) == want {
			return pids
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Codex descendant presence did not become %v", want)
	return nil
}

func codexDescendants(rootPID int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	parents := map[int]int{}
	cmdlines := map[int]string{}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		status, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "status"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "PPid:") {
				parents[pid], _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")))
				break
			}
		}
		cmdline, _ := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		cmdlines[pid] = strings.ReplaceAll(string(cmdline), "\x00", " ")
	}
	var found []int
	for pid, cmdline := range cmdlines {
		if !strings.Contains(cmdline, "app-server --stdio") || !strings.Contains(cmdline, "codex") {
			continue
		}
		for ancestor := parents[pid]; ancestor > 1; ancestor = parents[ancestor] {
			if ancestor == rootPID {
				found = append(found, pid)
				break
			}
		}
	}
	return found
}
