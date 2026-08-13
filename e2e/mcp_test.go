package e2e

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var referenceMCPTools = []string{
	"echo", "add", "create_order", "set_priority", "search",
	"describe_shape", "structured_report", "fail_tool_error",
	"fail_protocol_error", "slow_task", "never_returns", "log_when_asked",
	"book_ticket", "toggle_extra_tool", "long_job",
}

const (
	referenceMCPServerName   = "mcp-v2-reference-testserver"
	referenceMCPVersion      = "0.1.0"
	referenceMCPInstructions = "Deterministic MCP 2026-07-28 protocol and tool conformance fixture."
)

func TestMCPClassDynamicHumanJourney(t *testing.T) {
	python := os.Getenv("ATOLL_MCP_TESTSERVER")
	if python == "" {
		t.Skip("ATOLL_MCP_TESTSERVER is unset; set it to mcp-testserver/.venv/bin/python to run the real MCP fixture")
	}
	python, err := filepath.Abs(python)
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Dir(filepath.Dir(filepath.Dir(python)))
	if _, err := os.Stat(filepath.Join(project, "server", "main.py")); err != nil {
		t.Fatalf("ATOLL_MCP_TESTSERVER does not point into mcp-testserver/.venv: %v", err)
	}

	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, ws)
	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "e2e-mcp-daemon"})
	deviceID := stringField(t, device, "id")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": deviceID,
	})
	daemonLog := filepath.Join(h.root, "logs", "mcp-daemon.log")
	daemon := startProc(t, "mcp-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", "e2e-mcp-daemon", "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	const stdioDecl = "e2e-mcp-stdio"
	const stdioName = "local-stdio"
	stdioID := registerAndIntroduceMCP(t, ws, registrar, stdioDecl, map[string]any{
		"name": stdioName, "transport": "stdio", "command": python,
		"args": []string{"-m", "server.main", "--transport", "stdio"}, "cwd": project,
		"call_timeout_ms": 750,
	})
	waitActorPresence(t, ws, stdioID, true, daemon, daemonLog)
	stdioChild := waitMCPChild(t, daemon.cmd.Process.Pid, true)

	requestID, echo := ws.requestWithID(c0ChannelID, stdioName+".echo", stdioID, map[string]any{"text": "dynamic-stdio"})
	if echo["text"] != "dynamic-stdio" {
		t.Fatalf("stdio echo=%v", echo)
	}
	assertAdjacentQuestionAnswer(t, api, requestID)
	stdioDescribe := ws.request(c0ChannelID, "actor.describe", stdioID, map[string]any{})
	assertMCPDescribe(t, stdioDescribe, stdioName, 15)
	t.Logf("stdio: %s.echo returned %q; describe exposed 15 tools and server self-report", stdioName, echo["text"])
	assertMCPTimeoutSurvives(t, api, ws, stdioID, stdioName)

	const scriptDecl = "e2e-mcp-script"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": scriptDecl, "name": scriptDecl, "class": "script",
		"config":     map[string]any{"tool_id": stdioID, "tool_type": stdioName + ".echo"},
		"visibility": "private",
	})
	scriptIntro := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{
		"kind": "agent", "decl_id": scriptDecl,
	})
	scriptID := stringField(t, scriptIntro, "instance_id")
	waitActorPresence(t, ws, scriptID, true, daemon, daemonLog)
	_, scriptReply, scriptErr := ws.tryRequest(c0ChannelID, "loop.chat", scriptID, map[string]any{"text": "through-script-agent"})
	if scriptErr == nil || !strings.Contains(fmt.Sprint(scriptReply["detail"]), "resource_not_found") {
		t.Fatalf("script's expected post-tool resource failure=%v err=%v", scriptReply, scriptErr)
	}
	assertActorSubcallCompleted(t, api, scriptID, stdioID, stdioName+".echo", "through-script-agent")
	t.Logf("script agent called %s.echo through call_actor", stdioName)

	httpPort := freePort(t)
	httpLog := filepath.Join(h.root, "logs", "mcp-http.log")
	httpServer := startProc(t, "mcp-http", python, []string{
		"-m", "server.main", "--transport", "http", "--port", strconv.Itoa(httpPort),
	}, h.env, project, httpLog)
	if err := waitHealthTCP(fmt.Sprintf("127.0.0.1:%d", httpPort), httpServer, 10*time.Second); err != nil {
		t.Fatalf("%v\n%s", err, tailLog(httpLog, 100))
	}

	const httpDecl = "e2e-mcp-http"
	const httpName = "local-http"
	httpConfig := map[string]any{
		"name": httpName, "transport": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/mcp", httpPort),
		"call_timeout_ms": 750,
	}
	httpID := registerAndIntroduceMCP(t, ws, registrar, httpDecl, httpConfig)
	waitActorPresence(t, ws, httpID, true, daemon, daemonLog)
	httpEcho := ws.request(c0ChannelID, httpName+".echo", httpID, map[string]any{"text": "dynamic-http"})
	if httpEcho["text"] != "dynamic-http" {
		t.Fatalf("http echo=%v", httpEcho)
	}
	httpDescribe := ws.request(c0ChannelID, "actor.describe", httpID, map[string]any{})
	assertMCPDescribe(t, httpDescribe, httpName, 15)
	t.Logf("http: %s.echo returned %q; describe exposed 15 tools and server self-report", httpName, httpEcho["text"])
	assertMCPTimeoutSurvives(t, api, ws, httpID, httpName)
	assertMCPHTTPConcurrent(t, api, ws, httpID, httpName)

	renamedConfig := make(map[string]any, len(httpConfig))
	for key, value := range httpConfig {
		renamedConfig[key] = value
	}
	const renamedName = "renamed-http"
	renamedConfig["name"] = renamedName
	const renamedDecl = "e2e-mcp-http-renamed"
	renamedID := registerAndIntroduceMCP(t, ws, registrar, renamedDecl, renamedConfig)
	waitActorPresence(t, ws, renamedID, true, daemon, daemonLog)
	renamedDescribe := ws.request(c0ChannelID, "actor.describe", renamedID, map[string]any{})
	assertMCPDescribe(t, renamedDescribe, renamedName, 15)
	assertMCPTypePrefixChanged(t, httpDescribe, httpName, renamedDescribe, renamedName)
	t.Logf("name-only config change renamed all 15 types from %s.* to %s.*", httpName, renamedName)

	slow := ws.request(c0ChannelID, httpName+".slow_task", httpID, map[string]any{"seconds": 0.12})
	if slow["text"] != "slow task complete" {
		t.Fatalf("slow_task=%v", slow)
	}

	_, business, businessErr := ws.tryRequest(c0ChannelID, httpName+".fail_tool_error", httpID, map[string]any{})
	_, protocol, protocolErr := ws.tryRequest(c0ChannelID, httpName+".fail_protocol_error", httpID, map[string]any{})
	if businessErr == nil || protocolErr == nil {
		t.Fatalf("failure terminals: business=%v err=%v protocol=%v err=%v", business, businessErr, protocol, protocolErr)
	}
	if business["error_code"] != "mcp_tool_error" || !strings.Contains(fmt.Sprint(business["detail"]), "intentional tool execution failure") {
		t.Fatalf("business failure=%v", business)
	}
	if protocol["error_code"] != "mcp_protocol_-32602" || !strings.Contains(fmt.Sprint(protocol["detail"]), "-32602") {
		t.Fatalf("protocol failure=%v", protocol)
	}
	t.Logf("failures distinguished: business=%v protocol=%v", business["error_code"], protocol["error_code"])

	missingPort := freePort(t)
	const absentDecl = "e2e-mcp-absent"
	const absentName = "local-absent"
	absentID := registerAndIntroduceMCP(t, ws, registrar, absentDecl, map[string]any{
		"name": absentName, "transport": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/mcp", missingPort),
	})
	waitActorPresence(t, ws, absentID, true, daemon, daemonLog)
	absentDescribe := ws.request(c0ChannelID, "actor.describe", absentID, map[string]any{})
	if !strings.Contains(fmt.Sprint(absentDescribe["description"]), "从未成功连接") {
		t.Fatalf("absent description=%v", absentDescribe)
	}
	if types, ok := absentDescribe["types"].(map[string]any); ok && len(types) != 0 {
		t.Fatalf("absent actor fabricated types=%v", types)
	}
	_, absentCall, absentCallErr := ws.tryRequest(c0ChannelID, absentName+".echo", absentID, map[string]any{"text": "must-fail"})
	if absentCallErr == nil || absentCall["error_code"] != "mcp_unreachable" {
		t.Fatalf("never-connected call=%v err=%v", absentCall, absentCallErr)
	}
	t.Logf("never-connected actor reported reachability failure and fabricated no types")

	httpServer.kill9(t)
	disconnected := ws.request(c0ChannelID, "actor.describe", httpID, map[string]any{})
	if !strings.Contains(fmt.Sprint(disconnected["description"]), "当前够不着") {
		t.Fatalf("disconnected description=%v", disconnected)
	}
	assertMCPDescribe(t, disconnected, httpName, 15)
	_, failedCall, callErr := ws.tryRequest(c0ChannelID, httpName+".echo", httpID, map[string]any{"text": "must-fail"})
	if callErr == nil || failedCall["error_code"] != "mcp_unreachable" {
		t.Fatalf("disconnected call=%v err=%v", failedCall, callErr)
	}
	t.Logf("disconnected actor retained 15-type snapshot and subsequent call failed")

	removed := ws.request(c0ChannelID, "channel.remove_actor", systemActor, map[string]any{"instance_id": stdioID})
	removedIDs, _ := removed["removed"].([]any)
	if len(removedIDs) != 1 || removedIDs[0] != stdioID {
		t.Fatalf("remove stdio=%v", removed)
	}
	waitActorPresence(t, ws, stdioID, false, nil, daemonLog)
	waitPIDGone(t, stdioChild)
	t.Logf("removed stdio actor; child pid %d no longer exists", stdioChild)
}

func registerAndIntroduceMCP(t *testing.T, ws *wsClient, registrar, declID string, config map[string]any) string {
	t.Helper()
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declID, "name": declID, "class": "mcp", "config": config, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{
		"kind": "tool", "decl_id": declID,
	})
	return stringField(t, introduced, "instance_id")
}

func assertMCPDescribe(t *testing.T, describe map[string]any, prefix string, want int) {
	t.Helper()
	types, _ := describe["types"].(map[string]any)
	if len(types) != want {
		t.Fatalf("%s types=%d want=%d: %v", prefix, len(types), want, describe)
	}
	for _, tool := range referenceMCPTools {
		if _, ok := types[prefix+"."+tool]; !ok {
			t.Fatalf("describe omitted %s.%s", prefix, tool)
		}
	}
	create, _ := types[prefix+".create_order"].(map[string]any)
	notes := fmt.Sprint(create["notes"])
	if !strings.HasPrefix(notes, "过渡形：") || !strings.Contains(notes, `"$defs"`) {
		t.Fatalf("create_order raw schema notes=%q", notes)
	}
	for _, field := range []string{"description", "skill_doc"} {
		text := fmt.Sprint(describe[field])
		for _, value := range []string{referenceMCPServerName, referenceMCPVersion, referenceMCPInstructions} {
			if !strings.Contains(text, value) {
				t.Fatalf("%s omitted server self-report %q: %q", field, value, text)
			}
		}
	}
}

func assertMCPTypePrefixChanged(t *testing.T, before map[string]any, beforePrefix string, after map[string]any, afterPrefix string) {
	t.Helper()
	beforeTypes, _ := before["types"].(map[string]any)
	afterTypes, _ := after["types"].(map[string]any)
	for _, tool := range referenceMCPTools {
		if _, ok := beforeTypes[afterPrefix+"."+tool]; ok {
			t.Fatalf("original config unexpectedly used renamed prefix %s.%s", afterPrefix, tool)
		}
		if _, ok := afterTypes[beforePrefix+"."+tool]; ok {
			t.Fatalf("name-only config change retained old prefix %s.%s", beforePrefix, tool)
		}
	}
}

func assertMCPTimeoutSurvives(t *testing.T, api *apiClient, ws *wsClient, actorID, prefix string) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	requestID := ws.submit(c0ChannelID, prefix+".never_returns", "request", []string{actorID}, map[string]any{})
	request := audit.awaitEnvelope(func(envelope map[string]any) bool {
		return envelope["id"] == requestID
	}, 10*time.Second)
	// ws.submit has no expires-at argument, so this is deliberately a caller-
	// unbounded request. The substrate currently writes its own 24h fallback
	// into the durable envelope; accept that fallback, but reject any short
	// caller-style deadline that could mask the MCP actor's own timeout.
	if expires, present := request["expires_at"].(float64); present {
		if remaining := time.Until(time.UnixMilli(int64(expires))); remaining < 23*time.Hour {
			t.Fatalf("%s never_returns got a short external expires_at (%s)", prefix, remaining)
		}
	}
	terminal := awaitMCPTerminal(t, ws, requestID, 10*time.Second)
	if terminal["status"] != "failed" || terminal["error_code"] != "mcp_timeout" {
		t.Fatalf("%s never_returns terminal=%v", prefix, terminal)
	}
	describe := ws.request(c0ChannelID, "actor.describe", actorID, map[string]any{})
	assertMCPDescribe(t, describe, prefix, 15)
	echo := ws.request(c0ChannelID, prefix+".echo", actorID, map[string]any{"text": "after-timeout"})
	if echo["text"] != "after-timeout" {
		t.Fatalf("%s echo after timeout=%v", prefix, echo)
	}
	t.Logf("%s: caller-unbounded never_returns failed as mcp_timeout, then describe and echo remained healthy", prefix)
}

func assertMCPHTTPConcurrent(t *testing.T, api *apiClient, ws *wsClient, actorID, prefix string) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	slowWS := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	slowID := slowWS.submit(c0ChannelID, prefix+".slow_task", "request", []string{actorID}, map[string]any{"seconds": 0.4})
	echoID, echo := ws.requestWithID(c0ChannelID, prefix+".echo", actorID, map[string]any{"text": "overtook-slow"})
	if echo["text"] != "overtook-slow" {
		t.Fatalf("concurrent echo=%v", echo)
	}
	slow := awaitMCPTerminal(t, slowWS, slowID, 10*time.Second)
	if slow["status"] != "completed" || slow["text"] != "slow task complete" {
		t.Fatalf("concurrent slow_task=%v", slow)
	}
	seqs := map[string]float64{}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for len(seqs) != 2 {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "response" {
				continue
			}
			parent, _ := envelope["parent_id"].(string)
			if parent == echoID || parent == slowID {
				seqs[parent], _ = item["seq"].(float64)
			}
		case <-deadline.C:
			t.Fatalf("did not observe both concurrent terminals: %v", seqs)
		}
	}
	if seqs[echoID] >= seqs[slowID] {
		t.Fatalf("http echo did not overtake slow_task: echo seq=%.0f slow seq=%.0f", seqs[echoID], seqs[slowID])
	}
	t.Logf("http concurrency: echo terminal seq %.0f preceded slow_task seq %.0f", seqs[echoID], seqs[slowID])
}

func awaitMCPTerminal(t *testing.T, ws *wsClient, requestID string, timeout time.Duration) map[string]any {
	t.Helper()
	terminal := ws.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" || envelope["parent_id"] != requestID {
			return false
		}
		body, _ := envelope["payload"].(map[string]any)
		return body["status"] == "completed" || body["status"] == "failed"
	}, timeout)
	body, _ := terminal["payload"].(map[string]any)
	return body
}

func assertAdjacentQuestionAnswer(t *testing.T, api *apiClient, requestID string) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	var requestSeq, responseSeq float64
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for requestSeq == 0 || responseSeq == 0 {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil {
				continue
			}
			seq, _ := item["seq"].(float64)
			if envelope["id"] == requestID {
				requestSeq = seq
			}
			if envelope["parent_id"] == requestID && envelope["kind"] == "response" {
				responseSeq = seq
			}
		case <-deadline.C:
			t.Fatalf("audit did not find request/response %s", requestID)
		}
	}
	if responseSeq != requestSeq+1 {
		t.Fatalf("MCP call appended extra rows: request seq=%v response seq=%v", requestSeq, responseSeq)
	}
	t.Logf("request/response log rows adjacent at seq %.0f/%.0f", requestSeq, responseSeq)
}

func assertActorSubcallCompleted(t *testing.T, api *apiClient, senderID, targetID, msgType, wantText string) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	var requestID string
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["type"] != msgType {
				continue
			}
			sender, _ := envelope["sender"].(map[string]any)
			if envelope["kind"] == "request" && sender["id"] == senderID && audienceContains(envelope["audience"], targetID) {
				requestID, _ = envelope["id"].(string)
				continue
			}
			if requestID != "" && envelope["kind"] == "response" && envelope["parent_id"] == requestID {
				payload, _ := envelope["payload"].(map[string]any)
				if payload["status"] != "completed" || payload["text"] != wantText {
					t.Fatalf("script MCP subcall terminal=%v", envelope)
				}
				return
			}
		case <-deadline.C:
			t.Fatalf("did not find completed %s subcall from %s to %s", msgType, senderID, targetID)
		}
	}
}

func audienceContains(raw any, target string) bool {
	values, _ := raw.([]any)
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func waitHealthTCP(address string, p *proc, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("%s exited before listening", p.name)
		}
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s did not listen on %s", p.name, address)
}

func waitMCPChild(t *testing.T, daemonPID int, want bool) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := exec.Command("pgrep", "-P", strconv.Itoa(daemonPID), "-f", "server.main.*stdio").Output()
		if err == nil {
			fields := strings.Fields(string(raw))
			if len(fields) > 0 {
				pid, _ := strconv.Atoi(fields[0])
				if want {
					return pid
				}
			}
		} else if !want {
			return 0
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("stdio child presence never became %v", want)
	return 0
}

func waitPIDGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("stdio child %d still exists after actor removal", pid)
}
