package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCoderunnerHumanJourney(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is unavailable")
	}
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": "coderunner-host"})
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": stringField(t, device, "id"),
	})
	daemonLog := filepath.Join(h.root, "logs", "coderunner-daemon.log")
	daemon := startProc(t, "coderunner-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", "coderunner-host", "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	echoID := introduceClass(t, ws, registrar, "coderunner-echo", "coderunner-echo", "echo", map[string]any{})
	runnerID := introduceClass(t, ws, registrar, "coderunner-mode-one", "coderunner-mode-one", "coderunner", map[string]any{})
	waitActorPresence(t, ws, echoID, true, daemon, daemonLog)
	waitActorPresence(t, ws, runnerID, true, daemon, daemonLog)

	rootID := rootActorID(t, ws, c0ChannelID)
	program := `export async function run({atoll,args}) {
  const [first,second] = await atoll.all([
    () => atoll.call({target:"echo", type:"echo.say", input:{text:args.first}}),
    () => atoll.call({target:"echo", type:"echo.say", input:{text:args.second}})
  ]);
  const members = await atoll.call({target:"system", type:"system.member.list", input:{}});
  return {texts:[first.text,second.text], memberCount:members.actors.length};
}`
	runID, completed := ws.requestWithID(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": program, "requires": []string{"echo", "system"}, "args": map[string]any{"first": "one", "second": "two"},
	})
	value, _ := completed["value"].(map[string]any)
	texts, _ := value["texts"].([]any)
	if len(texts) != 2 || texts[0] != "one" || texts[1] != "two" {
		t.Fatalf("mode-one result=%v", completed)
	}
	assertCoderunnerCalls(t, api, runID, runnerID, echoID, rootID, 2, true)

	undeclaredProgram := `export async function run({atoll}) { try { await atoll.call({target:"tool:missing:1",type:"echo.say",input:{}}) } catch (e) { return {code:e.code} } }`
	undeclaredID, undeclared := ws.requestWithID(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": undeclaredProgram, "requires": []string{},
	})
	undeclaredValue, _ := undeclared["value"].(map[string]any)
	if undeclaredValue["code"] != "undeclared_capability" {
		t.Fatalf("undeclared result=%v", undeclared)
	}
	assertNoChildType(t, api, undeclaredID, "echo.say")

	missingRunID, missing, missingErr := ws.tryRequest(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run(){return null}`, "requires": []string{"mcp:nonexistent"},
	})
	if missingErr == nil || missing["error_code"] != "dependency_missing" {
		t.Fatalf("dependency failure=%v err=%v", missing, missingErr)
	}
	missingList, _ := missing["missing"].([]any)
	if len(missingList) != 1 || missingList[0] != "mcp:nonexistent" {
		t.Fatalf("dependency missing field=%v", missing)
	}
	if strings.Contains(tailLog(daemonLog, 100), `"request":"`+missingRunID+`"`) {
		t.Fatalf("dependency-missing request started node: %s", missingRunID)
	}

	_, thrown, thrownErr := ws.tryRequest(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run({atoll}) { atoll.log("before"); throw new Error("x") }`, "requires": []string{},
	})
	if thrownErr == nil || thrown["error_code"] != "runtime_failed" || thrown["kind"] != "exception" || thrown["message"] != "x" {
		t.Fatalf("runtime failure=%v err=%v", thrown, thrownErr)
	}
	if logs, _ := thrown["logs"].([]any); len(logs) == 0 {
		t.Fatalf("runtime failure omitted logs: %v", thrown)
	}

	cancelID := ws.submit(c0ChannelID, "code.run", "request", []string{runnerID}, map[string]any{
		"program": `export async function run(){ await new Promise(resolve => setTimeout(resolve, 60000)); return null }`, "requires": []string{},
	})
	waitForNodeChild(t, daemon.cmd.Process.Pid, true)
	ws.cancel(c0ChannelID, cancelID)
	cancelTerminal := ws.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" || envelope["parent_id"] != cancelID {
			return false
		}
		payload, _ := envelope["payload"].(map[string]any)
		return payload["status"] == "failed"
	}, 10*time.Second)
	cancelBody, _ := cancelTerminal["payload"].(map[string]any)
	// A human cancel self-closes its own account before the receiver sees the
	// cancel hint. The coderunner's invariant here is process cleanup; the
	// terminal vocabulary remains the channel-wide unanswered_timeout shape.
	if cancelBody["error_code"] != "unanswered_timeout" || cancelBody["cancelled"] != true {
		t.Fatalf("cancel terminal=%v", cancelBody)
	}
	waitForNodeChild(t, daemon.cmd.Process.Pid, false)

	_, flooded, floodedErr := ws.tryRequest(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run(){ const line="x".repeat(1024); for(let i=0;i<1100;i++) console.log(line); return null }`, "requires": []string{},
	})
	if floodedErr == nil || flooded["error_code"] != "output_limit" {
		t.Fatalf("output limit=%v err=%v", flooded, floodedErr)
	}
	waitForNodeChild(t, daemon.cmd.Process.Pid, false)

	fixedProgram := `export async function run({atoll,args}) { const out=await atoll.call({target:"echo",type:"echo.say",input:{text:args.text}}); return {text:out.text} }`
	fixedID := introduceClass(t, ws, registrar, "coderunner-fixed", "coderunner-fixed", "coderunner", map[string]any{
		"program": fixedProgram, "requires": []string{"echo"},
	})
	waitActorPresence(t, ws, fixedID, true, daemon, daemonLog)
	fixedRunID, fixed := ws.requestWithID(c0ChannelID, "code.run", fixedID, map[string]any{"args": map[string]any{"text": "fixed"}})
	fixedValue, _ := fixed["value"].(map[string]any)
	if fixedValue["text"] != "fixed" {
		t.Fatalf("fixed result=%v", fixed)
	}
	assertCoderunnerCalls(t, api, fixedRunID, fixedID, echoID, fixedID, 1, false)
	_, invalid, invalidErr := ws.tryRequest(c0ChannelID, "code.run", fixedID, map[string]any{"program": fixedProgram})
	if invalidErr == nil || invalid["error_code"] != "invalid_input" {
		t.Fatalf("fixed mode accepted request program: %v err=%v", invalid, invalidErr)
	}
}

func (c *wsClient) cancel(channelID, requestID string) {
	c.t.Helper()
	wireRef++
	ref := fmt.Sprintf("cancel-%d", wireRef)
	if err := c.conn.WriteJSON(wireFrame("cancel", ref, map[string]any{"channel_id": channelID, "req_id": requestID})); err != nil {
		c.t.Fatal(err)
	}
	ack := c.awaitAck(ref, 10*time.Second)
	if ack["frame_type"] != "receipt" {
		c.t.Fatalf("cancel rejected: %v", ack)
	}
}

func waitForNodeChild(t *testing.T, parentPID int, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last []string
	for time.Now().Before(deadline) {
		statuses, err := filepath.Glob("/proc/[0-9]*/status")
		if err != nil {
			t.Skipf("cannot inspect daemon children: %v", err)
		}
		found := false
		last = nil
		for _, statusPath := range statuses {
			raw, readErr := os.ReadFile(statusPath)
			if readErr != nil {
				continue
			}
			ppid := -1
			for _, line := range strings.Split(string(raw), "\n") {
				if value, ok := strings.CutPrefix(line, "PPid:"); ok {
					ppid, _ = strconv.Atoi(strings.TrimSpace(value))
					break
				}
			}
			if ppid != parentPID {
				continue
			}
			pidText := filepath.Base(filepath.Dir(statusPath))
			pid, _ := strconv.Atoi(pidText)
			comm, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
			last = append(last, fmt.Sprintf("%d:%s", pid, strings.TrimSpace(string(comm))))
			if strings.TrimSpace(string(comm)) == "node" {
				found = true
				break
			}
		}
		if found == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node child presence under daemon %d did not become %v; children=%v", parentPID, want, last)
}

func introduceClass(t *testing.T, ws *wsClient, registrar, declID, name, class string, config map[string]any) string {
	t.Helper()
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": declID, "name": name, "class": class, "config": config, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": declID})
	return stringField(t, introduced, "member")
}

func assertCoderunnerCalls(t *testing.T, api *apiClient, rootRequestID, senderID, targetID, effectiveActor string, want int, forwarded bool) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	seen := map[string]bool{}
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for len(seen) < want {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "request" || envelope["type"] != "echo.say" || envelope["parent_id"] != rootRequestID {
				continue
			}
			sender, _ := envelope["sender"].(map[string]any)
			if sender["id"] != senderID || !audienceContains(envelope["audience"], targetID) {
				t.Fatalf("wrong coderunner child request=%v", envelope)
			}
			payload, _ := envelope["payload"].(map[string]any)
			context, hasContext := payload["_context"].(map[string]any)
			if forwarded {
				caller, _ := context["caller"].(map[string]any)
				if !hasContext || caller["actor"] != effectiveActor {
					t.Fatalf("forwarded caller=%v want=%s envelope=%v", caller, effectiveActor, envelope)
				}
			} else if hasContext {
				t.Fatalf("fixed mode unexpectedly forwarded caller: %v", envelope)
			}
			seen[fmt.Sprint(envelope["id"])] = true
		case <-deadline.C:
			t.Fatalf("found %d/%d coderunner child calls", len(seen), want)
		}
	}
}

func assertNoChildType(t *testing.T, api *apiClient, parentID, msgType string) {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	timer := time.NewTimer(750 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope != nil && envelope["kind"] == "request" && envelope["parent_id"] == parentID && envelope["type"] == msgType {
				t.Fatalf("undeclared call reached ledger: %v", envelope)
			}
		case <-timer.C:
			return
		}
	}
}
