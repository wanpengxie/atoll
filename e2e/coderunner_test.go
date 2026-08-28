package e2e

import (
	"encoding/json"
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
	deviceID := stringField(t, device, "id")
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": deviceID,
	})
	daemonLog := filepath.Join(h.root, "logs", "coderunner-daemon.log")
	daemon := startProc(t, "coderunner-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", "coderunner-host", "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	echoID := introduceClass(t, ws, registrar, "coderunner-echo", "coderunner-echo", "echo", deviceID, map[string]any{})
	runnerID := introduceClass(t, ws, registrar, "coderunner-mode-one", "coderunner-mode-one", "coderunner", deviceID, map[string]any{})
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

	// code.validate: the pre-flight half of code.run. Same resolution, plus
	// each resolved actor's manifest; never starts Node.
	validateID, validated := ws.requestWithID(c0ChannelID, "code.validate", runnerID, map[string]any{
		"requires": []string{"echo", "system", "mcp:nonexistent"},
	})
	if validated["ok"] != false {
		t.Fatalf("validate with a missing requirement must not be ok: %v", validated)
	}
	validateMissing, _ := validated["missing"].([]any)
	if len(validateMissing) != 1 || validateMissing[0] != "mcp:nonexistent" {
		t.Fatalf("validate missing=%v", validated)
	}
	validateResolved, _ := validated["resolved"].(map[string]any)
	echoEntry, _ := validateResolved["echo"].(map[string]any)
	if echoEntry["actor"] != echoID || echoEntry["class"] != "echo" {
		t.Fatalf("validate resolved echo=%v (want %s)", echoEntry, echoID)
	}
	echoWords, _ := echoEntry["words"].(map[string]any)
	if _, ok := echoWords["echo.say"]; !ok {
		t.Fatalf("validate must carry echo's words, got %v", echoWords)
	}
	systemEntry, _ := validateResolved["system"].(map[string]any)
	systemWords, _ := systemEntry["words"].(map[string]any)
	if _, ok := systemWords["system.member.list"]; !ok {
		t.Fatalf("validate must carry the system door's words, got %v", systemEntry)
	}
	if strings.Contains(tailLog(daemonLog, 100), `"request":"`+validateID+`"`) {
		t.Fatalf("code.validate started node: %s", validateID)
	}
	_, badValidate, badValidateErr := ws.tryRequest(c0ChannelID, "code.validate", runnerID, map[string]any{
		"requires": []string{}, "program": "export async function run(){}",
	})
	if badValidateErr == nil || badValidate["error_code"] != "invalid_input" {
		t.Fatalf("code.validate must refuse program (code is not config): %v err=%v", badValidate, badValidateErr)
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

	// --- closure matrix: timeout / late result / concurrency / no auto retry ---

	// timeout: the declared deadline (a short expires_at on the run) collapses
	// the receiver's in-station account first, so the coderunner's own
	// `timeout` terminal is a late write and is refused — the receiver never
	// closes a deadline. What must hold: the runtime is killed promptly, and
	// the substrate reaper closes the run as unanswered_timeout, system-signed,
	// within one reconcile cadence (30 s). Logs cannot ride that terminal.
	timeoutID := ws.submitWithExpiry(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run(){ console.log("before hang"); await new Promise(resolve => setTimeout(resolve, 60000)); return null }`, "requires": []string{},
	}, time.Now().Add(1500*time.Millisecond).UnixMilli())
	waitForNodeChild(t, daemon.cmd.Process.Pid, true)
	waitForNodeChild(t, daemon.cmd.Process.Pid, false)
	timeoutEnvelope := ws.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" || envelope["parent_id"] != timeoutID {
			return false
		}
		body, _ := envelope["payload"].(map[string]any)
		return body["status"] == "failed"
	}, 45*time.Second)
	timeoutTerminal, _ := timeoutEnvelope["payload"].(map[string]any)
	timeoutSender, _ := timeoutEnvelope["sender"].(map[string]any)
	if timeoutTerminal["reason"] != "unanswered_timeout" || timeoutTerminal["closed_by"] != "system" || timeoutSender["id"] != "system" {
		t.Fatalf("timeout terminal=%v sender=%v (want the reaper's system-signed unanswered_timeout)", timeoutTerminal, timeoutSender)
	}
	if n := countTerminals(t, api, timeoutID); n != 1 {
		t.Fatalf("timed-out run has %d terminals, want exactly 1 (the coderunner's late `timeout` must be refused)", n)
	}

	// late result: a run is cancelled while its child call (echo countdown, 3 s)
	// is still counting. The child is cancelled with the run; echo's real
	// answer lands after closure and is absorbed as late — exactly one
	// terminal on the run and exactly one on the child.
	lateID := ws.submit(c0ChannelID, "code.run", "request", []string{runnerID}, map[string]any{
		"program": `export async function run({atoll}){ return await atoll.call({target:"echo", type:"countdown.start", input:{seconds:3, note:"late"}, deadlineMs: 20000}) }`, "requires": []string{"echo"},
	})
	lateChild := ws.awaitEnvelope(func(envelope map[string]any) bool {
		return envelope["kind"] == "request" && envelope["type"] == "countdown.start" && envelope["parent_id"] == lateID
	}, 15*time.Second)
	lateChildID, _ := lateChild["id"].(string)
	ws.cancel(c0ChannelID, lateID)
	lateTerminal := ws.awaitTerminal(lateID, 15*time.Second)
	if lateTerminal["error_code"] != "unanswered_timeout" || lateTerminal["cancelled"] != true {
		t.Fatalf("late-run terminal=%v", lateTerminal)
	}
	// Wait past the countdown so echo's late Reply has definitely been attempted.
	time.Sleep(4 * time.Second)
	if n := countTerminals(t, api, lateID); n != 1 {
		t.Fatalf("run %s has %d terminals, want exactly 1", lateID, n)
	}
	if n := countTerminals(t, api, lateChildID); n != 1 {
		t.Fatalf("late child %s has %d terminals, want exactly 1 (the cancel; echo's answer must be absorbed as late)\nrows under child: %s\nrows under run: %s\nserver log:\n%s\ndaemon log:\n%s", lateChildID, n, dumpUnder(t, api, lateChildID), dumpUnder(t, api, lateID), tailLog(h.server.logPath, 60), tailLog(daemonLog, 60))
	}
	waitForNodeChild(t, daemon.cmd.Process.Pid, false)

	// concurrency (the only "busy" a tool has): three runs on one member at
	// once, each its own process, each its own terminal; and one run fanning
	// 20 child calls through the 8-wide host semaphore, all answered.
	parallelIDs := map[string]bool{}
	for i := 0; i < 3; i++ {
		id := ws.submit(c0ChannelID, "code.run", "request", []string{runnerID}, map[string]any{
			"program": `export async function run({args}){ await new Promise(resolve => setTimeout(resolve, 500)); return args.i }`, "requires": []string{}, "args": map[string]any{"i": i},
		})
		parallelIDs[id] = false
	}
	parallelValues := map[string]any{}
	ws.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" {
			return false
		}
		parent, _ := envelope["parent_id"].(string)
		if _, mine := parallelIDs[parent]; !mine {
			return false
		}
		body, _ := envelope["payload"].(map[string]any)
		if body["status"] != "completed" {
			return false
		}
		parallelIDs[parent] = true
		parallelValues[parent] = body["value"]
		for _, seen := range parallelIDs {
			if !seen {
				return false
			}
		}
		return true
	}, 20*time.Second)
	seenValues := map[any]bool{}
	for _, value := range parallelValues {
		seenValues[value] = true
	}
	if len(seenValues) != 3 {
		t.Fatalf("parallel runs did not each return their own arg: %v", parallelValues)
	}
	waitForNodeChild(t, daemon.cmd.Process.Pid, false)

	fanID, fanned := ws.requestWithID(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run({atoll}){ const outs = await atoll.all(Array.from({length:20}, (_, i) => () => atoll.call({target:"echo", type:"echo.say", input:{text:"n"+i}}))); return outs.map(o => o.text) }`, "requires": []string{"echo"},
	})
	if fanValues, _ := fanned["value"].([]any); len(fanValues) != 20 {
		t.Fatalf("fan-out returned %d of 20: %v", len(fanValues), fanned)
	}
	assertCoderunnerCalls(t, api, fanID, runnerID, echoID, rootID, 20, true)

	// no automatic retry: a failed run stays failed; nothing spawns a second
	// attempt on the caller's behalf. A retry is the caller's own new request.
	failedRetryID, _, failedRetryErr := ws.tryRequest(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run(){ throw new Error("flaky") }`, "requires": []string{},
	})
	if failedRetryErr == nil {
		t.Fatal("flaky program unexpectedly succeeded")
	}
	assertNoChildType(t, api, failedRetryID, "code.run")
	if n := countRequests(t, api, "code.run", failedRetryID); n != 0 {
		t.Fatalf("%d runtime-created attempts hang off the failed run; retries belong to the caller", n)
	}
	retryID, retried := ws.requestWithID(c0ChannelID, "code.run", runnerID, map[string]any{
		"program": `export async function run(){ return "second attempt" }`, "requires": []string{},
	})
	if retried["value"] != "second attempt" || retryID == failedRetryID {
		t.Fatalf("explicit retry=%v", retried)
	}

	fixedProgram := `export async function run({atoll,args}) { const out=await atoll.call({target:"echo",type:"echo.say",input:{text:args.text}}); return {text:out.text} }`
	fixedID := introduceClass(t, ws, registrar, "coderunner-fixed", "coderunner-fixed", "coderunner", deviceID, map[string]any{
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
	// A fixed-program member validates its own config and takes no input.
	fixedValidated := ws.request(c0ChannelID, "code.validate", fixedID, map[string]any{})
	fixedResolved, _ := fixedValidated["resolved"].(map[string]any)
	fixedEcho, _ := fixedResolved["echo"].(map[string]any)
	if fixedValidated["ok"] != true || fixedEcho["actor"] != echoID {
		t.Fatalf("fixed validate=%v", fixedValidated)
	}
	_, fixedOverride, fixedOverrideErr := ws.tryRequest(c0ChannelID, "code.validate", fixedID, map[string]any{"requires": []string{"echo"}})
	if fixedOverrideErr == nil || fixedOverride["error_code"] != "invalid_input" {
		t.Fatalf("fixed mode accepted a requires override in validate: %v err=%v", fixedOverride, fixedOverrideErr)
	}

	// A second runtime, same contract: the Python MCP client runs a Python
	// program against the same channel, and the ledger looks identical.
	if _, err := exec.LookPath("python3"); err == nil {
		pythonRuntime, err := filepath.Abs(filepath.Join("..", "drivers", "tools", "coderunner", "runtimes", "python", "atoll_runtime.py"))
		if err != nil {
			t.Fatal(err)
		}
		pythonID := introduceClass(t, ws, registrar, "coderunner-python", "coderunner-python", "coderunner", deviceID, map[string]any{
			"runtime": map[string]any{"command": "python3", "args": []string{pythonRuntime}, "suffix": ".py"},
		})
		waitActorPresence(t, ws, pythonID, true, daemon, daemonLog)
		pythonProgram := "async def run(atoll, args):\n" +
			"    out = await atoll.call(target='echo', type='echo.say', input={'text': args['text']})\n" +
			"    print('py says', out['text'])\n" +
			"    return {'text': out['text'], 'lang': 'python'}\n"
		pythonRunID, python := ws.requestWithID(c0ChannelID, "code.run", pythonID, map[string]any{
			"program": pythonProgram, "requires": []string{"echo"}, "args": map[string]any{"text": "snake"},
		})
		pythonValue, _ := python["value"].(map[string]any)
		if pythonValue["text"] != "snake" || pythonValue["lang"] != "python" {
			t.Fatalf("python result=%v", python)
		}
		pythonLogs, _ := python["logs"].([]any)
		var sawPrint bool
		for _, entry := range pythonLogs {
			row, _ := entry.(map[string]any)
			if row["stream"] == "stdout" && row["text"] == "py says snake" {
				sawPrint = true
			}
		}
		if !sawPrint {
			t.Fatalf("python print did not reach logs: %v", python["logs"])
		}
		assertCoderunnerCalls(t, api, pythonRunID, pythonID, echoID, rootID, 1, true)
	} else {
		t.Log("python3 unavailable; second-runtime scenario skipped")
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

// deviceID is named rather than left to the channel's default: c0 always has
// the node's local device attached, so an unnamed seat lands there and never on
// the daemon this test started.
func introduceClass(t *testing.T, ws *wsClient, registrar, declID, name, class, deviceID string, config map[string]any) string {
	t.Helper()
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": declID, "name": name, "class": class, "config": config, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": declID, "desired_host": deviceID})
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

// submitWithExpiry is submit with a declared request deadline (expires_at_ms).
func (c *wsClient) submitWithExpiry(channelID, msgType, audience string, payload any, expiresAtMs int64) string {
	c.t.Helper()
	wireRef++
	ref := fmt.Sprintf("submit-%d", wireRef)
	body := map[string]any{
		"channel_id": channelID, "msg_type": msgType, "kind": "request", "visibility": "public",
		"payload": payload, "audience": []string{audience}, "expires_at_ms": expiresAtMs,
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

// awaitTerminal returns the terminal payload of request id.
func (c *wsClient) awaitTerminal(id string, timeout time.Duration) map[string]any {
	c.t.Helper()
	terminal := c.awaitEnvelope(func(envelope map[string]any) bool {
		if envelope["kind"] != "response" || envelope["parent_id"] != id {
			return false
		}
		body, _ := envelope["payload"].(map[string]any)
		return body["status"] == "completed" || body["status"] == "failed"
	}, timeout)
	body, _ := terminal["payload"].(map[string]any)
	return body
}

// countTerminals replays the ledger and counts terminal responses under id.
func countTerminals(t *testing.T, api *apiClient, id string) int {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	count := 0
	for {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "response" || envelope["parent_id"] != id {
				continue
			}
			body, _ := envelope["payload"].(map[string]any)
			if body["status"] == "completed" || body["status"] == "failed" {
				count++
			}
		case <-timer.C:
			return count
		}
	}
}

// countRequests replays the ledger and counts requests of msgType whose
// correlation (errand) is rootID — the shape a runtime-created retry would have.
func countRequests(t *testing.T, api *apiClient, msgType, rootID string) int {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	count := 0
	for {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "request" || envelope["type"] != msgType {
				continue
			}
			if envelope["id"] != rootID && (envelope["correlation_id"] == rootID || envelope["parent_id"] == rootID) {
				count++
			}
		case <-timer.C:
			return count
		}
	}
}

// dumpUnder renders every ledger row whose parent is id, for failure output.
func dumpUnder(t *testing.T, api *apiClient, id string) string {
	t.Helper()
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	var rows []string
	for {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["parent_id"] != id {
				continue
			}
			sender, _ := envelope["sender"].(map[string]any)
			payload, _ := json.Marshal(envelope["payload"])
			rows = append(rows, fmt.Sprintf("%v %v from %v: %s", envelope["kind"], envelope["type"], sender["id"], payload))
		case <-timer.C:
			return strings.Join(rows, "\n")
		}
	}
}
