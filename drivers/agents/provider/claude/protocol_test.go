package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const (
	goldenU = "11111111-1111-4111-8111-111111111111"
	goldenS = "22222222-2222-4222-8222-222222222222"
	goldenT = "44444444-4444-4444-8444-444444444444"
)

func readyHarness(t *testing.T, seed string) *harness {
	t.Helper()
	h := newHarness(t, seed)
	h.initializeSuccess()
	waitAs[driverproto.WorkerReady](h)
	return h
}

func TestOpenInitializesThenReadyAndSeedIsClientSession(t *testing.T) {
	h := newHarness(t, "")
	init := h.input()
	if requestSubtype(init) != "initialize" {
		t.Fatalf("initialize=%v", init)
	}
	request := init["request"].(map[string]any)
	if _, present := request["sdkMcpServers"]; present {
		t.Fatalf("failed SDK-hosted transport was still advertised: %v", request)
	}
	if !containsArgs(h.args, "--mcp-config", "/dev/fd/3") {
		t.Fatalf("loopback MCP config not inherited: args=%q", h.args)
	}
	id := requestID(init)
	line := goldenLines(t, "probeA.out.jsonl")[0]
	h.proc.emitRaw(t, rewriteGolden(t, line, nil, map[string]string{"init-1": id}))
	waitAs[driverproto.WorkerReady](h)
	seeds := eventsAs[driverproto.SeedUpdated](h.sink.snapshot())
	ready := eventsAs[driverproto.WorkerReady](h.sink.snapshot())
	if len(seeds) != 1 || len(ready) != 1 || string(seeds[0].Value) == "" {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
	if !containsArgs(h.args, "--session-id", string(seeds[0].Value)) {
		t.Fatalf("args=%q seed=%q", h.args, seeds[0].Value)
	}
	if strings.Contains(eventsAs[driverproto.Diagnostic](h.sink.snapshot())[0].Detail, "email") {
		t.Fatal("initialize diagnostic leaked account email field")
	}
}

func TestSDKMCPMessageUsesEntryTargetSnapshotAndNativeID(t *testing.T) {
	h, target, _ := activeHarness(t)
	port := &captureToolPort{seen: make(chan capturedInvocation, 1)}
	h.worker.host = workerHost{sink: h.sink, tools: port}
	raw := json.RawMessage(`{"subtype":"mcp_message","server_name":"atoll","message":{"jsonrpc":"2.0","id":"native-7","method":"tools/call","params":{"name":"call_actor","arguments":{"actor_id":"tool:x","type":"x.run"}}}}`)
	handler := h.worker.prepareServerRequest(h.worker.conn, "mcp_message", raw)
	h.worker.mu.Lock()
	h.worker.target = driverproto.WorkerTurnTarget{Attempt: target.Attempt + 1, Native: "new-turn"}
	h.worker.mu.Unlock()
	result, isError := handler()
	if isError {
		t.Fatalf("mcp handler error=%v", result)
	}
	seen := <-port.seen
	if seen.target != target || !strings.HasSuffix(string(seen.invocation.CallID), `:s:"native-7"`) {
		t.Fatalf("captured=%+v want target=%+v", seen, target)
	}
	encoded, _ := json.Marshal(result)
	if !strings.Contains(string(encoded), `mcp_response`) || !strings.Contains(string(encoded), `"isError":false`) {
		t.Fatalf("response=%s", encoded)
	}
}

type capturedInvocation struct {
	target     driverproto.WorkerTurnTarget
	invocation driverproto.ToolInvocation
}

type captureToolPort struct{ seen chan capturedInvocation }

func (p *captureToolPort) Catalog() []driverproto.ToolSpec { return testToolPort{}.Catalog() }
func (p *captureToolPort) Invoke(_ context.Context, target driverproto.WorkerTurnTarget, invocation driverproto.ToolInvocation) driverproto.ToolResult {
	p.seen <- capturedInvocation{target: target, invocation: invocation}
	return driverproto.ToolResult{Text: `{"ok":true}`}
}

func TestCanUseToolAllowsOnlyAtollServer(t *testing.T) {
	h := readyHarness(t, "")
	allow := h.worker.prepareServerRequest(h.worker.conn, "can_use_tool", json.RawMessage(`{"subtype":"can_use_tool","tool_name":"mcp__atoll__call_actor"}`))
	deny := h.worker.prepareServerRequest(h.worker.conn, "can_use_tool", json.RawMessage(`{"subtype":"can_use_tool","tool_name":"mcp__other__call_actor"}`))
	allowed, allowedErr := allow()
	denied, deniedErr := deny()
	if allowedErr || deniedErr || allowed.(map[string]any)["behavior"] != "allow" || denied.(map[string]any)["behavior"] != "deny" {
		t.Fatalf("allow=%v/%v deny=%v/%v", allowed, allowedErr, denied, deniedErr)
	}
}

func TestOpenWithSeedResumesAndDoesNotRepublishSeed(t *testing.T) {
	h := readyHarness(t, "existing-session")
	if !containsArgs(h.args, "--resume", "existing-session") {
		t.Fatalf("args=%q", h.args)
	}
	if got := eventsAs[driverproto.SeedUpdated](h.sink.snapshot()); len(got) != 0 {
		t.Fatalf("seed events=%#v", got)
	}
}

func TestOpenInitializeErrorRejectsOnce(t *testing.T) {
	h := newHarness(t, "")
	frame := h.input()
	h.proc.emit(t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "error", "request_id": requestID(frame), "error": "denied"}})
	got := waitAs[driverproto.OpenRejected](h)
	if got.Class != driverproto.FailureProvider {
		t.Fatalf("rejection=%+v", got)
	}
	h.proc.closeOutput()
	<-h.worker.conn.wire.pumpDone
	if events := eventsAs[driverproto.OpenRejected](h.sink.snapshot()); len(events) != 1 {
		t.Fatalf("rejections=%#v", events)
	}
	if events := eventsAs[driverproto.WorkerEnded](h.sink.snapshot()); len(events) != 0 {
		t.Fatalf("worker ended=%#v", events)
	}
}

func TestOpenEOFBeforeInitializeRejectsOnce(t *testing.T) {
	h := newHarness(t, "")
	_ = h.input()
	h.proc.closeOutput()
	got := waitAs[driverproto.OpenRejected](h)
	if got.Class != driverproto.FailureTransport {
		t.Fatalf("rejection=%+v", got)
	}
	if len(eventsAs[driverproto.WorkerEnded](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestStartTurnLifecycleFromStartedToResult(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(1, "ack")
	lines := goldenLines(t, "probeA.out.jsonl")
	emitGolden(t, h.proc, lines, 2, 13, map[string]string{goldenU: u}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.FinalText != "ack" || ended.Target.Native != driverproto.WorkerTurnRef(u) {
		t.Fatalf("ended=%+v", ended)
	}
	started := eventsAs[driverproto.TurnStarted](h.sink.snapshot())
	if len(started) != 1 || started[0].Target.Native != driverproto.WorkerTurnRef(u) {
		t.Fatalf("started=%#v", started)
	}
	if len(eventsAs[driverproto.Activity](h.sink.snapshot())) == 0 {
		t.Fatal("golden activity was not mapped")
	}
}

func TestResumeInvalidBeforeStartedRejectsSubmissionForRetry(t *testing.T) {
	h := readyHarness(t, "missing-session")
	_ = h.start(7, "hello")
	h.proc.emitRaw(t, goldenLines(t, "probeC.out.jsonl")[0])
	got := waitAs[driverproto.SubmissionRejected](h)
	if got.Attempt != 7 || got.Class != driverproto.FailureResumeInvalid || got.Disposition != driverproto.RetireWorker {
		t.Fatalf("rejection=%+v", got)
	}
	h.proc.reportExit(errors.New("exit status 1"))
	h.proc.closeOutput()
	if len(eventsAs[driverproto.WorkerEnded](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestPreStartResultForOurUUIDRejectsProvider(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(2, "hello")
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true, "errors": []string{"boom"}, "user_message_uuid": u})
	got := waitAs[driverproto.SubmissionRejected](h)
	if got.Attempt != 2 || got.Class != driverproto.FailureProvider || got.Disposition != driverproto.RetireWorker {
		t.Fatalf("rejection=%+v", got)
	}
}

func TestPreStartUnsolicitedResultIsIsolatedAndTurnStillStarts(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(3, "hello")
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "stray"})
	warn := waitAs[driverproto.Diagnostic](h)
	if warn.Code != "unsolicited_cycle" || warn.Level != driverproto.DiagnosticWarn {
		t.Fatalf("diagnostic=%+v", warn)
	}
	if got := eventsAs[driverproto.SubmissionRejected](h.sink.snapshot()); len(got) != 0 {
		t.Fatalf("rejections=%#v", got)
	}
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started"})
	started := waitAs[driverproto.TurnStarted](h)
	if started.Target.Native != driverproto.WorkerTurnRef(u) {
		t.Fatalf("started=%+v", started)
	}
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "ack", "user_message_uuid": u})
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.FinalText != "ack" || ended.Target != started.Target {
		t.Fatalf("ended=%+v", ended)
	}
	var unsolicitedWarns int
	for _, diagnostic := range eventsAs[driverproto.Diagnostic](h.sink.snapshot()) {
		if diagnostic.Code == "unsolicited_cycle" && diagnostic.Level == driverproto.DiagnosticWarn {
			unsolicitedWarns++
		}
	}
	if unsolicitedWarns != 1 {
		t.Fatalf("unsolicited warn count=%d events=%#v", unsolicitedWarns, h.sink.snapshot())
	}
}

func TestEOFAfterStartedEndsWorkerTransport(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(3, "hello")
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started"})
	waitAs[driverproto.TurnStarted](h)
	h.proc.closeOutput()
	got := waitAs[driverproto.WorkerEnded](h)
	if got.Cause != driverproto.WorkerTransportEnded {
		t.Fatalf("ended=%+v", got)
	}
}

func TestNonZeroExitEndsWorkerCrash(t *testing.T) {
	h := readyHarness(t, "")
	h.proc.reportExit(errors.New("exit status 17"))
	h.proc.closeOutput()
	got := waitAs[driverproto.WorkerEnded](h)
	if got.Cause != driverproto.WorkerCrash {
		t.Fatalf("ended=%+v", got)
	}
	if len(eventsAs[driverproto.WorkerEnded](h.sink.snapshot())) != 1 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestResultFailedMapsToTurnFailed(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(4, "hello")
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started"})
	waitAs[driverproto.TurnStarted](h)
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true, "errors": []string{"first", "second"}, "user_message_uuid": u})
	got := waitAs[driverproto.TurnEnded](h)
	if got.Status != driverproto.TurnFailed || got.ErrorDetail != "first; second" {
		t.Fatalf("ended=%+v", got)
	}
}

func TestSteerIsAcceptedOnQueuedAndMergesIntoActiveTurn(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(10, "count")
	lines := goldenLines(t, "probeD.out.jsonl")
	emitGolden(t, h.proc, lines, 1, 48, map[string]string{goldenU: u}, nil)
	started := waitAs[driverproto.TurnStarted](h)
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 21, Kind: driverproto.ControlSteer, Target: started.Target, Message: &driverproto.DriverMessage{Text: "include BANANA"}})
	s := frameUUID(h.input())
	emitGolden(t, h.proc, lines, 49, 49, map[string]string{goldenU: u, goldenS: s}, nil)
	outcome := waitAs[driverproto.ControlOutcome](h)
	if outcome.Verdict != driverproto.ControlAccepted {
		t.Fatalf("outcome=%+v", outcome)
	}
	emitGolden(t, h.proc, lines, 50, 76, map[string]string{goldenU: u, goldenS: s}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || !strings.Contains(ended.FinalText, "BANANA") || ended.Target.Native != driverproto.WorkerTurnRef(u) {
		t.Fatalf("ended=%+v", ended)
	}
	if got := eventsAs[driverproto.ControlOutcome](h.sink.snapshot()); len(got) != 1 {
		t.Fatalf("control outcomes=%#v", got)
	}
	if got := eventsAs[driverproto.TurnEnded](h.sink.snapshot()); len(got) != 1 {
		t.Fatalf("turn ends=%#v", got)
	}
}

func TestSteerCancelledBeforeQueuedIsRejected(t *testing.T) {
	h, target, _ := activeHarness(t)
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 22, Kind: driverproto.ControlSteer, Target: target, Message: &driverproto.DriverMessage{Text: "later"}})
	s := frameUUID(h.input())
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": s, "state": "cancelled"})
	got := waitAs[driverproto.ControlOutcome](h)
	if got.Verdict != driverproto.ControlRejected || !strings.Contains(got.Detail, "before queued") {
		t.Fatalf("outcome=%+v", got)
	}
}

func TestSteerNotStartedWhenTurnEndsIsCancelledOnce(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(11, "count")
	lines := goldenLines(t, "probeD.out.jsonl")
	emitGolden(t, h.proc, lines, 1, 48, map[string]string{goldenU: u}, nil)
	target := waitAs[driverproto.TurnStarted](h).Target
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 23, Kind: driverproto.ControlSteer, Target: target, Message: &driverproto.DriverMessage{Text: "banana"}})
	s := frameUUID(h.input())
	emitGolden(t, h.proc, lines, 49, 49, map[string]string{goldenS: s}, nil)
	waitAs[driverproto.ControlOutcome](h)
	emitGolden(t, h.proc, lines, 75, 75, map[string]string{goldenU: u}, nil)
	waitAs[driverproto.TurnEnded](h)
	cancel := h.input()
	if requestSubtype(cancel) != "interrupt" || !cancel["request"].(map[string]any)["cancel_queued"].(bool) {
		t.Fatalf("cancel frame=%v", cancel)
	}
	// Defensive (not observed in probeD): if Claude starts the queued command
	// after cancellation, the entire unsolicited cycle stays isolated.
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": s, "state": "started"})
	h.proc.emit(t, map[string]any{"type": "assistant", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "late"}}}, "parent_tool_use_id": nil})
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "late", "user_message_uuid": s})
	waitAs[driverproto.Diagnostic](h)
	if len(eventsAs[driverproto.ControlOutcome](h.sink.snapshot())) != 1 || len(eventsAs[driverproto.TurnEnded](h.sink.snapshot())) != 1 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestSteerNotQueuedWhenTurnEndsIsTargetGone(t *testing.T) {
	h, target, u := activeHarness(t)
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 24, Kind: driverproto.ControlSteer, Target: target, Message: &driverproto.DriverMessage{Text: "later"}})
	_ = h.input()
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "done", "user_message_uuid": u})
	got := waitAs[driverproto.ControlOutcome](h)
	if got.Verdict != driverproto.ControlTargetGone {
		t.Fatalf("outcome=%+v", got)
	}
	if cancel := h.input(); requestSubtype(cancel) != "interrupt" {
		t.Fatalf("cancel=%v", cancel)
	}
	waitAs[driverproto.TurnEnded](h)
}

func TestInterruptCancelsQueuedAndEndsTurnInterrupted(t *testing.T) {
	h := readyHarness(t, "")
	u := h.start(12, "count")
	lines := goldenLines(t, "probeE.out.jsonl")
	emitGolden(t, h.proc, lines, 1, 31, map[string]string{goldenU: u}, nil)
	target := waitAs[driverproto.TurnStarted](h).Target
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 25, Kind: driverproto.ControlSteer, Target: target, Message: &driverproto.DriverMessage{Text: "queued"}})
	s := frameUUID(h.input())
	emitGolden(t, h.proc, lines, 32, 32, map[string]string{goldenS: s}, nil)
	steer := waitAs[driverproto.ControlOutcome](h)
	if steer.Verdict != driverproto.ControlAccepted {
		t.Fatalf("steer=%+v", steer)
	}
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 26, Kind: driverproto.ControlInterrupt, Target: target})
	interrupt := h.input()
	interruptID := requestID(interrupt)
	emitGolden(t, h.proc, lines, 33, 54, map[string]string{goldenU: u, goldenS: s}, map[string]string{"int-1": interruptID})
	accepted := waitAs[driverproto.ControlOutcome](h)
	if accepted.Action != 26 || accepted.Verdict != driverproto.ControlAccepted {
		t.Fatalf("interrupt=%+v", accepted)
	}
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnInterrupted || ended.Target != target {
		t.Fatalf("ended=%+v", ended)
	}
	third := h.start(13, "what was last?")
	emitGolden(t, h.proc, lines, 55, 66, map[string]string{goldenT: third}, nil)
	next := waitAs[driverproto.TurnEnded](h)
	if next.Status != driverproto.TurnOK || next.FinalText != "STEP_11" || next.Target.Native != driverproto.WorkerTurnRef(third) {
		t.Fatalf("next=%+v", next)
	}
	if len(eventsAs[driverproto.ControlOutcome](h.sink.snapshot())) != 2 {
		t.Fatalf("outcomes=%#v", eventsAs[driverproto.ControlOutcome](h.sink.snapshot()))
	}
}

// Defensive: Claude Code 2.1.233 probes did not emit an interrupt error.
func TestInterruptErrorClearsInflightAndAbortedResultWithoutUUIDIsIsolated(t *testing.T) {
	h, target, _ := activeHarness(t)
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 27, Kind: driverproto.ControlInterrupt, Target: target})
	request := h.input()
	h.proc.emit(t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "error", "request_id": requestID(request), "error": "cannot interrupt"}})
	got := waitAs[driverproto.ControlOutcome](h)
	if got.Verdict != driverproto.ControlRejected {
		t.Fatalf("outcome=%+v", got)
	}
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true, "terminal_reason": "aborted_streaming"})
	warn := waitAs[driverproto.Diagnostic](h)
	if warn.Code != "unsolicited_cycle" || warn.Level != driverproto.DiagnosticWarn {
		t.Fatalf("diagnostic=%+v", warn)
	}
	if len(eventsAs[driverproto.TurnEnded](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

// Defensive: an unmatched request id was not observed in the golden probes.
func TestInterruptUnmatchedRequestIDIsIgnored(t *testing.T) {
	h, target, _ := activeHarness(t)
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 28, Kind: driverproto.ControlInterrupt, Target: target})
	_ = h.input()
	h.proc.emit(t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": "wrong-id", "response": map[string]any{}}})
	diagnostic := waitAs[driverproto.Diagnostic](h)
	if diagnostic.Code != "unmatched_control_response" {
		t.Fatalf("diagnostic=%+v", diagnostic)
	}
	if len(eventsAs[driverproto.ControlOutcome](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestValidResumeContinuesConversation(t *testing.T) {
	h := readyHarness(t, "existing-session")
	u := h.start(14, "password?")
	lines := goldenLines(t, "probeB.out.jsonl")
	emitGolden(t, h.proc, lines, 1, 11, map[string]string{goldenS: u}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.FinalText != "PINEAPPLE" {
		t.Fatalf("ended=%+v", ended)
	}
}

func TestCapabilityMissingIsProtocolFault(t *testing.T) {
	h, _, _ := activeHarness(t)
	h.proc.emit(t, map[string]any{"type": "system", "subtype": "init", "capabilities": []string{"msg_lifecycle_v1"}, "claude_code_version": "2.1.233"})
	ended := waitAs[driverproto.WorkerEnded](h)
	if ended.Cause != driverproto.WorkerProtocolFault {
		t.Fatalf("ended=%+v", ended)
	}
	diagnostics := eventsAs[driverproto.Diagnostic](h.sink.snapshot())
	found := false
	for _, diagnostic := range diagnostics {
		found = found || diagnostic.Code == "capability_missing" && diagnostic.Level == driverproto.DiagnosticError
	}
	if !found || len(eventsAs[driverproto.WorkerEnded](h.sink.snapshot())) != 1 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestSubagentFramesAndNoiseAreDropped(t *testing.T) {
	h, _, _ := activeHarness(t)
	h.proc.emit(t, map[string]any{
		"type": "assistant", "parent_tool_use_id": "parent",
		"message": map[string]any{"content": []any{
			map[string]any{"type": "tool_use", "id": "nested", "name": "Task", "input": map[string]any{}},
		}},
	})
	h.proc.emit(t, map[string]any{"type": "rate_limit_event"})
	diagnostic := waitAs[driverproto.Diagnostic](h)
	if diagnostic.Code != "noise_frame" {
		t.Fatalf("diagnostic=%+v", diagnostic)
	}
	if len(eventsAs[driverproto.Tool](h.sink.snapshot())) != 0 || len(eventsAs[driverproto.Activity](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

func TestUnsolicitedCycleIsIsolated(t *testing.T) {
	h := readyHarness(t, "")
	h.proc.emit(t, map[string]any{"type": "assistant", "parent_tool_use_id": nil, "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "background"}}}})
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "background", "user_message_uuid": "unknown"})
	warn := waitAs[driverproto.Diagnostic](h)
	if warn.Code != "unsolicited_cycle" || warn.Level != driverproto.DiagnosticWarn {
		t.Fatalf("diagnostic=%+v", warn)
	}
	waitAs[driverproto.Diagnostic](h)
	if len(eventsAs[driverproto.TurnEnded](h.sink.snapshot())) != 0 {
		t.Fatalf("events=%#v", h.sink.snapshot())
	}
}

// Defensive: reverse control_request was not observed in the 2.1.233 probes.
func TestServerRequestsAreAnswered(t *testing.T) {
	h := readyHarness(t, "")
	h.proc.emit(t, map[string]any{"type": "control_request", "request_id": "server-1", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "Bash"}})
	response := h.input()
	body := response["response"].(map[string]any)
	result := body["response"].(map[string]any)
	if response["type"] != "control_response" || body["subtype"] != "success" || body["request_id"] != "server-1" || result["behavior"] != "deny" {
		t.Fatalf("response=%v", response)
	}
}

func TestOversizeLineEndsWorker(t *testing.T) {
	h := readyHarness(t, "")
	raw := append(bytes.Repeat([]byte{'x'}, maxLineBytes+1), '\n')
	go func() { _, _ = h.proc.out.Write(raw) }()
	ended := waitAs[driverproto.WorkerEnded](h)
	if ended.Cause != driverproto.WorkerTransportEnded || !strings.Contains(ended.Detail, "8 MiB") {
		t.Fatalf("ended=%+v", ended)
	}
}

func TestRetireSilencesLateFrames(t *testing.T) {
	h, target, u := activeHarness(t)
	c := h.worker.conn
	before := len(h.sink.snapshot())
	h.worker.Retire()
	<-h.worker.Reaped()
	h.worker.onLifecycle(c, u, "completed")
	h.worker.onFrame(c, "assistant", "", json.RawMessage(`{"type":"assistant","parent_tool_use_id":null,"message":{"content":[{"type":"text","text":"late"}]}}`))
	h.worker.Open(context.Background(), driverproto.OpenRequest{})
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 99, Messages: []driverproto.DriverMessage{{Text: "late"}}})
	h.worker.Control(context.Background(), driverproto.ControlRequest{Action: 99, Kind: driverproto.ControlInterrupt, Target: target})
	if after := len(h.sink.snapshot()); after != before {
		t.Fatalf("late events=%#v", h.sink.snapshot()[before:])
	}
}

func activeHarness(t *testing.T) (*harness, driverproto.WorkerTurnTarget, string) {
	t.Helper()
	h := readyHarness(t, "")
	u := h.start(1, "hello")
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started"})
	target := waitAs[driverproto.TurnStarted](h).Target
	return h, target, u
}
