package claude

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestOpenSpawnArgsCarryModelAndEffort(t *testing.T) {
	got := spawnArgs(Config{}, "session", false, driverproto.TurnOptions{Model: "claude-test", Effort: "high"})
	if !containsArgs(got, "--model", "claude-test") || !containsArgs(got, "--effort", "high") {
		t.Fatalf("args=%q", got)
	}
}

func startClaudeCommand(t *testing.T, h *harness, attempt driverproto.AttemptToken, kind driverproto.TurnKind, options driverproto.TurnOptions) string {
	t.Helper()
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: attempt, Kind: kind, Options: options})
	frame := h.input()
	if frame["type"] != "user" {
		t.Fatalf("command frame=%v", frame)
	}
	return frameUUID(frame)
}

func TestCompactTurnEndsOnMatchingResultAndReportsMetadata(t *testing.T) {
	h := readyHarness(t, "")
	u := startClaudeCommand(t, h, 31, driverproto.TurnCompact, driverproto.TurnOptions{})
	lines := goldenLines(t, "probeG.out.jsonl")
	emitGolden(t, h.proc, lines, 13, 21, map[string]string{goldenS: u}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.Target.Native != driverproto.WorkerTurnRef(u) || ended.Usage.ContextTokens != 4177 {
		t.Fatalf("ended=%+v", ended)
	}
	diagnostics := eventsAs[driverproto.Diagnostic](h.sink.snapshot())
	found := false
	for _, diagnostic := range diagnostics {
		found = found || diagnostic.Level == driverproto.DiagnosticInfo && diagnostic.Code == "compact" && strings.Contains(diagnostic.Detail, `"pre_tokens":48915`) && strings.Contains(diagnostic.Detail, `"post_tokens":4177`) && strings.Contains(diagnostic.Detail, `"duration_ms":59134`)
	}
	if !found {
		t.Fatalf("compact diagnostics=%#v", diagnostics)
	}
}

func TestCompactFailureMapsToTurnFailed(t *testing.T) {
	h := readyHarness(t, "")
	h.worker.mu.Lock()
	h.worker.usage.ContextTokens = 25811
	h.worker.mu.Unlock()
	u := startClaudeCommand(t, h, 32, driverproto.TurnCompact, driverproto.TurnOptions{})
	lines := goldenLines(t, "probeF.out.jsonl")
	emitGolden(t, h.proc, lines, 13, 19, map[string]string{goldenS: u}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnFailed || ended.ErrorDetail != "Not enough messages to compact." || ended.Usage.ContextTokens != 25811 {
		t.Fatalf("ended=%+v", ended)
	}
}

func TestNewTurnUsesClearAndPersistsReplacementSession(t *testing.T) {
	h := readyHarness(t, "old-session")
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 33, Kind: driverproto.TurnNew})
	frame := h.input()
	u := frameUUID(frame)
	content := frame["message"].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["text"] != "/clear" {
		t.Fatalf("new frame=%v", frame)
	}
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "queued", "session_id": "old-session"})
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started", "session_id": "old-session"})
	started := waitAs[driverproto.TurnStarted](h)
	h.proc.emit(t, map[string]any{"type": "conversation_reset", "new_conversation_id": "reset-not-the-session", "session_id": "old-session"})
	h.proc.emit(t, map[string]any{"type": "system", "subtype": "init", "session_id": "new-session", "claude_code_version": "test"})
	seed := waitAs[driverproto.SeedUpdated](h)
	if string(seed.Value) != "new-session" {
		t.Fatalf("seed=%q", seed.Value)
	}
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "num_turns": 0, "session_id": "new-session"})
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Target != started.Target || ended.Status != driverproto.TurnOK || ended.FinalText != "" || ended.Usage.ContextTokens != 0 {
		t.Fatalf("started=%+v ended=%+v", started, ended)
	}

	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 34, Messages: []driverproto.DriverMessage{{Text: "next"}}})
	next := h.input()
	if next["session_id"] != "new-session" {
		t.Fatalf("next frame session=%v", next)
	}
}

func TestSelectSendsSetModelThenEffortLine(t *testing.T) {
	h := readyHarness(t, "")
	h.worker.mu.Lock()
	h.worker.usage.ContextTokens = 25885
	h.worker.mu.Unlock()
	options := driverproto.TurnOptions{Model: "claude-sonnet-5", Effort: "high"}
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 33, Kind: driverproto.TurnSelect, Options: options})
	setModel := h.input()
	if requestSubtype(setModel) != "set_model" || setModel["request"].(map[string]any)["model"] != options.Model {
		t.Fatalf("set_model=%v", setModel)
	}
	h.proc.emit(t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": requestID(setModel)}})
	effort := h.input()
	u := frameUUID(effort)
	content := effort["message"].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["text"] != "/effort high" {
		t.Fatalf("effort=%v", effort)
	}
	lines := goldenLines(t, "probeH.out.jsonl")
	emitGolden(t, h.proc, lines, 17, 21, map[string]string{goldenS: u}, nil)
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.Usage.ContextTokens != 100 || ended.Usage.ContextWindow != 200000 || ended.Usage.Model != options.Model || ended.Usage.Effort != options.Effort {
		t.Fatalf("ended=%+v", ended)
	}
}

func TestTurnEndedUsageComesFromLatestResultIterationWithoutWaiting(t *testing.T) {
	h := readyHarness(t, "")
	h.proc.autoContext.Store(false)
	lines := goldenLines(t, "probeI.out.jsonl")
	turns := []struct {
		attempt driverproto.AttemptToken
		from    int
		through int
		golden  string
		want    int64
	}{
		{attempt: 34, from: 1, through: 15, golden: goldenU, want: 25811},
		{attempt: 35, from: 18, through: 27, golden: "21111111-1111-4111-8111-111111111111", want: 25885},
		{attempt: 36, from: 30, through: 38, golden: "31111111-1111-4111-8111-111111111111", want: 25941},
	}
	for _, turn := range turns {
		u := h.start(turn.attempt, "next")
		started := time.Now()
		emitGolden(t, h.proc, lines, turn.from, turn.through, map[string]string{turn.golden: u}, nil)
		ended := waitAs[driverproto.TurnEnded](h)
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("TurnEnded waited for a control response: %s", elapsed)
		}
		if ended.Usage.ContextTokens != turn.want || ended.Usage.ContextWindow != 200000 || ended.Usage.Model != "claude-haiku-4-5-20251001" {
			t.Fatalf("attempt %d usage=%+v", turn.attempt, ended.Usage)
		}
	}
	h.noInputFor(100 * time.Millisecond)
}

func TestOpenFetchesContextWindowOnce(t *testing.T) {
	h := newHarness(t, "")
	h.proc.autoContext.Store(false)
	h.initializeSuccess()
	request := h.input()
	if requestSubtype(request) != "get_context_usage" {
		t.Fatalf("context request=%v", request)
	}
	lines := goldenLines(t, "probeH.out.jsonl")
	h.proc.emitRaw(t, rewriteGolden(t, lines[13], nil, map[string]string{"ctx-1": requestID(request)}))
	waitAs[driverproto.WorkerReady](h)
	h.worker.mu.Lock()
	tokens, window := h.worker.usage.ContextTokens, h.worker.usage.ContextWindow
	h.worker.mu.Unlock()
	if tokens != 25787 || window != 200000 {
		t.Fatalf("context usage=%d/%d", tokens, window)
	}
	h.noInputFor(100 * time.Millisecond)
}

func TestSelectRefreshesContextWindowAsync(t *testing.T) {
	h := readyHarness(t, "")
	h.proc.autoContext.Store(false)
	options := driverproto.TurnOptions{Model: "claude-sonnet-5"}
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 37, Kind: driverproto.TurnSelect, Options: options})
	setModel := h.input()
	if requestSubtype(setModel) != "set_model" {
		t.Fatalf("set_model=%v", setModel)
	}
	h.proc.emit(t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": requestID(setModel)}})
	ended := waitAs[driverproto.TurnEnded](h)
	if ended.Status != driverproto.TurnOK || ended.Usage.ContextWindow != 200000 || ended.Usage.Model != options.Model {
		t.Fatalf("select ended=%+v", ended)
	}
	request := h.input()
	if requestSubtype(request) != "get_context_usage" {
		t.Fatalf("context refresh=%v", request)
	}
	lines := goldenLines(t, "probeH.out.jsonl")
	h.proc.emitRaw(t, rewriteGolden(t, lines[28], nil, map[string]string{"ctx-2": requestID(request)}))

	deadline := time.Now().Add(time.Second)
	for {
		h.worker.mu.Lock()
		window := h.worker.usage.ContextWindow
		h.worker.mu.Unlock()
		if window == 967000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("context window was not refreshed; got %d", window)
		}
		time.Sleep(time.Millisecond)
	}

	u := h.start(38, "after select")
	h.proc.emit(t, map[string]any{"type": "command_lifecycle", "command_uuid": u, "state": "started"})
	waitAs[driverproto.TurnStarted](h)
	h.proc.emit(t, map[string]any{"type": "result", "subtype": "success", "result": "done", "user_message_uuid": u, "usage": map[string]any{"input_tokens": 1, "cache_creation_input_tokens": 2, "cache_read_input_tokens": 3, "iterations": []map[string]any{{"input_tokens": 1, "cache_creation_input_tokens": 2, "cache_read_input_tokens": 3}}}})
	next := waitAs[driverproto.TurnEnded](h)
	if next.Usage.ContextTokens != 6 || next.Usage.ContextWindow != 967000 || next.Usage.Model != options.Model {
		t.Fatalf("next usage=%+v", next.Usage)
	}
}

func TestLatestIterationContextTokensDoesNotUseAccumulatedBillingUsage(t *testing.T) {
	usage := resultUsage{
		InputTokens:         800000,
		CacheCreationTokens: 1200000,
		CacheReadTokens:     1800000,
		Iterations: []resultIteration{
			{InputTokens: 10, CacheCreationTokens: 300000, CacheReadTokens: 400000},
			{InputTokens: 10, CacheCreationTokens: 5000, CacheReadTokens: 745000},
		},
	}
	tokens, ok := latestIterationContextTokens(usage)
	if !ok || tokens != 750010 {
		t.Fatalf("latest context=%d ok=%v", tokens, ok)
	}
}
