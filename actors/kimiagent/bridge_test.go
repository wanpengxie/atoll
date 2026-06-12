package kimiagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/ActOS/actors/kimiagent"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const (
	testChannelID = channel.ID("ch-test")
	testActorID   = actor.ActorID("agent:T")
)

// recordingWriter is a concurrency-safe harness.Writer double.
type recordingWriter struct {
	mu      sync.Mutex
	written []message.Envelope
	wErr    error // optional injected write error
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.wErr != nil {
		return harness.WriteResult{}, w.wErr
	}
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) Written() []message.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]message.Envelope, len(w.written))
	copy(out, w.written)
	return out
}

// waitWritten polls until at least n envelopes landed or the timeout
// elapses; returns the snapshot either way.
func (w *recordingWriter) waitWritten(t *testing.T, n int, timeout time.Duration) []message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := w.Written()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// scriptedAgent is the kimiAgent test double — emits a canned wire
// sequence into the bridge's wire channel and returns the configured
// `runErr`. Used through the agentNew test hook.
type scriptedAgent struct {
	emitFn   func(ctx context.Context, input string) error
	runErr   error
	closeErr error
}

func (s *scriptedAgent) Run(ctx context.Context, input string) error {
	if s.emitFn != nil {
		if err := s.emitFn(ctx, input); err != nil {
			return err
		}
	}
	return s.runErr
}

func (s *scriptedAgent) Close() error { return s.closeErr }

func testConfig() kimiagent.Config {
	return kimiagent.Config{
		APIKey:       "k",
		Model:        "m",
		ProviderType: "anthropic",
	}
}

// newStartedBridge builds + starts a bridge with the given scripted agent.
// Cleanup stops it.
func newStartedBridge(t *testing.T, w *recordingWriter, sa kimiagent.Agent) *kimiagent.Bridge {
	t.Helper()
	b, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	kimiagent.SetAgentFactory(b, func(kimiagent.AgentConfig) (kimiagent.Agent, error) { return sa, nil })
	if err := b.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// triggerEnv returns a representative trigger envelope (a user request).
func triggerEnv(id string) message.Envelope {
	body, _ := json.Marshal(map[string]string{"text": "hi"})
	return message.Envelope{
		ID:            message.ID(id),
		ChannelID:     testChannelID,
		Type:          "human.text",
		Visibility:    message.VisibilityPublic,
		Sender:        message.Sender{Kind: actor.KindHuman, ID: "user-A"},
		Kind:          message.KindRequest,
		Payload:       body,
		Audience:      message.Audience{testActorID},
		CorrelationID: "corr-1",
	}
}

// TestChannelTools_ShellResolvesWhenBuiltBeforeStart pins the
// order-independence of the meta-tool surface. buildAgent installs the tools
// into the LLM loop DURING Start, before Start assigns b.shell — so a tool that
// captured the *Shell value at construction would freeze a nil shell and every
// meta call would return "tool not configured" forever. The post-Start
// ChannelToolsForTest rebuild masks that (it sees the live shell), so this test
// deliberately builds the tool instance BEFORE Start and executes it AFTER,
// asserting the lazy b.shellRef resolver reached the real shell.
func TestChannelTools_ShellResolvesWhenBuiltBeforeStart(t *testing.T) {
	w := &recordingWriter{}
	b, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	// Build the surface while b.shell is still nil (mirrors buildAgent).
	var listPending gokimitools.Tool
	for _, raw := range kimiagent.ChannelToolsForTest(b) {
		if raw.Name() == "list_pending" {
			listPending = raw.(gokimitools.Tool)
		}
	}
	if listPending == nil {
		t.Fatal("list_pending tool absent before Start")
	}
	// Start assigns the real shell; execute the PRE-built instance.
	kimiagent.SetAgentFactory(b, func(kimiagent.AgentConfig) (kimiagent.Agent, error) {
		return &scriptedAgent{}, nil
	})
	if err := b.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	res, err := listPending.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.IsError {
		t.Fatalf("pre-Start tool froze a nil shell — got error result %+v (want live shell)", res.Value)
	}
}

// pickToolByName looks up a channel tool by Name(). Returns nil when
// absent — caller fails the test.
func pickToolByName(b *kimiagent.Bridge, name string) gokimitools.Tool {
	for _, raw := range kimiagent.ChannelToolsForTest(b) {
		if raw.Name() == name {
			if tool, ok := raw.(gokimitools.Tool); ok {
				return tool
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Config / prompt
// ---------------------------------------------------------------------------

func TestNewConfigFromEnv_RequiresAPIKey(t *testing.T) {
	t.Setenv(kimiagent.EnvKeyAPIKey, "")
	t.Setenv(kimiagent.EnvKeyModel, "m")
	if _, err := kimiagent.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error for missing api key")
	}
}

func TestNewConfigFromEnv_RequiresModel(t *testing.T) {
	t.Setenv(kimiagent.EnvKeyAPIKey, "k")
	t.Setenv(kimiagent.EnvKeyModel, "")
	if _, err := kimiagent.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error for missing model")
	}
}

func TestNewConfigFromEnv_ReadsAllFields(t *testing.T) {
	t.Setenv(kimiagent.EnvKeyAPIKey, "k1")
	t.Setenv(kimiagent.EnvKeyBaseURL, "https://api.example.com/anthropic")
	t.Setenv(kimiagent.EnvKeyModel, "deepseek-v4")
	cfg, err := kimiagent.NewConfigFromEnv("sys-prompt")
	if err != nil {
		t.Fatalf("NewConfigFromEnv: %v", err)
	}
	if cfg.APIKey != "k1" || cfg.BaseURL != "https://api.example.com/anthropic" ||
		cfg.Model != "deepseek-v4" || cfg.ProviderType != "anthropic" ||
		cfg.SystemPrompt != "sys-prompt" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestBuildSystemPrompt_EmptyDomain(t *testing.T) {
	p := kimiagent.BuildSystemPrompt(kimiagent.Situation{Host: "server"}, "", "")
	if !strings.Contains(p, "coagent agent") {
		t.Fatalf("platform teaching missing: %q", p[:80])
	}
	if strings.Contains(p, "Channel template") {
		t.Fatal("empty domain must not add a template header")
	}
}

func TestBuildSystemPrompt_WithDomain(t *testing.T) {
	p := kimiagent.BuildSystemPrompt(kimiagent.Situation{Host: "daemon"}, "xhs-creator", "domain rules here")
	if !strings.Contains(p, "# Channel template: xhs-creator") {
		t.Fatal("template header missing")
	}
	if !strings.Contains(p, "domain rules here") {
		t.Fatal("domain prompt missing")
	}
}

// TestBuildSystemPrompt_SituationDrivesBehaviour pins the two-roles-one-
// skeleton design: facts in, behaviour out — and NO role labels anywhere.
func TestBuildSystemPrompt_SituationDrivesBehaviour(t *testing.T) {
	noWS := kimiagent.BuildSystemPrompt(kimiagent.Situation{Host: "server"}, "", "")
	if !strings.Contains(noWS, "NO private workspace") {
		t.Fatal("no-workspace fact missing")
	}
	if !strings.Contains(noWS, "guide them to attach") {
		t.Fatal("bootstrap-guide behaviour not derived from the no-workspace fact")
	}

	withWS := kimiagent.BuildSystemPrompt(kimiagent.Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: "/ws/ch-1"}, "", "")
	if !strings.Contains(withWS, "/ws/ch-1") {
		t.Fatal("workspace dir missing")
	}
	if !strings.Contains(withWS, "Persist durable working rules") {
		t.Fatal("principal discipline not derived from the workspace fact")
	}

	for _, p := range []string{noWS, withWS} {
		for _, label := range []string{"bootloader", "work agent", "role:"} {
			if strings.Contains(strings.ToLower(p), label) {
				t.Fatalf("prompt carries a role label %q — roles derive from facts, never labels", label)
			}
		}
	}
}

func TestBuildSystemPrompt_NoFrozenActorSnapshot(t *testing.T) {
	p := kimiagent.BuildSystemPrompt(kimiagent.Situation{}, "group", "d")
	for _, banned := range []string{"actor_id\":", "frozen", "COAGENT_CHANNEL_CONTEXT_JSON"} {
		if strings.Contains(p, banned) {
			t.Fatalf("prompt carries frozen snapshot marker %q", banned)
		}
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewBridge_Validations(t *testing.T) {
	w := &recordingWriter{}
	if _, err := kimiagent.NewBridge(kimiagent.Config{Model: "m"}, testActorID, testChannelID, w); err == nil {
		t.Fatal("missing api key: want error")
	}
	if _, err := kimiagent.NewBridge(kimiagent.Config{APIKey: "k"}, testActorID, testChannelID, w); err == nil {
		t.Fatal("missing model: want error")
	}
	if _, err := kimiagent.NewBridge(testConfig(), "", testChannelID, w); err == nil {
		t.Fatal("missing self: want error")
	}
	if _, err := kimiagent.NewBridge(testConfig(), testActorID, "", w); err == nil {
		t.Fatal("missing channel: want error")
	}
	if _, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, nil); err == nil {
		t.Fatal("nil writer: want error")
	}
}

// ---------------------------------------------------------------------------
// Turn machinery (Receive → private loop → emitted envelopes)
// ---------------------------------------------------------------------------

// scriptTextTurn emits deltas + a TurnEnd through the bridge's wire channel.
func scriptTextTurn(b **kimiagent.Bridge, text string) *scriptedAgent {
	return &scriptedAgent{emitFn: func(ctx context.Context, _ string) error {
		em := kimiagent.BridgeWireEmitter(*b)
		em.Emit(wire.TextDelta{Delta: text})
		em.Emit(wire.TurnEnd{StopReason: "end_turn"})
		return nil
	}}
}

func TestReceive_RequestRunsTurnAndEmitsSingleTerminal(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	b = newStartedBridge(t, w, scriptTextTurn(&b, "hello there"))

	env := triggerEnv("req-1")
	if err := b.Receive(context.Background(), &env); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	got := w.waitWritten(t, 1, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("want exactly 1 envelope, got %d: %+v", len(got), got)
	}
	out := got[0]
	if out.Type != "agent.text" || out.Visibility != message.VisibilityPublic {
		t.Fatalf("terminal shape: %+v", out)
	}
	if out.ParentID != "req-1" || out.CorrelationID != "corr-1" {
		t.Fatalf("threading: parent=%s corr=%s", out.ParentID, out.CorrelationID)
	}
	if out.Sender.ID != testActorID || out.Sender.Kind != actor.KindAgent {
		t.Fatalf("sender: %+v", out.Sender)
	}
	if len(out.Audience) != 1 || out.Audience[0] != "user-A" {
		t.Fatalf("reply audience: %+v", out.Audience)
	}
	var payload map[string]any
	_ = json.Unmarshal(out.Payload, &payload)
	if payload["text"] != "hello there" || payload["next_action"] != "done" {
		t.Fatalf("payload: %+v", payload)
	}
}

func TestReceive_ProgressPerToolStep(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	sa := &scriptedAgent{emitFn: func(ctx context.Context, _ string) error {
		em := kimiagent.BridgeWireEmitter(b)
		em.Emit(wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t1", Name: "call_actor", Arguments: map[string]any{"cmd": "ls"}}})
		em.Emit(wire.ToolCallResult{})
		em.Emit(wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t2", Name: "call_actor"}})
		em.Emit(wire.ToolCallResult{})
		em.Emit(wire.TextDelta{Delta: "final answer"})
		em.Emit(wire.TurnEnd{StopReason: "end_turn"})
		return nil
	}}
	b = newStartedBridge(t, w, sa)

	env := triggerEnv("req-p")
	_ = b.Receive(context.Background(), &env)

	got := w.waitWritten(t, 3, 2*time.Second)
	if len(got) != 3 {
		t.Fatalf("want 2 progress + 1 terminal, got %d", len(got))
	}
	for i := 0; i < 2; i++ {
		if got[i].Visibility != message.VisibilitySystem {
			t.Fatalf("progress %d visibility = %s", i, got[i].Visibility)
		}
		var p map[string]any
		_ = json.Unmarshal(got[i].Payload, &p)
		if p["step_index"] != float64(i+1) {
			t.Fatalf("progress %d step_index = %v", i, p["step_index"])
		}
	}
	if got[2].Visibility != message.VisibilityPublic {
		t.Fatalf("terminal visibility = %s", got[2].Visibility)
	}
}

func TestReceive_LLMErrorEmitsFailedTerminal(t *testing.T) {
	w := &recordingWriter{}
	llmErr := &kimierrors.LLMError{StatusCode: 429, Cause: errors.New("rate limited")}
	b := newStartedBridge(t, w, &scriptedAgent{runErr: llmErr})

	env := triggerEnv("req-err")
	_ = b.Receive(context.Background(), &env)

	got := w.waitWritten(t, 1, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("want 1 failed terminal, got %d", len(got))
	}
	var p map[string]any
	_ = json.Unmarshal(got[0].Payload, &p)
	if p["next_action"] != "failed" || p["reason"] != "llm_rate_limit" {
		t.Fatalf("payload: %+v", p)
	}
}

// ---------------------------------------------------------------------------
// call_actor tool: three-step call + fast-path window + author#2
// ---------------------------------------------------------------------------

// startToolCall invokes call_actor in a goroutine and returns the result chan.
func startToolCall(t *testing.T, b *kimiagent.Bridge, trigger message.Envelope, params map[string]any) <-chan types.ToolResult {
	t.Helper()
	tool := pickToolByName(b, "call_actor")
	if tool == nil {
		t.Fatal("call_actor tool absent")
	}
	raw, _ := json.Marshal(params)
	ctx := kimiagent.WithTurnContext(context.Background(), trigger)
	out := make(chan types.ToolResult, 1)
	go func() {
		res, _ := tool.Execute(ctx, raw)
		out <- res
	}()
	return out
}

func TestCallActor_EmitsRequestAndReturnsInlineResponse(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	b = newStartedBridge(t, w, &scriptedAgent{})

	trigger := triggerEnv("trig-1")
	resCh := startToolCall(t, b, trigger, map[string]any{
		"actor_id": "tool:xhs", "type": "xhs.publish", "payload": map[string]any{"title": "t"},
	})

	// The request envelope must land on the writer.
	reqs := w.waitWritten(t, 1, 2*time.Second)
	if len(reqs) != 1 {
		t.Fatalf("want request envelope, got %d", len(reqs))
	}
	req := reqs[0]
	if req.Kind != message.KindRequest || req.Type != "xhs.publish" {
		t.Fatalf("request shape: %+v", req)
	}
	if len(req.Audience) != 1 || req.Audience[0] != "tool:xhs" {
		t.Fatalf("request audience: %+v", req.Audience)
	}
	if req.ParentID != "trig-1" {
		t.Fatalf("request parent: %s", req.ParentID)
	}
	if req.ExpiresAt == nil {
		t.Fatal("request carries no ExpiresAt (author#2 deadline)")
	}

	// Feed the final response through the mailbox (Receive), as the
	// substrate would.
	respPayload, _ := json.Marshal(map[string]any{"status": "completed", "note_id": "n1"})
	resp := message.Envelope{
		ID: "resp-1", ChannelID: testChannelID, Kind: message.KindResponse,
		Type: "xhs.publish.response", ParentID: req.ID,
		Sender:  message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: respPayload,
	}
	if err := b.Receive(context.Background(), &resp); err != nil {
		t.Fatalf("Receive response: %v", err)
	}

	select {
	case res := <-resCh:
		if res.IsError {
			t.Fatalf("tool errored: %+v", res.Value)
		}
		m, _ := res.Value.Value.(map[string]any)
		if m["note_id"] != "n1" {
			t.Fatalf("inline result: %+v", res.Value.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not return after final response")
	}
}

func TestCallActor_OverWindowReturnsAck(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	cfg := testConfig()
	cfg.FastPathWindow = 30 * time.Millisecond
	bb, err := kimiagent.NewBridge(cfg, testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	b = bb
	kimiagent.SetAgentFactory(b, func(kimiagent.AgentConfig) (kimiagent.Agent, error) { return &scriptedAgent{}, nil })
	if err := b.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	trigger := triggerEnv("trig-ack")
	resCh := startToolCall(t, b, trigger, map[string]any{
		"actor_id": "tool:slow", "type": "slow.op",
	})

	select {
	case res := <-resCh:
		m, _ := res.Value.Value.(map[string]any)
		if m["status"] != "accepted" {
			t.Fatalf("want ack, got %+v", res.Value.Value)
		}
		if m["request_id"] == "" {
			t.Fatal("ack missing request_id")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not degrade to ack")
	}
}

func TestCallActor_TerminalFailureReturnsErrorResult(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	b = newStartedBridge(t, w, &scriptedAgent{})

	trigger := triggerEnv("trig-f")
	resCh := startToolCall(t, b, trigger, map[string]any{
		"actor_id": "tool:xhs", "type": "xhs.publish",
	})
	reqs := w.waitWritten(t, 1, 2*time.Second)
	if len(reqs) == 0 {
		t.Fatal("no request emitted")
	}

	respPayload, _ := json.Marshal(map[string]any{
		"status": "failed", "error": string(message.TerminalReceiverUnavailable),
	})
	resp := message.Envelope{
		ID: "resp-f", ChannelID: testChannelID, Kind: message.KindResponse,
		ParentID: reqs[0].ID, Sender: message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: respPayload,
	}
	_ = b.Receive(context.Background(), &resp)

	select {
	case res := <-resCh:
		if !res.IsError {
			t.Fatalf("want error result, got %+v", res.Value.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not return")
	}
}

func TestCallActor_ProvisionalBeforeFinalDoesNotResolveEarly(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	b = newStartedBridge(t, w, &scriptedAgent{})

	trigger := triggerEnv("trig-prov")
	resCh := startToolCall(t, b, trigger, map[string]any{
		"actor_id": "tool:xhs", "type": "xhs.publish",
	})
	reqs := w.waitWritten(t, 1, 2*time.Second)
	if len(reqs) == 0 {
		t.Fatal("no request emitted")
	}

	// Provisional (non-final status) must not resolve the waiter.
	provPayload, _ := json.Marshal(map[string]any{"status": "processing"})
	prov := message.Envelope{
		ID: "resp-prov", ChannelID: testChannelID, Kind: message.KindResponse,
		ParentID: reqs[0].ID, Sender: message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: provPayload,
	}
	_ = b.Receive(context.Background(), &prov)

	select {
	case res := <-resCh:
		t.Fatalf("resolved on provisional: %+v", res.Value.Value)
	case <-time.After(50 * time.Millisecond):
	}

	finalPayload, _ := json.Marshal(map[string]any{"status": "completed", "ok": true})
	final := message.Envelope{
		ID: "resp-final", ChannelID: testChannelID, Kind: message.KindResponse,
		ParentID: reqs[0].ID, Sender: message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: finalPayload,
	}
	_ = b.Receive(context.Background(), &final)

	select {
	case res := <-resCh:
		if res.IsError {
			t.Fatalf("final errored: %+v", res.Value.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final did not resolve the waiter")
	}
}

// TestReceive_UnawaitedFinalBecomesNewTurn pins the async continuation: a
// final response nobody waits for feeds the LLM as a new turn.
func TestReceive_UnawaitedFinalBecomesNewTurn(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	var inputs []string
	var inputsMu sync.Mutex
	sa := &scriptedAgent{emitFn: func(ctx context.Context, input string) error {
		inputsMu.Lock()
		inputs = append(inputs, input)
		inputsMu.Unlock()
		em := kimiagent.BridgeWireEmitter(b)
		em.Emit(wire.TextDelta{Delta: "ok"})
		em.Emit(wire.TurnEnd{StopReason: "end_turn"})
		return nil
	}}
	b = newStartedBridge(t, w, sa)

	finalPayload, _ := json.Marshal(map[string]any{"status": "completed", "url": "https://x"})
	resp := message.Envelope{
		ID: "resp-async", ChannelID: testChannelID, Kind: message.KindResponse,
		Type: "xhs.publish.response", ParentID: "req-long-gone",
		Sender:  message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Payload: finalPayload,
	}
	_ = b.Receive(context.Background(), &resp)

	w.waitWritten(t, 1, 2*time.Second)
	inputsMu.Lock()
	defer inputsMu.Unlock()
	if len(inputs) != 1 {
		t.Fatalf("want 1 continuation turn, got %d", len(inputs))
	}
	if !strings.Contains(inputs[0], "tool:xhs") {
		t.Fatalf("continuation input missing sender label: %q", inputs[0])
	}
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

func TestEnvelopeIDUniqueWithinSameMillisecond(t *testing.T) {
	w := &recordingWriter{}
	b, err := kimiagent.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := kimiagent.EnvelopeIDForTest(b, 1234567890)
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
}

func TestClassifyLLMError_NetworkBuckets(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&kimierrors.LLMError{StatusCode: 429}, "llm_rate_limit"},
		{&kimierrors.LLMError{StatusCode: 401}, "llm_auth"},
		{&kimierrors.LLMError{StatusCode: 503}, "llm_server"},
		{&kimierrors.LLMError{StatusCode: 418}, "llm_unknown"},
		{&url.Error{Op: "Post", URL: "https://x", Err: errors.New("refused")}, "llm_network"},
		{context.DeadlineExceeded, "llm_network"},
		{fmt.Errorf("opaque"), "llm_unknown"},
	}
	for _, c := range cases {
		if got := kimiagent.ClassifyLLMError(c.err); got != c.want {
			t.Fatalf("classify(%v) = %s want %s", c.err, got, c.want)
		}
	}
}

func TestStop_TearsDownCleanly(t *testing.T) {
	w := &recordingWriter{}
	var b *kimiagent.Bridge
	b = newStartedBridge(t, w, scriptTextTurn(&b, "x"))

	env := triggerEnv("req-stop")
	_ = b.Receive(context.Background(), &env)
	w.waitWritten(t, 1, 2*time.Second)

	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Second Stop via cleanup would double-close; guard by re-newing in
	// cleanup-safe way: Stop again must not panic thanks to idempotence?
	// (cleanup calls Stop once more — the test passes if no panic.)
}
