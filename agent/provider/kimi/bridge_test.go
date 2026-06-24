package kimi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/ActOS/agent/provider/kimi"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const (
	testChannelID = channel.ID("ch-test")
	testActorID   = actor.ActorID("agent:T")
)

// recordingWriter is a concurrency-safe harness.Pen double. It records the
// envelope EXACTLY as the bridge emits it — it does NOT inject identity (a raw
// recorder, not a boundPen). Under sealed-pen the bridge leaves Sender/ChannelID
// zero; the real pen injects the welded identity at write time.
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

func testConfig() kimi.Config {
	return kimi.Config{
		APIKey:       "k",
		Model:        "m",
		ProviderType: "anthropic",
	}
}

// newStartedBridge builds + starts a bridge with the given scripted agent.
// Cleanup stops it.
func newStartedBridge(t *testing.T, w *recordingWriter, sa kimi.Agent) *kimi.Bridge {
	t.Helper()
	b, err := kimi.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	kimi.SetAgentFactory(b, func(kimi.AgentConfig) (kimi.Agent, error) { return sa, nil })
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
	b, err := kimi.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	// Build the surface while b.shell is still nil (mirrors buildAgent).
	var listPending gokimitools.Tool
	for _, raw := range kimi.ChannelToolsForTest(b) {
		if raw.Name() == "list_pending" {
			listPending = raw.(gokimitools.Tool)
		}
	}
	if listPending == nil {
		t.Fatal("list_pending tool absent before Start")
	}
	// Start assigns the real shell; execute the PRE-built instance.
	kimi.SetAgentFactory(b, func(kimi.AgentConfig) (kimi.Agent, error) {
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

// TestReceive_SelfAuthoredMessageNeverTurns pins the self-loop guard: an actor
// must never react to its OWN emissions. A self-authored event (agent.text) and
// a self-authored unconsumed final response (a metatool timeout terminal for the
// agent's own outbound request, sender==self) must NOT enqueue a turn — otherwise
// replyAudience (reply to the trigger's sender == self) spins an infinite loop.
// A non-self message still triggers a turn (the guard is precise, not a freeze).
func TestReceive_SelfAuthoredMessageNeverTurns(t *testing.T) {
	w := &recordingWriter{}
	var runs atomic.Int32
	sa := &scriptedAgent{emitFn: func(context.Context, string) error {
		runs.Add(1)
		return nil
	}}
	b := newStartedBridge(t, w, sa)
	ctx := context.Background()

	// Self-authored event (the agent's own agent.text fed back to it).
	selfEvent := &message.Envelope{
		ID: "self-ev", ChannelID: testChannelID,
		Sender: message.Sender{Kind: actor.KindAgent, ID: testActorID},
		Kind:   message.KindEvent, Type: "agent.text",
		Payload: json.RawMessage(`{"text":"ack"}`), Audience: message.Audience{testActorID},
	}
	if err := b.Receive(ctx, selfEvent); err != nil {
		t.Fatalf("Receive self event: %v", err)
	}

	// Self-authored unconsumed final response (own outbound-request timeout
	// terminal): still delivered to the shell, but never a turn.
	selfResp := &message.Envelope{
		ID: "self-resp", ChannelID: testChannelID, ParentID: "some-req",
		Sender: message.Sender{Kind: actor.KindAgent, ID: testActorID},
		Kind:   message.KindResponse, Type: "actor.list",
		Payload:  json.RawMessage(`{"status":"failed","reason":"unanswered_timeout"}`),
		Audience: message.Audience{testActorID},
	}
	if err := b.Receive(ctx, selfResp); err != nil {
		t.Fatalf("Receive self response: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if n := runs.Load(); n != 0 {
		t.Fatalf("self-authored messages triggered %d turn(s); want 0 (self-loop)", n)
	}

	// Sanity: a NON-self request DOES still drive a turn.
	ext := triggerEnv("ext-req")
	if err := b.Receive(ctx, &ext); err != nil {
		t.Fatalf("Receive external req: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if runs.Load() == 0 {
		t.Fatal("a non-self request did not drive a turn — guard is too broad")
	}
}

// pickToolByName looks up a channel tool by Name(). Returns nil when
// absent — caller fails the test.
func pickToolByName(b *kimi.Bridge, name string) gokimitools.Tool {
	for _, raw := range kimi.ChannelToolsForTest(b) {
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
	t.Setenv(kimi.EnvKeyAPIKey, "")
	t.Setenv(kimi.EnvKeyModel, "m")
	if _, err := kimi.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error for missing api key")
	}
}

func TestNewConfigFromEnv_RequiresModel(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "k")
	t.Setenv(kimi.EnvKeyModel, "")
	if _, err := kimi.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error for missing model")
	}
}

func TestNewConfigFromEnv_ReadsAllFields(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "k1")
	t.Setenv(kimi.EnvKeyBaseURL, "https://api.example.com/anthropic")
	t.Setenv(kimi.EnvKeyModel, "deepseek-v4")
	cfg, err := kimi.NewConfigFromEnv("sys-prompt")
	if err != nil {
		t.Fatalf("NewConfigFromEnv: %v", err)
	}
	if cfg.APIKey != "k1" || cfg.BaseURL != "https://api.example.com/anthropic" ||
		cfg.Model != "deepseek-v4" || cfg.ProviderType != "anthropic" ||
		cfg.SystemPrompt != "sys-prompt" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestNewConfigFromSpec_OverlayAndFallback(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "env-key")
	t.Setenv(kimi.EnvKeyModel, "env-model")
	t.Setenv(kimi.EnvKeyBaseURL, "")

	// Empty spec → pure env (the server fallback agent's path).
	cfg, err := kimi.NewConfigFromSpec(nil, "sys")
	if err != nil {
		t.Fatalf("empty spec: %v", err)
	}
	if cfg.APIKey != "env-key" || cfg.Model != "env-model" {
		t.Fatalf("empty spec should ride env: %+v", cfg)
	}

	// Overlay overrides set fields; an agent carrying its own key + model needs
	// no platform env for those.
	cfg, err = kimi.NewConfigFromSpec(
		json.RawMessage(`{"model":"spec-model","api_key":"spec-key","base_url":"https://x/anthropic"}`), "sys")
	if err != nil {
		t.Fatalf("overlay: %v", err)
	}
	if cfg.Model != "spec-model" || cfg.APIKey != "spec-key" || cfg.BaseURL != "https://x/anthropic" {
		t.Fatalf("overlay should win over env: %+v", cfg)
	}

	// A partial overlay leaves unset fields on the env default.
	cfg, err = kimi.NewConfigFromSpec(json.RawMessage(`{"model":"only-model"}`), "sys")
	if err != nil {
		t.Fatalf("partial overlay: %v", err)
	}
	if cfg.Model != "only-model" || cfg.APIKey != "env-key" {
		t.Fatalf("partial overlay: model from spec, key from env: %+v", cfg)
	}

	// Malformed config is a hard error (fail fast at assembly).
	if _, err := kimi.NewConfigFromSpec(json.RawMessage(`{bad`), "sys"); err == nil {
		t.Fatal("expected error for malformed spec config")
	}
}

func TestBuildSystemPrompt_EmptyDomain(t *testing.T) {
	p := kimi.BuildSystemPrompt(kimi.Situation{Host: "server"}, "", "")
	if !strings.Contains(p, "coagent agent") {
		t.Fatalf("platform teaching missing: %q", p[:80])
	}
	if strings.Contains(p, "Channel template") {
		t.Fatal("empty domain must not add a template header")
	}
}

func TestBuildSystemPrompt_WithDomain(t *testing.T) {
	p := kimi.BuildSystemPrompt(kimi.Situation{Host: "daemon"}, "xhs-creator", "domain rules here")
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
	noWS := kimi.BuildSystemPrompt(kimi.Situation{Host: "server"}, "", "")
	if !strings.Contains(noWS, "NO private workspace") {
		t.Fatal("no-workspace fact missing")
	}
	if !strings.Contains(noWS, "guide them to attach") {
		t.Fatal("bootstrap-guide behaviour not derived from the no-workspace fact")
	}

	withWS := kimi.BuildSystemPrompt(kimi.Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: "/ws/ch-1"}, "", "")
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
	p := kimi.BuildSystemPrompt(kimi.Situation{}, "group", "d")
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
	if _, err := kimi.NewBridge(kimi.Config{Model: "m"}, testActorID, testChannelID, w); err == nil {
		t.Fatal("missing api key: want error")
	}
	if _, err := kimi.NewBridge(kimi.Config{APIKey: "k"}, testActorID, testChannelID, w); err == nil {
		t.Fatal("missing model: want error")
	}
	if _, err := kimi.NewBridge(testConfig(), "", testChannelID, w); err == nil {
		t.Fatal("missing self: want error")
	}
	if _, err := kimi.NewBridge(testConfig(), testActorID, "", w); err == nil {
		t.Fatal("missing channel: want error")
	}
	if _, err := kimi.NewBridge(testConfig(), testActorID, testChannelID, nil); err == nil {
		t.Fatal("nil writer: want error")
	}
}

// ---------------------------------------------------------------------------
// Turn machinery (Receive → private loop → emitted envelopes)
// ---------------------------------------------------------------------------

// scriptTextTurn emits deltas + a TurnEnd through the bridge's wire channel.
func scriptTextTurn(b **kimi.Bridge, text string) *scriptedAgent {
	return &scriptedAgent{emitFn: func(ctx context.Context, _ string) error {
		em := kimi.BridgeWireEmitter(*b)
		em.Emit(wire.TextDelta{Delta: text})
		em.Emit(wire.TurnEnd{StopReason: "end_turn"})
		return nil
	}}
}

func TestReceive_RequestRunsTurnAndEmitsSingleTerminal(t *testing.T) {
	w := &recordingWriter{}
	var b *kimi.Bridge
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
	// Sealed-pen: the bridge leaves identity ZERO; the pen injects the welded
	// (actorID, channelID) at write time. The recorder sees the pre-injection env.
	if out.Sender.ID != "" || out.Sender.Kind != "" {
		t.Fatalf("sender must be left empty for the pen to inject; got %+v", out.Sender)
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
	var b *kimi.Bridge
	sa := &scriptedAgent{emitFn: func(ctx context.Context, _ string) error {
		em := kimi.BridgeWireEmitter(b)
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
func startToolCall(t *testing.T, b *kimi.Bridge, trigger message.Envelope, params map[string]any) <-chan types.ToolResult {
	t.Helper()
	tool := pickToolByName(b, "call_actor")
	if tool == nil {
		t.Fatal("call_actor tool absent")
	}
	raw, _ := json.Marshal(params)
	ctx := kimi.WithTurnContext(context.Background(), trigger)
	out := make(chan types.ToolResult, 1)
	go func() {
		res, _ := tool.Execute(ctx, raw)
		out <- res
	}()
	return out
}

func TestCallActor_EmitsRequestAndReturnsInlineResponse(t *testing.T) {
	w := &recordingWriter{}
	var b *kimi.Bridge
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
	var b *kimi.Bridge
	cfg := testConfig()
	cfg.FastPathWindow = 30 * time.Millisecond
	bb, err := kimi.NewBridge(cfg, testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	b = bb
	kimi.SetAgentFactory(b, func(kimi.AgentConfig) (kimi.Agent, error) { return &scriptedAgent{}, nil })
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
	var b *kimi.Bridge
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
	var b *kimi.Bridge
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
	var b *kimi.Bridge
	var inputs []string
	var inputsMu sync.Mutex
	sa := &scriptedAgent{emitFn: func(ctx context.Context, input string) error {
		inputsMu.Lock()
		inputs = append(inputs, input)
		inputsMu.Unlock()
		em := kimi.BridgeWireEmitter(b)
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
	b, err := kimi.NewBridge(testConfig(), testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := kimi.EnvelopeIDForTest(b, 1234567890)
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
		if got := kimi.ClassifyLLMError(c.err); got != c.want {
			t.Fatalf("classify(%v) = %s want %s", c.err, got, c.want)
		}
	}
}

func TestStop_TearsDownCleanly(t *testing.T) {
	w := &recordingWriter{}
	var b *kimi.Bridge
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
