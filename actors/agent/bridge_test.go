package agent_test

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

	"github.com/wanpengxie/ActOS/actors/agent"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// pickToolByName looks up an AdditionalTools entry by Name(). Returns
// nil when absent — caller fails the test.
func pickToolByName(tools []gokimitools.Tool, name string) gokimitools.Tool {
	for _, t := range tools {
		if t == nil {
			continue
		}
		if t.Name() == name {
			return t
		}
	}
	return nil
}

// fakeIPC is a minimal IPCFacade satisfying the bridge contract for
// tests. Envelopes written via WriteEnvelope land on `written` so the
// test can assert visibility / payload / parent_id.
type fakeIPC struct {
	channelID channel.ID
	workerID  string
	actorID   actor.ActorID
	triggers  chan agent.TriggerPayload

	mu      sync.Mutex
	written []message.Envelope
	wErr    error // optional injected write error
}

func newFakeIPC() *fakeIPC {
	return &fakeIPC{
		channelID: channel.ID("ch-test"),
		workerID:  "worker-T",
		actorID:   "agent:worker-T",
		triggers:  make(chan agent.TriggerPayload, 8),
	}
}

func (f *fakeIPC) ChannelID() channel.ID                { return f.channelID }
func (f *fakeIPC) WorkerID() string                     { return f.workerID }
func (f *fakeIPC) WorkerActorID() actor.ActorID         { return f.actorID }
func (f *fakeIPC) Triggers() <-chan agent.TriggerPayload { return f.triggers }

func (f *fakeIPC) WriteEnvelope(_ context.Context, env message.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wErr != nil {
		return f.wErr
	}
	f.written = append(f.written, env)
	return nil
}

func (f *fakeIPC) Written() []message.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]message.Envelope, len(f.written))
	copy(out, f.written)
	return out
}

// scriptedAgent is the kimiAgent test double — emits a canned wire
// sequence into the bridge's wire channel and returns the configured
// `runErr`. Used through the agentNew test hook (see bridgeWithAgent).
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

// triggerEnv returns a representative trigger payload.
func triggerEnv(id string) agent.TriggerPayload {
	body, _ := json.Marshal(map[string]string{"text": "hi"})
	return agent.TriggerPayload{
		Envelope: message.Envelope{
			ID:         message.ID(id),
			ChannelID:  "ch-test",
			Type:       "human.text",
			Visibility: message.VisibilityPublic,
			Sender:     message.Sender{Kind: actor.KindHuman, ID: "user-A"},
			Kind:       message.KindEvent,
			Payload:    body,
			Audience:   message.Audience{actor.SystemActorID},
		},
		CorrelationID: "corr-1",
		Cursor:        42,
	}
}

func TestNewConfigFromEnv_RequiresAPIKey(t *testing.T) {
	t.Setenv(agent.EnvKeyAPIKey, "")
	t.Setenv(agent.EnvKeyModel, "deepseek-v4-pro")
	if _, err := agent.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error when KIMI_API_KEY empty")
	}
}

func TestNewConfigFromEnv_RequiresModel(t *testing.T) {
	t.Setenv(agent.EnvKeyAPIKey, "k")
	t.Setenv(agent.EnvKeyModel, "")
	if _, err := agent.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error when KIMI_MODEL empty")
	}
}

func TestNewConfigFromEnv_ReadsAllFields(t *testing.T) {
	t.Setenv(agent.EnvKeyAPIKey, "k")
	t.Setenv(agent.EnvKeyBaseURL, "https://example.test/anthropic")
	t.Setenv(agent.EnvKeyModel, "deepseek-v4-pro")
	cfg, err := agent.NewConfigFromEnv("prompt")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cfg.APIKey != "k" || cfg.Model != "deepseek-v4-pro" {
		t.Errorf("cfg=%+v", cfg)
	}
	if cfg.BaseURL != "https://example.test/anthropic" {
		t.Errorf("BaseURL=%s", cfg.BaseURL)
	}
	if cfg.SystemPrompt != "prompt" {
		t.Errorf("SystemPrompt=%s", cfg.SystemPrompt)
	}
	if cfg.ProviderType != "anthropic" {
		t.Errorf("ProviderType=%s", cfg.ProviderType)
	}
}

func TestBuildBasePrompt_EmptyDomain(t *testing.T) {
	got := agent.BuildBasePrompt("group", "")
	if !strings.Contains(got, "coagent worker") {
		t.Errorf("base prompt missing platform teaching: %q", got)
	}
	if strings.Contains(got, "Channel template") {
		t.Errorf("base prompt should omit Channel template heading when domain empty")
	}
	if strings.Contains(got, "Channel context") {
		t.Errorf("base prompt must not carry a frozen channel-context appendix")
	}
}

func TestBuildBasePrompt_WithDomain(t *testing.T) {
	got := agent.BuildBasePrompt("xhs-creator", "You handle xhs-creator workflow.")
	if !strings.Contains(got, "coagent worker") {
		t.Error("missing platform teaching")
	}
	if !strings.Contains(got, "Channel template: xhs-creator") {
		t.Error("missing channel type heading")
	}
	if !strings.Contains(got, "xhs-creator workflow") {
		t.Error("missing domain prompt body")
	}
}

// TestBuildBasePrompt_NoFrozenActorSnapshot pins the defrost contract: the
// base prompt carries NO actor/type catalog. The concrete set of actors +
// request-callable types is dynamic channel state discovered live via the
// list_actors / describe_* meta tools, never baked into the cached prefix.
func TestBuildBasePrompt_NoFrozenActorSnapshot(t *testing.T) {
	got := agent.BuildBasePrompt("xhs-creator", "domain body")
	for _, banned := range []string{
		"Channel context",
		"## Actors in this channel",
		"## Device actors",
		"tool:xhs",
	} {
		if strings.Contains(got, banned) {
			t.Errorf("base prompt must not contain frozen snapshot fragment %q\nfull prompt:\n%s", banned, got)
		}
	}
	if !strings.Contains(got, "domain body") {
		t.Error("missing domain prompt body")
	}
}

// TestBridge_NewBridgeRequiresAPIKey covers the construction-time
// fail-fast: callers must pass a non-empty API key.
func TestBridge_NewBridgeRequiresAPIKey(t *testing.T) {
	if _, err := agent.NewBridge(agent.Config{Model: "x"}); err == nil {
		t.Fatal("NewBridge accepted empty APIKey")
	}
}

func TestBridge_NewBridgeRequiresModel(t *testing.T) {
	if _, err := agent.NewBridge(agent.Config{APIKey: "k"}); err == nil {
		t.Fatal("NewBridge accepted empty Model")
	}
}

// TestBridge_RunEmitsSingleTerminalOnTextDelta — feed many TextDelta
// chunks + a TurnEnd and assert the bridge emits EXACTLY ONE envelope
// (the terminal one), carrying the full concatenated text. Streaming
// chunks must never leak into the v4 envelope layer.
func TestBridge_RunEmitsSingleTerminalOnTextDelta(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	chunks := []string{"Hello", " ", "stream", " ", "world", "!"}

	// Inject the scripted agent via the exported test hook below.
	agent.SetAgentFactory(b, func(_ agent.AgentConfig) (agent.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				for _, c := range chunks {
					if err := emitter.Emit(wire.TextDelta{Delta: c}); err != nil {
						return err
					}
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		ipc.triggers <- triggerEnv("t-1")
		close(ipc.triggers)
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}

	written := ipc.Written()
	if len(written) != 1 {
		t.Fatalf("expected exactly 1 envelope (zero streaming, one terminal); got %d", len(written))
	}
	last := written[0]
	if last.Visibility != message.VisibilityPublic {
		t.Errorf("terminal envelope visibility=%q want public", last.Visibility)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if payload["next_action"] != "done" {
		t.Errorf("next_action=%v want done", payload["next_action"])
	}
	if payload["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason=%v want end_turn", payload["stop_reason"])
	}
	wantText := strings.Join(chunks, "")
	if got, _ := payload["text"].(string); got != wantText {
		t.Errorf("text=%q want %q", got, wantText)
	}
	if last.CorrelationID != "corr-1" {
		t.Errorf("correlation_id=%q want corr-1", last.CorrelationID)
	}
	if last.ParentID != "t-1" {
		t.Errorf("parent_id=%q want t-1", last.ParentID)
	}
	if last.Sender.ID != ipc.WorkerActorID() {
		t.Errorf("sender.id=%q want %q", last.Sender.ID, ipc.WorkerActorID())
	}
}

// TestBridge_RunEmitsProgressPerToolStep — when the wire stream carries
// ToolCallRequest → ToolCallResult pairs inside one Agent.Run (the
// go-kimi soul "step" boundary), the bridge MUST emit one progress
// envelope per step, BEFORE the terminal agent.text envelope. With 2
// step boundaries we expect 2 progress + 1 text.
//
// Progress envelopes share type `agent.text` with the terminal reply
// but carry `visibility=system` per impl-vocabulary §2.3 (the
// historical standalone `agent.progress` type was collapsed during
// m1.3 freeze — see R5-19).
func TestBridge_RunEmitsProgressPerToolStep(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	agent.SetAgentFactory(b, func(_ agent.AgentConfig) (agent.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				// Step 1: one shell tool call → result.
				if err := emitter.Emit(wire.ToolCallRequest{
					ID: "tc-1",
					ToolCall: types.ToolCall{
						ID:        "tc-1",
						Name:      "shell",
						Arguments: map[string]any{"cmd": "ls -laR .kimi/"},
					},
				}); err != nil {
					return err
				}
				if err := emitter.Emit(wire.ToolCallResult{
					ID:     "tc-1",
					Result: types.ToolResult{ToolCallID: "tc-1"},
				}); err != nil {
					return err
				}
				// Step 2: a read_file call → result.
				if err := emitter.Emit(wire.ToolCallRequest{
					ID: "tc-2",
					ToolCall: types.ToolCall{
						ID:        "tc-2",
						Name:      "read_file",
						Arguments: map[string]any{"path": "state.json"},
					},
				}); err != nil {
					return err
				}
				if err := emitter.Emit(wire.ToolCallResult{
					ID:     "tc-2",
					Result: types.ToolResult{ToolCallID: "tc-2"},
				}); err != nil {
					return err
				}
				// Final text reply.
				if err := emitter.Emit(wire.TextDelta{Delta: "Done — found 3 files."}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "stop"})
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		ipc.triggers <- triggerEnv("t-prog-1")
		close(ipc.triggers)
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}

	written := ipc.Written()
	if len(written) != 3 {
		t.Fatalf("expected 3 envelopes (2 progress + 1 text); got %d", len(written))
	}

	// Envelope 1 — progress for step 1 (shell): agent.text + system.
	p1 := written[0]
	if p1.Type != "agent.text" {
		t.Fatalf("written[0].type=%q want agent.text (progress)", p1.Type)
	}
	if p1.Visibility != message.VisibilitySystem {
		t.Errorf("p1 visibility=%q want system", p1.Visibility)
	}
	var pp1 map[string]any
	if err := json.Unmarshal(p1.Payload, &pp1); err != nil {
		t.Fatalf("p1 payload decode: %v", err)
	}
	if ti, _ := pp1["turn_index"].(float64); int(ti) != 1 {
		t.Errorf("p1 turn_index=%v want 1", pp1["turn_index"])
	}
	if si, _ := pp1["step_index"].(float64); int(si) != 1 {
		t.Errorf("p1 step_index=%v want 1", pp1["step_index"])
	}
	tools1, ok := pp1["tool_calls"].([]any)
	if !ok || len(tools1) != 1 {
		t.Fatalf("p1 tool_calls shape unexpected: %v", pp1["tool_calls"])
	}
	tc1, _ := tools1[0].(map[string]any)
	if tc1["name"] != "shell" {
		t.Errorf("p1 tool name=%v want shell", tc1["name"])
	}
	if got, _ := tc1["preview"].(string); !strings.Contains(got, "ls -laR") {
		t.Errorf("p1 tool preview=%q want contains 'ls -laR'", got)
	}

	// Envelope 2 — progress for step 2 (read_file): agent.text + system.
	p2 := written[1]
	if p2.Type != "agent.text" {
		t.Fatalf("written[1].type=%q want agent.text (progress)", p2.Type)
	}
	if p2.Visibility != message.VisibilitySystem {
		t.Errorf("p2 visibility=%q want system", p2.Visibility)
	}
	var pp2 map[string]any
	if err := json.Unmarshal(p2.Payload, &pp2); err != nil {
		t.Fatalf("p2 payload decode: %v", err)
	}
	if si, _ := pp2["step_index"].(float64); int(si) != 2 {
		t.Errorf("p2 step_index=%v want 2", pp2["step_index"])
	}
	tools2, _ := pp2["tool_calls"].([]any)
	if len(tools2) != 1 {
		t.Fatalf("p2 tool_calls len=%d want 1", len(tools2))
	}
	tc2, _ := tools2[0].(map[string]any)
	if tc2["name"] != "read_file" {
		t.Errorf("p2 tool name=%v want read_file", tc2["name"])
	}

	// Envelope 3 — terminal agent.text (visibility=public, the actual
	// reply, distinct from the per-step progress bubbles above).
	final := written[2]
	if final.Type != "agent.text" {
		t.Fatalf("written[2].type=%q want agent.text", final.Type)
	}
	if final.Visibility != message.VisibilityPublic {
		t.Errorf("final visibility=%q want public", final.Visibility)
	}
	var fp map[string]any
	if err := json.Unmarshal(final.Payload, &fp); err != nil {
		t.Fatalf("final payload decode: %v", err)
	}
	if fp["next_action"] != "done" {
		t.Errorf("final next_action=%v want done", fp["next_action"])
	}
	if fp["text"] != "Done — found 3 files." {
		t.Errorf("final text=%v", fp["text"])
	}

	// All three envelopes share the same correlation + parent_id.
	for i, env := range written {
		if env.CorrelationID != "corr-1" {
			t.Errorf("written[%d] correlation_id=%q want corr-1", i, env.CorrelationID)
		}
		if env.ParentID != "t-prog-1" {
			t.Errorf("written[%d] parent_id=%q want t-prog-1", i, env.ParentID)
		}
	}
}

func TestBridge_CallActorToolEmitsRequestAndReturnsResponse(t *testing.T) {
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)

	agent.SetAgentFactory(b, func(ac agent.AgentConfig) (agent.Agent, error) {
		// Substrate-native actor-CLI meta-tool surface.
		// Direct per-type injection was retired in favour of the
		// uniform envelope invocation primitive (see meta_tool.go +
		// channel_tool.go::channelTools). The async-decision trio
		// (await_result / abandon / list_pending) joined the original
		// four actor-CLI verbs for the fast-path mechanism.
		if len(ac.AdditionalTools) != 7 {
			return nil, fmt.Errorf("AdditionalTools len=%d want 7 (4 actor-CLI verbs + await_result/abandon/list_pending)", len(ac.AdditionalTools))
		}
		for _, name := range []string{"list_actors", "describe_actor", "describe_type", "await_result", "abandon", "list_pending"} {
			if pickToolByName(ac.AdditionalTools, name) == nil {
				return nil, fmt.Errorf("%s tool missing from AdditionalTools", name)
			}
		}
		callActor := pickToolByName(ac.AdditionalTools, "call_actor")
		if callActor == nil {
			return nil, fmt.Errorf("call_actor tool missing from AdditionalTools")
		}
		if schema := string(callActor.ParameterSchema()); !strings.Contains(schema, `"actor_id"`) {
			return nil, fmt.Errorf("call_actor schema missing actor_id: %s", schema)
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				call := types.ToolCall{
					ID:        "call-xhs-publish",
					Name:      "call_actor",
					Arguments: map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish", "payload": map[string]any{"title": "hello", "content": "world"}},
				}
				if err := emitter.Emit(wire.ToolCallRequest{ID: call.ID, ToolCall: call}); err != nil {
					return err
				}
				result, err := callActor.Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"hello","content":"world"}}`))
				if err != nil {
					return err
				}
				resultCh <- result
				if err := emitter.Emit(wire.ToolCallResult{ID: call.ID, Result: result}); err != nil {
					return err
				}
				if err := emitter.Emit(wire.TextDelta{Delta: "publish complete"}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	injectErr := make(chan error, 1)
	go func() {
		ipc.triggers <- triggerEnv("t-tool")
		req, err := waitForWritten(ctx, ipc, func(env message.Envelope) bool {
			return env.Type == "xhs.publish" && env.Kind == message.KindRequest
		})
		if err != nil {
			injectErr <- err
			return
		}
		if req.Audience[0] != "tool:xhs" {
			injectErr <- fmt.Errorf("request audience=%v want [tool:xhs]", req.Audience)
			return
		}
		if req.ParentID != "t-tool" || req.CorrelationID != "corr-1" {
			injectErr <- fmt.Errorf("request parent/correlation=%q/%q", req.ParentID, req.CorrelationID)
			return
		}
		var payload map[string]string
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			injectErr <- fmt.Errorf("request payload decode: %w", err)
			return
		}
		if payload["title"] != "hello" || payload["content"] != "world" {
			injectErr <- fmt.Errorf("request payload=%v", payload)
			return
		}
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"completed","note_id":"n-1","url":"https://xhs.test/n-1"}`))
		close(ipc.triggers)
		injectErr <- nil
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := <-injectErr; err != nil {
		t.Fatal(err)
	}
	result := <-resultCh
	if result.IsError {
		t.Fatalf("ToolResult.IsError=true; value=%#v", result.Value.Value)
	}
	value, ok := result.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("ToolResult value=%T want map", result.Value.Value)
	}
	if value["note_id"] != "n-1" {
		t.Errorf("ToolResult note_id=%v want n-1", value["note_id"])
	}
}

// TestBridge_CallActorToolSuperWindowReturnsAck asserts that an async
// call_actor (wait=false, fan-out) returns an ACK rather than blocking — the
// request stays in flight and the agent can await_result / let it return as a
// new trigger (§2.3.2). Post-defrost the caller no longer knows a per-type
// max_pending_ms client-side (the daemon owns it), so the deterministic
// degrade-to-ack path is the explicit wait=false mode.
func TestBridge_CallActorToolSuperWindowReturnsAck(t *testing.T) {
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)
	agent.SetAgentFactory(b, func(ac agent.AgentConfig) (agent.Agent, error) {
		callActor := pickToolByName(ac.AdditionalTools, "call_actor")
		if callActor == nil {
			return nil, fmt.Errorf("call_actor tool missing")
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				result, err := callActor.Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"slow"},"wait":false}`))
				if err != nil {
					return err
				}
				resultCh <- result
				emitter := agent.BridgeWireEmitter(b)
				if err := emitter.Emit(wire.ToolCallResult{ID: "call-timeout", Result: result}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		ipc.triggers <- triggerEnv("t-timeout")
		close(ipc.triggers)
	}()
	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	result := <-resultCh
	if result.IsError {
		t.Fatalf("super-window should return an ack, not an error: %#v", result.Value.Value)
	}
	root, ok := result.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("ack value type=%T", result.Value.Value)
	}
	if root["status"] != "accepted" {
		t.Fatalf("ack status=%v want accepted", root["status"])
	}
	if root["request_id"] == nil || root["request_id"] == "" {
		t.Fatalf("ack missing request_id: %#v", root)
	}
}

func TestBridge_CallActorToolTerminalFailureReturnsErrorResult(t *testing.T) {
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)
	agent.SetAgentFactory(b, func(ac agent.AgentConfig) (agent.Agent, error) {
		callActor := pickToolByName(ac.AdditionalTools, "call_actor")
		if callActor == nil {
			return nil, fmt.Errorf("call_actor tool missing")
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				result, err := callActor.Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"boom"}}`))
				if err != nil {
					return err
				}
				resultCh <- result
				emitter := agent.BridgeWireEmitter(b)
				if err := emitter.Emit(wire.ToolCallResult{ID: "call-failed", Result: result}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		ipc.triggers <- triggerEnv("t-failed")
		req, err := waitForWritten(ctx, ipc, func(env message.Envelope) bool {
			return env.Type == "xhs.publish" && env.Kind == message.KindRequest
		})
		if err == nil {
			ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"failed","reason":"unanswered_timeout"}`))
		}
		close(ipc.triggers)
	}()
	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	result := <-resultCh
	if !result.IsError {
		t.Fatalf("ToolResult.IsError=false; value=%#v", result.Value.Value)
	}
	value, ok := result.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("ToolResult value=%T want map", result.Value.Value)
	}
	errObj, ok := value["error"].(map[string]any)
	if !ok || errObj["code"] != "timeout" {
		t.Fatalf("ToolResult value=%#v want error.code=timeout", result.Value.Value)
	}
}

// TestBridge_RunEmitsFailedTerminalOnLLMError — when go-kimi returns
// an *LLMError with StatusCode 429, the bridge MUST emit a public
// terminal envelope with payload.next_action=failed + reason=llm_rate_limit.
//
// §3 ack 三分: an LLM error is a COMPLETE outcome (observable failed
// terminal envelope = closure), NOT a worker-fatal error. The trigger was
// already ACKed accepted and the turn ran async, so Run does not propagate
// the error — the worker survives and keeps serving subsequent triggers.
func TestBridge_RunEmitsFailedTerminalOnLLMError(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	agent.SetAgentFactory(b, func(_ agent.AgentConfig) (agent.Agent, error) {
		return &scriptedAgent{
			runErr: &kimierrors.LLMError{Provider: "deepseek", StatusCode: 429, Cause: errors.New("rate limited")},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		ipc.triggers <- triggerEnv("t-err")
		close(ipc.triggers)
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run should survive a per-turn LLM error (closure via envelope): %v", err)
	}
	written := ipc.Written()
	if len(written) == 0 {
		t.Fatalf("no terminal envelope emitted")
	}
	last := written[len(written)-1]
	var payload map[string]any
	if e := json.Unmarshal(last.Payload, &payload); e != nil {
		t.Fatalf("payload decode: %v", e)
	}
	if payload["next_action"] != "failed" {
		t.Errorf("next_action=%v want failed", payload["next_action"])
	}
	if payload["reason"] != "llm_rate_limit" {
		t.Errorf("reason=%v want llm_rate_limit", payload["reason"])
	}
	if last.Visibility != message.VisibilityPublic {
		t.Errorf("visibility=%q want public", last.Visibility)
	}
	if last.CorrelationID != "corr-1" {
		t.Errorf("correlation_id=%q want corr-1", last.CorrelationID)
	}
}

func TestBridge_EnvelopeIDUniqueWithinSameMillisecond(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		id := agent.EnvelopeIDForTest(b, ipc, 123_456)
		if seen[id] {
			t.Fatalf("duplicate envelope id generated: %s", id)
		}
		seen[id] = true
	}
}

func TestBridge_ConsumeWireErrorCancelsAgentRun(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()
	ipc.wErr = errors.New("ipc write failed")
	agentExited := make(chan struct{})

	agent.SetAgentFactory(b, func(_ agent.AgentConfig) (agent.Agent, error) {
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				if err := emitter.Emit(wire.TurnEnd{StopReason: "end_turn"}); err != nil {
					return err
				}
				<-ctx.Done()
				close(agentExited)
				return ctx.Err()
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		ipc.triggers <- triggerEnv("t-cancel")
		close(ipc.triggers)
	}()

	err := b.Run(ctx, ipc)
	if err == nil || !strings.Contains(err.Error(), "ipc write failed") {
		t.Fatalf("Run error=%v want ipc write failed", err)
	}
	select {
	case <-agentExited:
	case <-time.After(1 * time.Second):
		t.Fatal("agent.Run did not exit within 1s after consumeWire error")
	}
}

// TestBridge_ClassifyLLMError_NetworkBuckets — confirm net.Error /
// url.Error / context.DeadlineExceeded all collapse into llm_network.
func TestBridge_ClassifyLLMError_NetworkBuckets(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"rate_limit", &kimierrors.LLMError{StatusCode: 429}, "llm_rate_limit"},
		{"auth_401", &kimierrors.LLMError{StatusCode: 401}, "llm_auth"},
		{"auth_403", &kimierrors.LLMError{StatusCode: 403}, "llm_auth"},
		{"server_500", &kimierrors.LLMError{StatusCode: 500}, "llm_server"},
		{"server_503", &kimierrors.LLMError{StatusCode: 503}, "llm_server"},
		{"unknown_400", &kimierrors.LLMError{StatusCode: 400}, "llm_unknown"},
		{"url_err", &url.Error{Op: "Post", URL: "https://x", Err: errors.New("dial")}, "llm_network"},
		{"deadline", context.DeadlineExceeded, "llm_network"},
		{"canceled", context.Canceled, "llm_network"},
		{"plain", errors.New("boom"), "llm_unknown"},
		{"nil", nil, ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := agent.ClassifyLLMError(tc.err); got != tc.want {
				t.Errorf("classify(%v)=%q want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestBridge_CallActorToolProvisionalBeforeFinalDoesNotResolveEarly
// asserts the end-to-end async-aware path: when the daemon delivers a
// Layer 2 provisional response (`status=processing`) before the final
// (`status=completed`), the bridge's call_actor tool MUST wait for the
// final and return that payload to the LLM — provisional traffic is
// quarantined per response-multitype-refactor.md §3.4 D v1 ("ignore
// provisional, still wait for final").
//
// Also covers the Layer 3 namespace case: a `xhs.login_queued`
// provisional in between two responses must not short-circuit the
// future either.
func TestBridge_CallActorToolProvisionalBeforeFinalDoesNotResolveEarly(t *testing.T) {
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)

	agent.SetAgentFactory(b, func(ac agent.AgentConfig) (agent.Agent, error) {
		callActor := pickToolByName(ac.AdditionalTools, "call_actor")
		if callActor == nil {
			return nil, fmt.Errorf("call_actor tool missing")
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				call := types.ToolCall{
					ID:        "call-prov-final",
					Name:      "call_actor",
					Arguments: map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish", "payload": map[string]any{"title": "hello"}},
				}
				if err := emitter.Emit(wire.ToolCallRequest{ID: call.ID, ToolCall: call}); err != nil {
					return err
				}
				result, err := callActor.Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"hello"}}`))
				if err != nil {
					return err
				}
				resultCh <- result
				if err := emitter.Emit(wire.ToolCallResult{ID: call.ID, Result: result}); err != nil {
					return err
				}
				if err := emitter.Emit(wire.TextDelta{Delta: "published"}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	injectErr := make(chan error, 1)
	go func() {
		ipc.triggers <- triggerEnv("t-prov-final")
		req, err := waitForWritten(ctx, ipc, func(env message.Envelope) bool {
			return env.Type == "xhs.publish" && env.Kind == message.KindRequest
		})
		if err != nil {
			injectErr <- err
			return
		}

		// 1. Layer 2 core provisional — must NOT resolve the future.
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"processing","progress":"uploading"}`))
		// 2. Layer 3 business namespace provisional — must NOT resolve.
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"xhs.login_queued","queue_position":3}`))
		// Brief pause so the bridge demonstrably did not resolve early.
		// (50ms is generous — dispatch is sync.)
		time.Sleep(50 * time.Millisecond)
		select {
		case <-resultCh:
			injectErr <- fmt.Errorf("call_actor resolved on provisional before final arrived")
			return
		default:
		}
		// 3. Final completed — resolves the future.
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"completed","note_id":"n-2","url":"https://xhs.test/n-2"}`))
		close(ipc.triggers)
		injectErr <- nil
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := <-injectErr; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-resultCh:
		if result.IsError {
			t.Fatalf("ToolResult.IsError=true; value=%#v", result.Value.Value)
		}
		value, ok := result.Value.Value.(map[string]any)
		if !ok {
			t.Fatalf("ToolResult value=%T want map", result.Value.Value)
		}
		if value["note_id"] != "n-2" {
			t.Errorf("ToolResult note_id=%v want n-2 (final payload)", value["note_id"])
		}
	default:
		t.Fatal("call_actor never received the final response")
	}
}

// TestBridge_CallActorToolProvisionalOnlyDegradesToAck asserts that when only
// provisional responses arrive (no final), provisional traffic alone does NOT
// satisfy the caller — the agent gets an ack and the request stays in flight
// (§2.3.2); provisionals are swallowed, never resolving the future early.
// Post-defrost the caller no longer knows a per-type max_pending_ms (the
// daemon owns it), so the deterministic non-blocking path is wait=false.
func TestBridge_CallActorToolProvisionalOnlyDegradesToAck(t *testing.T) {
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)

	agent.SetAgentFactory(b, func(ac agent.AgentConfig) (agent.Agent, error) {
		callActor := pickToolByName(ac.AdditionalTools, "call_actor")
		if callActor == nil {
			return nil, fmt.Errorf("call_actor tool missing")
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				result, err := callActor.Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"slow"},"wait":false}`))
				if err != nil {
					return err
				}
				resultCh <- result
				emitter := agent.BridgeWireEmitter(b)
				if err := emitter.Emit(wire.ToolCallResult{ID: "call-prov-timeout", Result: result}); err != nil {
					return err
				}
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		ipc.triggers <- triggerEnv("t-prov-timeout")
		req, err := waitForWritten(ctx, ipc, func(env message.Envelope) bool {
			return env.Type == "xhs.publish" && env.Kind == message.KindRequest
		})
		if err != nil {
			close(ipc.triggers)
			return
		}
		// Drip provisional updates but never send the final.
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"processing"}`))
		ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"queued"}`))
		// Do not close ipc.triggers here — callActor.Execute is waiting
		// on its own timeout (max_pending_ms=100). Closing triggers
		// would tear the bridge down before the timeout returns the
		// ToolResult.
	}()

	// We expect Run to keep going until either Execute times out (ToolResult
	// flows + TurnEnd) or ctx fires. The scripted agent emits TurnEnd as
	// soon as Execute returns.
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- b.Run(ctx, ipc) }()

	select {
	case result := <-resultCh:
		if result.IsError {
			t.Fatalf("provisional-only should degrade to an ack, not an error: %#v", result.Value.Value)
		}
		root, ok := result.Value.Value.(map[string]any)
		if !ok {
			t.Fatalf("ack value type=%T", result.Value.Value)
		}
		if root["status"] != "accepted" {
			t.Fatalf("ack status=%v want accepted (provisionals must not resolve the future)", root["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call_actor did not degrade to ack on provisional-only stream")
	}
	cancel()
	// Drain Run; it may return ctx.Err or a clean nil — either is fine.
	select {
	case <-runErrCh:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestBridge_RunMaxTurnsExit — feed N triggers (N = MaxTurns) and
// assert the bridge emits the canonical max_turns terminal envelope.
func TestBridge_RunMaxTurnsExit(t *testing.T) {
	cfg := mustConfig(t)
	cfg.MaxTurns = 2
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatal(err)
	}

	agent.SetAgentFactory(b, func(_ agent.AgentConfig) (agent.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := agent.BridgeWireEmitter(b)
				return emitter.Emit(wire.TurnEnd{StopReason: "end_turn"})
			},
		}, nil
	})

	ipc := newFakeIPC()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		ipc.triggers <- triggerEnv("t-1")
		ipc.triggers <- triggerEnv("t-2")
	}()

	if err := b.Run(ctx, ipc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	written := ipc.Written()
	if len(written) == 0 {
		t.Fatal("no envelopes emitted")
	}
	last := written[len(written)-1]
	var payload map[string]any
	if e := json.Unmarshal(last.Payload, &payload); e != nil {
		t.Fatalf("payload decode: %v", e)
	}
	if !strings.Contains(fmt.Sprint(payload["text"]), "max_turns") {
		t.Errorf("terminal text=%v want contains max_turns", payload["text"])
	}
	if payload["next_action"] != "done" {
		t.Errorf("next_action=%v want done", payload["next_action"])
	}
}

// ---- helpers ----

func mustConfig(t *testing.T) agent.Config {
	t.Helper()
	return agent.Config{
		APIKey:       "fake-key",
		Model:        "fake-model",
		ProviderType: "anthropic",
		SystemPrompt: "test prompt",
		MaxTurns:     4,
		WorkDir:      t.TempDir(),
	}
}

func mustBridge(t *testing.T) *agent.Bridge {
	t.Helper()
	cfg := mustConfig(t)
	b, err := agent.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	return b
}

func waitForWritten(ctx context.Context, ipc *fakeIPC, match func(message.Envelope) bool) (message.Envelope, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, env := range ipc.Written() {
			if match(env) {
				return env, nil
			}
		}
		select {
		case <-ctx.Done():
			return message.Envelope{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func responseForRequest(req message.Envelope, payload json.RawMessage) agent.TriggerPayload {
	audience := message.Audience{req.Sender.ID}
	senderID := actor.ActorID("tool:test")
	if len(req.Audience) > 0 {
		senderID = req.Audience[0]
	}
	return agent.TriggerPayload{
		Envelope: message.Envelope{
			ID:            message.ID("response-" + req.ID.String()),
			ChannelID:     req.ChannelID,
			Type:          req.Type,
			Kind:          message.KindResponse,
			Visibility:    req.Visibility,
			Sender:        message.Sender{Kind: actor.KindTool, ID: senderID},
			Audience:      audience,
			Payload:       payload,
			ParentID:      req.ID,
			CorrelationID: req.CorrelationID,
		},
		CorrelationID: req.CorrelationID,
	}
}
