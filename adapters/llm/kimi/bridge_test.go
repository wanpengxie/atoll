package kimi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/ActOS/adapters/llm/kimi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// fakeIPC is a minimal IPCFacade satisfying the bridge contract for
// tests. Envelopes written via WriteEnvelope land on `written` so the
// test can assert visibility / payload / parent_id.
type fakeIPC struct {
	channelID channel.ID
	workerID  string
	actorID   string
	triggers  chan kimi.TriggerPayload

	mu      sync.Mutex
	written []message.Envelope
	wErr    error // optional injected write error
}

func newFakeIPC() *fakeIPC {
	return &fakeIPC{
		channelID: channel.ID("ch-test"),
		workerID:  "worker-T",
		actorID:   "agent:worker-T",
		triggers:  make(chan kimi.TriggerPayload, 8),
	}
}

func (f *fakeIPC) ChannelID() channel.ID                { return f.channelID }
func (f *fakeIPC) WorkerID() string                     { return f.workerID }
func (f *fakeIPC) WorkerActorID() string                { return f.actorID }
func (f *fakeIPC) Triggers() <-chan kimi.TriggerPayload { return f.triggers }

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
func triggerEnv(id string) kimi.TriggerPayload {
	body, _ := json.Marshal(map[string]string{"text": "hi"})
	return kimi.TriggerPayload{
		Envelope: message.Envelope{
			ID:         id,
			ChannelID:  "ch-test",
			Type:       "human.text",
			Visibility: message.VisibilityPublic,
			Sender:     message.Sender{Kind: actor.KindHuman, ID: "user-A"},
			Kind:       message.KindEvent,
			Payload:    body,
			Audience:   []string{"*"},
		},
		CorrelationID: "corr-1",
		Cursor:        42,
	}
}

func TestNewConfigFromEnv_RequiresAPIKey(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "")
	t.Setenv(kimi.EnvKeyModel, "deepseek-v4-pro")
	if _, err := kimi.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error when KIMI_API_KEY empty")
	}
}

func TestNewConfigFromEnv_RequiresModel(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "k")
	t.Setenv(kimi.EnvKeyModel, "")
	if _, err := kimi.NewConfigFromEnv(""); err == nil {
		t.Fatal("expected error when KIMI_MODEL empty")
	}
}

func TestNewConfigFromEnv_ReadsAllFields(t *testing.T) {
	t.Setenv(kimi.EnvKeyAPIKey, "k")
	t.Setenv(kimi.EnvKeyBaseURL, "https://example.test/anthropic")
	t.Setenv(kimi.EnvKeyModel, "deepseek-v4-pro")
	cfg, err := kimi.NewConfigFromEnv("prompt")
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
	got := kimi.BuildBasePrompt("group", "", kimi.ChannelContext{})
	if !strings.Contains(got, "coagent worker") {
		t.Errorf("base prompt missing platform teaching: %q", got)
	}
	if strings.Contains(got, "Channel template") {
		t.Errorf("base prompt should omit Channel template heading when domain empty")
	}
	if strings.Contains(got, "Channel context") {
		t.Errorf("base prompt should omit Channel context when ChannelContext zero")
	}
}

func TestBuildBasePrompt_WithDomain(t *testing.T) {
	got := kimi.BuildBasePrompt("xhs-creator", "You handle xhs-creator workflow.", kimi.ChannelContext{})
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

// TestBuildBasePrompt_WithChannelContext exercises the channel-context
// appendix (M1.6 follow-up — agent self-awareness fix). Asserts that
// actor_registry rows, type_registry rows, and device_sessions rows
// render into the prompt as markdown so the LLM can answer "what
// tools do I have / who else is in this channel" without exploring
// host filesystem.
func TestBuildBasePrompt_WithChannelContext(t *testing.T) {
	ctx := kimi.ChannelContext{
		ChannelID:   "f9831154-ch",
		ChannelType: "xhs-creator",
		Actors: []kimi.ActorInfo{
			{ActorID: "system", Kind: "system"},
			{ActorID: "agent:channel-agent", Kind: "agent", DisplayName: "channel agent"},
			{ActorID: "tool:xhs-adapter", Kind: "tool", Binding: "via_server_transit"},
			{ActorID: "user:2cc317ee", Kind: "human", DisplayName: "Wanpeng Xie"},
		},
		Types: []kimi.TypeInfo{
			{Type: "xhs.publish", HandlerActorID: "tool:xhs-adapter", HandlerBinding: "via_server_transit", AllowedKinds: []string{"request", "response", "event"}, MaxPendingMs: 300_000},
			{Type: "xhs.search", HandlerActorID: "tool:xhs-adapter", HandlerBinding: "via_server_transit", AllowedKinds: []string{"request", "response"}},
			{Type: "xhs.note.fetch", HandlerActorID: "tool:xhs-adapter", HandlerBinding: "via_server_transit", AllowedKinds: []string{"request", "response"}},
		},
		Devices: []kimi.DeviceInfo{
			{SessionID: "de67872c", DeviceID: "chrome-default", DeviceType: "xhs-chrome", State: "active"},
		},
	}
	got := kimi.BuildBasePrompt("xhs-creator", "domain body", ctx)
	for _, want := range []string{
		"# Channel context (xhs-creator)",
		"channel_id: f9831154-ch",
		"## Actors in this channel",
		"tool:xhs-adapter",
		"binding=via_server_transit",
		"## Tool / business types available",
		"xhs.publish",
		"300000",
		"xhs.search",
		"xhs.note.fetch",
		"## Active device sessions",
		"de67872c",
		"state=active",
		"domain body",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("base prompt missing %q\nfull prompt:\n%s", want, got)
		}
	}
}

// TestLoadChannelContextFile_RoundTrip ensures the JSON shape the
// daemon writes matches what cmd/worker reads. Guards the env-file
// hand-off contract.
func TestLoadChannelContextFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ctx.json")
	original := kimi.ChannelContext{
		ChannelID:   "abc",
		ChannelType: "xhs-creator",
		Actors:      []kimi.ActorInfo{{ActorID: "system", Kind: "system"}},
		Types: []kimi.TypeInfo{{
			Type:           "xhs.publish",
			HandlerActorID: "tool:xhs-adapter",
			AllowedKinds:   []string{"request"},
			SchemasByKind: map[string]json.RawMessage{
				"request": json.RawMessage(`{"type":"object","required":["title"]}`),
			},
			MaxPendingMs: 1234,
		}},
		Devices: []kimi.DeviceInfo{{SessionID: "s1", State: "active"}},
	}
	buf, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok, err := kimi.LoadChannelContextFile(path)
	if err != nil {
		t.Fatalf("LoadChannelContextFile: %v", err)
	}
	if !ok {
		t.Fatal("LoadChannelContextFile ok=false")
	}
	if got.ChannelID != "abc" || len(got.Actors) != 1 || len(got.Types) != 1 || len(got.Devices) != 1 {
		t.Errorf("round-trip mismatch: %#v", got)
	}
	if got.Types[0].MaxPendingMs != 1234 {
		t.Errorf("type max_pending_ms=%d want 1234", got.Types[0].MaxPendingMs)
	}
	if string(got.Types[0].SchemasByKind["request"]) != `{"type":"object","required":["title"]}` {
		t.Errorf("request schema=%s", got.Types[0].SchemasByKind["request"])
	}

	// Empty path → ok=false, no error.
	if _, ok, err := kimi.LoadChannelContextFile(""); err != nil || ok {
		t.Errorf("empty path: want ok=false err=nil, got ok=%v err=%v", ok, err)
	}

	// Missing file → error.
	if _, _, err := kimi.LoadChannelContextFile(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("missing file: want error, got nil")
	}
}

// TestBridge_NewBridgeRequiresAPIKey covers the construction-time
// fail-fast: callers must pass a non-empty API key.
func TestBridge_NewBridgeRequiresAPIKey(t *testing.T) {
	if _, err := kimi.NewBridge(kimi.Config{Model: "x"}); err == nil {
		t.Fatal("NewBridge accepted empty APIKey")
	}
}

func TestBridge_NewBridgeRequiresModel(t *testing.T) {
	if _, err := kimi.NewBridge(kimi.Config{APIKey: "k"}); err == nil {
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
	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
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
	if string(last.Sender.ID) != ipc.WorkerActorID() {
		t.Errorf("sender.id=%q want %q", last.Sender.ID, ipc.WorkerActorID())
	}
}

// TestBridge_RunEmitsProgressPerToolStep — when the wire stream carries
// ToolCallRequest → ToolCallResult pairs inside one Agent.Run (the
// go-kimi soul "step" boundary), the bridge MUST emit one
// `agent.progress` envelope per step, BEFORE the terminal `agent.text`
// envelope. With 2 step boundaries we expect 2 progress + 1 text.
func TestBridge_RunEmitsProgressPerToolStep(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
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

	// Envelope 1 — progress for step 1 (shell).
	p1 := written[0]
	if p1.Type != "agent.progress" {
		t.Fatalf("written[0].type=%q want agent.progress", p1.Type)
	}
	if p1.Visibility != message.VisibilityPublic {
		t.Errorf("p1 visibility=%q want public", p1.Visibility)
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

	// Envelope 2 — progress for step 2 (read_file).
	p2 := written[1]
	if p2.Type != "agent.progress" {
		t.Fatalf("written[1].type=%q want agent.progress", p2.Type)
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

	// Envelope 3 — terminal agent.text.
	final := written[2]
	if final.Type != "agent.text" {
		t.Fatalf("written[2].type=%q want agent.text", final.Type)
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

func TestBridge_ChannelTypeToolEmitsRequestAndReturnsResponse(t *testing.T) {
	cfg := mustConfig(t)
	cfg.ChannelContext = kimi.ChannelContext{
		Types: []kimi.TypeInfo{
			{
				Type:           "xhs.publish",
				HandlerActorID: "tool:xhs-adapter",
				AllowedKinds:   []string{"request", "response"},
				SchemasByKind: map[string]json.RawMessage{
					"request": json.RawMessage(`{"type":"object","required":["title","content"]}`),
				},
				MaxPendingMs: 1000,
			},
			{
				Type:           "xhs.note.archived",
				HandlerActorID: "tool:xhs-adapter",
				AllowedKinds:   []string{"event"},
			},
		},
	}
	b, err := kimi.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)

	kimi.SetAgentFactory(b, func(ac kimi.AgentConfig) (kimi.Agent, error) {
		if len(ac.AdditionalTools) != 1 {
			return nil, fmt.Errorf("AdditionalTools len=%d want 1", len(ac.AdditionalTools))
		}
		tool := ac.AdditionalTools[0]
		if tool.Name() != "xhs.publish" {
			return nil, fmt.Errorf("tool name=%q want xhs.publish", tool.Name())
		}
		if schema := string(tool.ParameterSchema()); !strings.Contains(schema, `"title"`) {
			return nil, fmt.Errorf("tool schema=%s want title requirement", schema)
		}
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
				call := types.ToolCall{
					ID:        "call-xhs-publish",
					Name:      "xhs.publish",
					Arguments: map[string]any{"title": "hello", "content": "world"},
				}
				if err := emitter.Emit(wire.ToolCallRequest{ID: call.ID, ToolCall: call}); err != nil {
					return err
				}
				result, err := tool.Execute(ctx, json.RawMessage(`{"title":"hello","content":"world"}`))
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
		if req.Audience[0] != "tool:xhs-adapter" {
			injectErr <- fmt.Errorf("request audience=%v want [tool:xhs-adapter]", req.Audience)
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

func TestBridge_ChannelTypeToolTimeoutReturnsErrorResult(t *testing.T) {
	cfg := mustConfig(t)
	cfg.ChannelContext = kimi.ChannelContext{Types: []kimi.TypeInfo{{
		Type:           "xhs.publish",
		HandlerActorID: "tool:xhs-adapter",
		AllowedKinds:   []string{"request"},
		SchemasByKind:  map[string]json.RawMessage{"request": json.RawMessage(`{"type":"object"}`)},
		MaxPendingMs:   20,
	}}}
	b, err := kimi.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)
	kimi.SetAgentFactory(b, func(ac kimi.AgentConfig) (kimi.Agent, error) {
		tool := ac.AdditionalTools[0]
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				result, err := tool.Execute(ctx, json.RawMessage(`{"title":"slow"}`))
				if err != nil {
					return err
				}
				resultCh <- result
				emitter := kimi.BridgeWireEmitter(b)
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
	if !result.IsError {
		t.Fatalf("ToolResult.IsError=false; value=%#v", result.Value.Value)
	}
	if !strings.Contains(fmt.Sprint(result.Value.Value), "timed out") {
		t.Errorf("ToolResult value=%#v want timeout text", result.Value.Value)
	}
}

func TestBridge_ChannelTypeToolTerminalFailureReturnsErrorResult(t *testing.T) {
	cfg := mustConfig(t)
	cfg.ChannelContext = kimi.ChannelContext{Types: []kimi.TypeInfo{{
		Type:           "xhs.publish",
		HandlerActorID: "tool:xhs-adapter",
		AllowedKinds:   []string{"request"},
		SchemasByKind:  map[string]json.RawMessage{"request": json.RawMessage(`{"type":"object"}`)},
		MaxPendingMs:   1000,
	}}}
	b, err := kimi.NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	ipc := newFakeIPC()
	resultCh := make(chan types.ToolResult, 1)
	kimi.SetAgentFactory(b, func(ac kimi.AgentConfig) (kimi.Agent, error) {
		tool := ac.AdditionalTools[0]
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				result, err := tool.Execute(ctx, json.RawMessage(`{"title":"boom"}`))
				if err != nil {
					return err
				}
				resultCh <- result
				emitter := kimi.BridgeWireEmitter(b)
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
			ipc.triggers <- responseForRequest(req, json.RawMessage(`{"status":"failed","reason":"adapter_default_timeout"}`))
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
	if !ok || value["error"] != "adapter_default_timeout" {
		t.Fatalf("ToolResult value=%#v want error=adapter_default_timeout", result.Value.Value)
	}
}

// TestBridge_RunEmitsFailedTerminalOnLLMError — when go-kimi returns
// an *LLMError with StatusCode 429, the bridge MUST emit a public
// terminal envelope with payload.next_action=failed + reason=llm_rate_limit.
func TestBridge_RunEmitsFailedTerminalOnLLMError(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
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

	err := b.Run(ctx, ipc)
	if err == nil {
		t.Fatal("Run should propagate the LLM error")
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
		id := kimi.EnvelopeIDForTest(b, ipc, 123_456)
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

	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
		return &scriptedAgent{
			emitFn: func(ctx context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
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
			if got := kimi.ClassifyLLMError(tc.err); got != tc.want {
				t.Errorf("classify(%v)=%q want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestBridge_RunMaxTurnsExit — feed N triggers (N = MaxTurns) and
// assert the bridge emits the canonical max_turns terminal envelope.
func TestBridge_RunMaxTurnsExit(t *testing.T) {
	cfg := mustConfig(t)
	cfg.MaxTurns = 2
	b, err := kimi.NewBridge(cfg)
	if err != nil {
		t.Fatal(err)
	}

	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
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

func mustConfig(t *testing.T) kimi.Config {
	t.Helper()
	return kimi.Config{
		APIKey:       "fake-key",
		Model:        "fake-model",
		ProviderType: "anthropic",
		SystemPrompt: "test prompt",
		MaxTurns:     4,
		WorkDir:      t.TempDir(),
	}
}

func mustBridge(t *testing.T) *kimi.Bridge {
	t.Helper()
	cfg := mustConfig(t)
	b, err := kimi.NewBridge(cfg)
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

func responseForRequest(req message.Envelope, payload json.RawMessage) kimi.TriggerPayload {
	audience := []string{string(req.Sender.ID)}
	senderID := actor.ActorID("tool:test")
	if len(req.Audience) > 0 {
		senderID = actor.ActorID(req.Audience[0])
	}
	return kimi.TriggerPayload{
		Envelope: message.Envelope{
			ID:            "response-" + req.ID,
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
