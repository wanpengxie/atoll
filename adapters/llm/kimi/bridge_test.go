package kimi_test

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
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/ActOS/adapters/llm/kimi"
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
			Sender:     message.Sender{Kind: message.SenderHuman, ID: "user-A"},
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
	got := kimi.BuildBasePrompt("group", "")
	if !strings.Contains(got, "coagent worker") {
		t.Errorf("base prompt missing platform teaching: %q", got)
	}
	if strings.Contains(got, "Channel template") {
		t.Errorf("base prompt should omit Channel template heading when domain empty")
	}
}

func TestBuildBasePrompt_WithDomain(t *testing.T) {
	got := kimi.BuildBasePrompt("xhs-creator", "You handle xhs-creator workflow.")
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

// TestBridge_RunEmitsAgentTextOnTextDelta — feed a TextDelta + TurnEnd
// wire stream and assert the bridge emits at least one system-visibility
// agent.text intermediate (M1.6-T7 phase-4 streaming contract) plus a
// final public-visibility agent.text with next_action stamped.
func TestBridge_RunEmitsAgentTextOnTextDelta(t *testing.T) {
	b := mustBridge(t)
	ipc := newFakeIPC()

	// Inject the scripted agent via the exported test hook below.
	kimi.SetAgentFactory(b, func(_ kimi.AgentConfig) (kimi.Agent, error) {
		return &scriptedAgent{
			emitFn: func(_ context.Context, _ string) error {
				emitter := kimi.BridgeWireEmitter(b)
				if err := emitter.Emit(wire.TextDelta{Delta: "Hello "}); err != nil {
					return err
				}
				if err := emitter.Emit(wire.TextDelta{Delta: "world!"}); err != nil {
					return err
				}
				// Allow the flush ticker one tick worth of buffering;
				// the final emitBuffered before TurnEnd will flush the
				// remainder.
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
	if len(written) < 1 {
		t.Fatalf("expected at least one envelope, got %d", len(written))
	}
	// Final envelope must be public + carry next_action.
	last := written[len(written)-1]
	if last.Visibility != message.VisibilityPublic {
		t.Errorf("final envelope visibility=%q want public", last.Visibility)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if payload["next_action"] != "done" {
		t.Errorf("next_action=%v want done", payload["next_action"])
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
	cfg.TextDeltaFlushInterval = 5 * time.Millisecond
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
		APIKey:                 "fake-key",
		Model:                  "fake-model",
		ProviderType:           "anthropic",
		SystemPrompt:           "test prompt",
		MaxTurns:               4,
		WorkDir:                t.TempDir(),
		TextDeltaFlushInterval: 10 * time.Millisecond,
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
