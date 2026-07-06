package kimi

// Internal test (package kimi) driving the unexported engine directly — the §1
// 三件套 unit seam. The mailbox loop / turn queue / response分拣 / emit live in
// agent/base (tested there); the substrate JobTable is tested in lib/actorbase.

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/atoll/agent/base"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// recordSink captures the Outputs a turn emits.
type recordSink struct{ outputs []base.Output }

func (s *recordSink) Emit(o base.Output) error {
	s.outputs = append(s.outputs, o)
	return nil
}

// scriptedAgent is the kimiAgent test double: on Run it emits a canned wire
// sequence into the engine's wire channel, then returns runErr.
type scriptedAgent struct {
	wireCh chan<- wire.WireMessage
	msgs   []wire.WireMessage
	runErr error
}

func (s *scriptedAgent) Run(ctx context.Context, input string) error {
	for _, m := range s.msgs {
		select {
		case s.wireCh <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.runErr
}
func (s *scriptedAgent) Close() error { return nil }

// newTestEngine wires an engine to a scripted agent over a shared wire channel.
func newTestEngine(msgs []wire.WireMessage, runErr error) (*engine, *scriptedAgent) {
	wireCh := make(chan wire.WireMessage, 128)
	sa := &scriptedAgent{wireCh: wireCh, msgs: msgs, runErr: runErr}
	e := &engine{cfg: Config{Model: "m"}, wireCh: wireCh, kagent: sa}
	return e, sa
}

func triggerEnv(id string) message.Envelope {
	body, _ := json.Marshal(map[string]string{"text": "hi"})
	return message.Envelope{
		ID:            message.ID(id),
		Type:          "human.text",
		Sender:        message.Sender{Kind: actor.KindHuman, ID: "user-A"},
		Kind:          message.KindRequest,
		Payload:       body,
		CorrelationID: "corr-1",
	}
}

// TestKimiTurn_EmitsTerminal pins a text turn → one terminal Output (done).
func TestKimiTurn_EmitsTerminal(t *testing.T) {
	e, _ := newTestEngine([]wire.WireMessage{
		wire.TextDelta{Delta: "hello there"},
		wire.TurnEnd{StopReason: "end_turn"},
	}, nil)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-1"), CorrelationID: "corr-1", Index: 1}
	if err := e.Turn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if len(sink.outputs) != 1 {
		t.Fatalf("want 1 terminal output, got %d: %+v", len(sink.outputs), sink.outputs)
	}
	o := sink.outputs[0]
	if !o.Final || o.Text != "hello there" || o.NextAction != "done" {
		t.Fatalf("terminal = %+v", o)
	}
}

// TestKimiTurn_ProgressPerToolStep pins 2 intermediate progress Outputs
// (Final=false, step_index 1/2) + 1 terminal.
func TestKimiTurn_ProgressPerToolStep(t *testing.T) {
	e, _ := newTestEngine([]wire.WireMessage{
		wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t1", Name: "call_actor", Arguments: map[string]any{"cmd": "ls"}}},
		wire.ToolCallResult{},
		wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t2", Name: "call_actor"}},
		wire.ToolCallResult{},
		wire.TextDelta{Delta: "final answer"},
		wire.TurnEnd{StopReason: "end_turn"},
	}, nil)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-p"), Index: 1}
	if err := e.Turn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if len(sink.outputs) != 3 {
		t.Fatalf("want 2 progress + 1 terminal, got %d", len(sink.outputs))
	}
	for i := 0; i < 2; i++ {
		o := sink.outputs[i]
		if o.Final {
			t.Fatalf("progress %d should be intermediate (Final=false)", i)
		}
		if o.Extra["step_index"] != i+1 {
			t.Fatalf("progress %d step_index = %v", i, o.Extra["step_index"])
		}
	}
	if !sink.outputs[2].Final {
		t.Fatalf("last output should be terminal")
	}
}

// TestKimiTurn_LLMErrorFailedTerminal pins an Agent.Run error → failed terminal
// Output (Turn returns nil, actor stays alive).
func TestKimiTurn_LLMErrorFailedTerminal(t *testing.T) {
	llmErr := &kimierrors.LLMError{StatusCode: 429, Cause: errors.New("rate limited")}
	e, _ := newTestEngine(nil, llmErr)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-err"), Index: 1}
	if err := e.Turn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn should stay alive on an LLM error: %v", err)
	}
	if len(sink.outputs) != 1 {
		t.Fatalf("want 1 failed terminal, got %d", len(sink.outputs))
	}
	o := sink.outputs[0]
	if !o.Final || o.NextAction != "failed" || o.Reason != "llm_rate_limit" {
		t.Fatalf("failed terminal = %+v", o)
	}
}

// TestKimiChannelTools_AllMetaTools pins the FULL meta-tool surface is installed.
func TestKimiChannelTools_AllMetaTools(t *testing.T) {
	e := &engine{cfg: Config{Model: "m"}}
	tools := e.channelTools()
	if len(tools) != 7 {
		t.Fatalf("want 7 meta tools, got %d", len(tools))
	}
	found := false
	for _, tl := range tools {
		if tl.Name() == "call_actor" {
			found = true
		}
	}
	if !found {
		t.Fatal("call_actor not installed into the kimi tool surface")
	}
}

// --- config ----------------------------------------------------------------

func TestNewConfigFromSpec_RequiresAPIKey(t *testing.T) {
	t.Setenv(EnvKeyAPIKey, "")
	t.Setenv(EnvKeyModel, "m")
	if _, err := NewConfigFromSpec(nil, Situation{}); err == nil {
		t.Fatal("expected error when no api key")
	}
}

func TestNewConfigFromSpec_RequiresModel(t *testing.T) {
	t.Setenv(EnvKeyAPIKey, "k")
	t.Setenv(EnvKeyModel, "")
	if _, err := NewConfigFromSpec(nil, Situation{}); err == nil {
		t.Fatal("expected error when no model")
	}
}

func TestNewConfigFromSpec_OverlayAndFallback(t *testing.T) {
	t.Setenv(EnvKeyAPIKey, "env-key")
	t.Setenv(EnvKeyModel, "env-model")
	t.Setenv(EnvKeyBaseURL, "env-url")

	// No overlay → env defaults.
	cfg, err := NewConfigFromSpec(nil, Situation{Host: "server"})
	if err != nil {
		t.Fatalf("env config: %v", err)
	}
	if cfg.APIKey != "env-key" || cfg.Model != "env-model" || cfg.BaseURL != "env-url" {
		t.Fatalf("env config wrong: %+v", cfg)
	}

	// Overlay wins; A3 domain prompt from spec.Config folds into the prompt.
	raw := json.RawMessage(`{"model":"spec-model","api_key":"spec-key","domain_prompt":"be terse","channel_type":"support"}`)
	cfg, err = NewConfigFromSpec(raw, Situation{Host: "server"})
	if err != nil {
		t.Fatalf("overlay config: %v", err)
	}
	if cfg.Model != "spec-model" || cfg.APIKey != "spec-key" {
		t.Fatalf("overlay should win: %+v", cfg)
	}
	if !strings.Contains(cfg.SystemPrompt, "be terse") || !strings.Contains(cfg.SystemPrompt, "support") {
		t.Fatalf("domain_prompt/channel_type from spec.Config not folded: %q", cfg.SystemPrompt)
	}
}

// --- prompt (prompt.go) ----------------------------------------------------

func TestBuildSystemPrompt_EmptyDomain(t *testing.T) {
	got := BuildSystemPrompt(Situation{Host: "server"}, "", "")
	if got == "" || !strings.Contains(got, "atoll agent") {
		t.Fatalf("empty-domain prompt missing skeleton: %q", got)
	}
}

func TestBuildSystemPrompt_SituationDrivesBehaviour(t *testing.T) {
	withWs := BuildSystemPrompt(Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: "/w"}, "", "")
	noWs := BuildSystemPrompt(Situation{Host: "server"}, "", "")
	if !strings.Contains(withWs, "/w") {
		t.Fatalf("workspace prompt missing dir")
	}
	if !strings.Contains(noWs, "NO private workspace") {
		t.Fatalf("no-workspace prompt missing bootstrap guidance")
	}
}

// --- classify --------------------------------------------------------------

func TestClassifyLLMError_NetworkBuckets(t *testing.T) {
	cases := map[error]string{
		&kimierrors.LLMError{StatusCode: 429}:            "llm_rate_limit",
		&kimierrors.LLMError{StatusCode: 401}:            "llm_auth",
		&kimierrors.LLMError{StatusCode: 500}:            "llm_server",
		&url.Error{Op: "Get", Err: errors.New("refused")}: "llm_network",
		context.DeadlineExceeded:                          "llm_network",
		errors.New("mystery"):                             "llm_unknown",
	}
	for err, want := range cases {
		if got := classifyLLMError(err); got != want {
			t.Fatalf("classify(%v) = %q, want %q", err, got, want)
		}
	}
}
