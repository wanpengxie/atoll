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

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type recordSink struct {
	started  []toolActivity
	ended    []toolActivity
	complete []finalValue
	failures []failure
}

func (s *recordSink) ToolStarted(a toolActivity) error {
	s.started = append(s.started, a)
	return nil
}
func (s *recordSink) ToolEnded(a toolActivity) error { s.ended = append(s.ended, a); return nil }
func (s *recordSink) Complete(v finalValue) error {
	s.complete = append(s.complete, v)
	return nil
}
func (s *recordSink) Fail(f failure) error { s.failures = append(s.failures, f); return nil }

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
		wire.TextDelta{Delta: "must be discarded"},
		wire.TurnEnd{StopReason: "end_turn", Output: types.ContentParts{types.TextPart{Text: "hello there"}}},
	}, nil)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-1"), CorrelationID: "corr-1", Index: 1}
	if err := e.runTurn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if len(sink.complete) != 1 {
		t.Fatalf("want 1 terminal value, got %d: %+v", len(sink.complete), sink.complete)
	}
	o := sink.complete[0]
	if o.Text != "hello there" || o.NextAction != "done" {
		t.Fatalf("terminal = %+v", o)
	}
}

func TestKimiTurn_TypedToolPhases(t *testing.T) {
	e, _ := newTestEngine([]wire.WireMessage{
		wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t1", Name: "call_actor", Arguments: map[string]any{"cmd": "ls"}}},
		wire.ToolCallResult{Result: types.ToolResult{ToolCallID: "t1", Name: "call_actor"}},
		wire.ToolCallRequest{ToolCall: types.ToolCall{ID: "t2", Name: "call_actor"}},
		wire.ToolCallResult{Result: types.ToolResult{ToolCallID: "t2", Name: "call_actor"}},
		wire.TextDelta{Delta: "discarded delta"},
		wire.TurnEnd{StopReason: "end_turn", Output: types.ContentParts{types.TextPart{Text: "final answer"}}},
	}, nil)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-p"), Index: 1}
	if err := e.runTurn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if len(sink.started) != 2 || len(sink.ended) != 2 || len(sink.complete) != 1 {
		t.Fatalf("phases started=%v ended=%v complete=%v", sink.started, sink.ended, sink.complete)
	}
	for i := 0; i < 2; i++ {
		wantID := []string{"t1", "t2"}[i]
		if sink.started[i].CallID != wantID || sink.ended[i].CallID != wantID {
			t.Fatalf("tool phase %d ids = %q/%q", i, sink.started[i].CallID, sink.ended[i].CallID)
		}
	}
	if sink.complete[0].Text != "final answer" {
		t.Fatalf("terminal value = %+v", sink.complete[0])
	}
}

// TestKimiTurn_LLMErrorFailedTerminal pins an Agent.Run error → failed terminal
// Output (Turn returns nil, actor stays alive).
func TestKimiTurn_LLMErrorFailedTerminal(t *testing.T) {
	llmErr := &kimierrors.LLMError{StatusCode: 429, Cause: errors.New("rate limited")}
	e, _ := newTestEngine(nil, llmErr)
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("req-err"), Index: 1}
	if err := e.runTurn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn should stay alive on an LLM error: %v", err)
	}
	if len(sink.failures) != 1 {
		t.Fatalf("want 1 failed terminal, got %d", len(sink.failures))
	}
	if sink.failures[0].ErrorCode != "llm_rate_limit" {
		t.Fatalf("failed terminal = %+v", sink.failures[0])
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
		&kimierrors.LLMError{StatusCode: 429}:             "llm_rate_limit",
		&kimierrors.LLMError{StatusCode: 401}:             "llm_auth",
		&kimierrors.LLMError{StatusCode: 500}:             "llm_server",
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
