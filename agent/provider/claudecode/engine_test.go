package claudecode

// Internal test (package claudecode) so it can drive the unexported engine
// directly — the §1 三件套 unit seam. The engine is the ONE thing this provider
// writes; the mailbox loop / emit / describe dispatch live in agent/base and are
// tested there.

import (
	"context"
	"encoding/json"
	"testing"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/agent/base"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// recordSink captures the Outputs a turn emits.
type recordSink struct{ outputs []base.Output }

func (s *recordSink) Emit(o base.Output) error {
	s.outputs = append(s.outputs, o)
	return nil
}

// scriptedClient replays a canned message sequence (ending in *ResultMessage)
// with no `claude` CLI process.
type scriptedClient struct {
	msgs      []claude.Message
	queryErr  error
	lastInput string
}

func (c *scriptedClient) Connect(context.Context) error { return nil }
func (c *scriptedClient) Query(_ context.Context, prompt string) error {
	c.lastInput = prompt
	return c.queryErr
}
func (c *scriptedClient) ReceiveResponse(context.Context) <-chan claude.Message {
	ch := make(chan claude.Message, len(c.msgs))
	for _, m := range c.msgs {
		ch <- m
	}
	close(ch)
	return ch
}
func (c *scriptedClient) Close() error { return nil }

func triggerEnv(id string) message.Envelope {
	body, _ := json.Marshal(map[string]string{"text": "hi"})
	return message.Envelope{
		ID:            message.ID(id),
		Type:          "human.text",
		Visibility:    message.VisibilityPublic,
		Sender:        message.Sender{Kind: actor.KindHuman, ID: "user-A"},
		Kind:          message.KindRequest,
		Payload:       body,
		CorrelationID: "corr-1",
	}
}

// TestClaudeTurn_EmitsFinal drives one turn and asserts the engine emits a
// terminal Output (Final, next_action=done) with the model text.
func TestClaudeTurn_EmitsFinal(t *testing.T) {
	e := &engine{cfg: Config{Model: "m"}}
	e.client = &scriptedClient{msgs: []claude.Message{
		&claude.AssistantMessage{Content: []claude.ContentBlock{
			&claude.TextBlock{Text: "hello "},
			&claude.TextBlock{Text: "world"},
		}},
		&claude.ResultMessage{SessionID: "sess-123", Result: "hello world"},
	}}
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("e1"), CorrelationID: "corr-1", Index: 1}
	if err := e.Turn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if len(sink.outputs) != 1 {
		t.Fatalf("want 1 output, got %d", len(sink.outputs))
	}
	o := sink.outputs[0]
	if !o.Final || o.Text != "hello world" || o.NextAction != "done" {
		t.Fatalf("output = %+v", o)
	}
	// Session id captured → Checkpoint returns it EVERY turn the session is set
	// (no dirty micro-opt: re-returning the unchanged seed is an idempotent
	// zero-cost upsert that makes the base's persist self-healing — P1-2).
	if cp := e.Checkpoint(); string(cp) != "sess-123" {
		t.Fatalf("checkpoint = %q, want sess-123", cp)
	}
	if cp := e.Checkpoint(); string(cp) != "sess-123" {
		t.Fatalf("second checkpoint should still return the seed (self-healing, no dirty gate), got %q", cp)
	}
}

// TestClaudeTurn_ResultErrorSurfacesFailed pins an engine error → failed
// terminal Output (actor stays alive, Turn returns nil).
func TestClaudeTurn_ResultErrorSurfacesFailed(t *testing.T) {
	e := &engine{cfg: Config{Model: "m"}}
	e.client = &scriptedClient{msgs: []claude.Message{
		&claude.ResultMessage{IsError: true, Result: "boom"},
	}}
	sink := &recordSink{}
	tr := base.Trigger{Envelope: triggerEnv("e2"), Index: 1}
	if err := e.Turn(context.Background(), tr, sink); err != nil {
		t.Fatalf("Turn should not return an error for an engine failure: %v", err)
	}
	if len(sink.outputs) != 1 || !sink.outputs[0].Final || sink.outputs[0].NextAction != "failed" {
		t.Fatalf("expected a failed terminal, got %+v", sink.outputs)
	}
}

// TestClaudeMCP_BridgesAllMetaTools pins that the claude engine gets the FULL
// atoll meta-tool surface via the in-process MCP server.
func TestClaudeMCP_BridgesAllMetaTools(t *testing.T) {
	e := &engine{cfg: Config{Model: "m"}}
	srv := e.buildMCPServer()
	if srv == nil || srv.Instance == nil {
		t.Fatal("nil MCP server")
	}
	if len(srv.Instance.Tools) != len(metatool.MetaTools()) {
		t.Fatalf("MCP wired %d tools, want %d", len(srv.Instance.Tools), len(metatool.MetaTools()))
	}
	found := false
	for _, tl := range srv.Instance.Tools {
		if tl.Name == "call_actor" {
			found = true
		}
	}
	if !found {
		t.Fatal("call_actor not bridged into the claude tool surface")
	}
}

// TestClaudeConfigOverlay pins env-default + per-instance overlay + A3 domain
// prompt from InstanceSpec.Config.
func TestClaudeConfigOverlay(t *testing.T) {
	sit := Situation{Host: "server"}
	t.Setenv(EnvKeyModel, "env-model")
	cfg, err := NewConfigFromSpec(nil, sit)
	if err != nil {
		t.Fatalf("empty spec: %v", err)
	}
	if cfg.Model != "env-model" {
		t.Fatalf("empty spec should ride env: %q", cfg.Model)
	}
	cfg, err = NewConfigFromSpec(json.RawMessage(`{"model":"spec-model","domain_prompt":"be terse"}`), sit)
	if err != nil {
		t.Fatalf("overlay: %v", err)
	}
	if cfg.Model != "spec-model" {
		t.Fatalf("overlay should win: %q", cfg.Model)
	}
	if !contains(cfg.SystemPrompt, "be terse") {
		t.Fatalf("domain_prompt from spec.Config not folded into the system prompt: %q", cfg.SystemPrompt)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
