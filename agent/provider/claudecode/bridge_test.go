package claudecode_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/ActOS/agent/provider/claudecode"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const (
	testChannelID = channel.ID("ch-test")
	testActorID   = actor.ActorID("agent:T")
)

// recordingWriter is a concurrency-safe harness.Pen double.
type recordingWriter struct {
	mu      sync.Mutex
	written []message.Envelope
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) waitWritten(t *testing.T, n int, timeout time.Duration) []message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		w.mu.Lock()
		got := make([]message.Envelope, len(w.written))
		copy(got, w.written)
		w.mu.Unlock()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// scriptedClient is the claudeClient test double: it replays a canned message
// sequence (ending in a *ResultMessage) on every ReceiveResponse, with no `claude`
// CLI process.
type scriptedClient struct {
	msgs     []claude.Message
	queryErr error

	mu         sync.Mutex
	lastInput  string
	queryCount int
}

func (c *scriptedClient) Connect(context.Context) error { return nil }

func (c *scriptedClient) Query(_ context.Context, prompt string) error {
	c.mu.Lock()
	c.lastInput = prompt
	c.queryCount++
	c.mu.Unlock()
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

func (c *scriptedClient) Interrupt(context.Context) error { return nil }
func (c *scriptedClient) Close() error                    { return nil }

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

// TestClaudeTurn_EmitsFinalAndCheckpoints drives one turn through the stubbed
// engine and asserts the bridge emits a public agent.text terminal addressed to
// the trigger sender, AND checkpoints the claude session id into the state slot
// — the second looper on the same lib contract as go-kimi.
func TestClaudeTurn_EmitsFinalAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}
	ckCh := make(chan string, 1)
	cfg := claudecode.Config{
		Model:      "m",
		Checkpoint: func(b json.RawMessage) error { ckCh <- string(b); return nil },
	}
	b, err := claudecode.NewBridge(cfg, testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	sc := &scriptedClient{msgs: []claude.Message{
		&claude.AssistantMessage{Content: []claude.ContentBlock{
			&claude.TextBlock{Text: "hello "},
			&claude.TextBlock{Text: "world"},
		}},
		&claude.ResultMessage{SessionID: "sess-123", Result: "hello world"},
	}}
	claudecode.SetClientFactory(b, func() (claudecode.ClaudeClient, error) { return sc, nil })
	if err := b.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(ctx) })

	env := triggerEnv("e1")
	if err := b.Receive(ctx, &env); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	got := w.waitWritten(t, 1, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("want 1 emitted envelope, got %d", len(got))
	}
	final := got[0]
	if final.Type != "agent.text" || final.Visibility != message.VisibilityPublic {
		t.Fatalf("terminal envelope = %s/%s", final.Type, final.Visibility)
	}
	var payload struct {
		Text       string `json:"text"`
		NextAction string `json:"next_action"`
	}
	_ = json.Unmarshal(final.Payload, &payload)
	if payload.Text != "hello world" || payload.NextAction != "done" {
		t.Fatalf("payload = %+v", payload)
	}
	if len(final.Audience) != 1 || final.Audience[0] != actor.ActorID("user-A") {
		t.Fatalf("audience = %v (want [user-A])", final.Audience)
	}

	select {
	case ck := <-ckCh:
		if ck != "sess-123" {
			t.Fatalf("checkpoint = %q (want sess-123)", ck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session never checkpointed into the state slot")
	}
}

// TestClaudeMCP_BridgesAllMetaTools pins that the claude engine gets the FULL
// coagent meta-tool surface (agent-spec §三 必须项 #1) via the in-process MCP
// server — the same 7 tools the go-kimi looper installs as AdditionalTools.
func TestClaudeMCP_BridgesAllMetaTools(t *testing.T) {
	w := &recordingWriter{}
	b, err := claudecode.NewBridge(claudecode.Config{Model: "m"}, testActorID, testChannelID, w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	names := claudecode.MCPToolNames(b)
	if len(names) != len(metatool.MetaTools()) {
		t.Fatalf("MCP wired %d tools, want %d", len(names), len(metatool.MetaTools()))
	}
	want := map[string]bool{"call_actor": false}
	for _, n := range names {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	if !want["call_actor"] {
		t.Fatalf("call_actor not bridged into the claude tool surface; got %v", names)
	}
}

// TestClaudeConfigOverlay pins env-default + per-instance overlay (agent-spec §三).
func TestClaudeConfigOverlay(t *testing.T) {
	t.Setenv(claudecode.EnvKeyModel, "env-model")
	cfg, err := claudecode.NewConfigFromSpec(nil, "sys")
	if err != nil {
		t.Fatalf("empty spec: %v", err)
	}
	if cfg.Model != "env-model" {
		t.Fatalf("empty spec should ride env: %q", cfg.Model)
	}
	cfg, err = claudecode.NewConfigFromSpec(json.RawMessage(`{"model":"spec-model"}`), "sys")
	if err != nil {
		t.Fatalf("overlay: %v", err)
	}
	if cfg.Model != "spec-model" {
		t.Fatalf("overlay should win: %q", cfg.Model)
	}
	t.Setenv(claudecode.EnvKeyModel, "")
	cfg, err = claudecode.NewConfigFromSpec(nil, "sys")
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if cfg.Model == "" {
		t.Fatal("a default model should be filled when neither env nor spec set one")
	}
}
