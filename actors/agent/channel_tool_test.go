package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// stubWriter is an in-package harness.Writer double.
type stubWriter struct {
	mu      sync.Mutex
	written []message.Envelope
	err     error
	reject  harness.HarnessRejectReason
}

func (w *stubWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return harness.WriteResult{}, w.err
	}
	if w.reject != "" {
		return harness.WriteResult{RejectReason: w.reject}, nil
	}
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func newToolTestBridge(t *testing.T, w *stubWriter) *Bridge {
	t.Helper()
	b, err := NewBridge(Config{APIKey: "k", Model: "m"}, "agent:tt", "ch-tt", w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	// Tool tests drive submit/await directly — no LLM loop needed, but the
	// caller (author#2) must exist for Arm.
	b.caller = nil // armCaller tolerates nil; Arm coverage lives in bridge_test
	return b
}

func TestSubmitChannelRequest_WriteErrorUnwindsFuture(t *testing.T) {
	w := &stubWriter{err: errors.New("link down")}
	b := newToolTestBridge(t, w)

	env := message.Envelope{ID: "req-x", ChannelID: "ch-tt", Kind: message.KindRequest}
	if err := b.submitChannelRequest(context.Background(), env, true); err == nil {
		t.Fatal("want write error")
	}
	if b.futures.Registered("req-x") {
		t.Fatal("future leaked after write failure")
	}
}

func TestSubmitChannelRequest_RejectUnwindsFuture(t *testing.T) {
	w := &stubWriter{reject: harness.HarnessRejectReason("harness_audience_invalid")}
	b := newToolTestBridge(t, w)

	env := message.Envelope{ID: "req-r", ChannelID: "ch-tt", Kind: message.KindRequest}
	if err := b.submitChannelRequest(context.Background(), env, true); err == nil {
		t.Fatal("want reject error")
	}
	if b.futures.Registered("req-r") {
		t.Fatal("future leaked after reject")
	}
}

func TestExecuteChannelRequest_FanOutRegistersFuture(t *testing.T) {
	w := &stubWriter{}
	b := newToolTestBridge(t, w)

	item := turnItem{env: message.Envelope{ID: "trig", ChannelID: "ch-tt"}}
	res := b.executeChannelRequest(context.Background(), item, metatool.RequestSpec{
		ToolName: "call_actor", EnvelopeType: "x.y", HandlerActorID: "tool:t",
		WaitMode: metatool.WaitNone,
	})
	m, _ := res.Value.Value.(map[string]any)
	if m["status"] != "accepted" {
		t.Fatalf("fan-out want ack, got %+v", res.Value.Value)
	}
	reqID := message.ID(fmt.Sprint(m["request_id"]))
	if !b.futures.Registered(reqID) {
		t.Fatal("fan-out future not registered")
	}
}

func TestEnqueueTurn_OverflowEvictsOldestAndNotes(t *testing.T) {
	w := &stubWriter{}
	b := newToolTestBridge(t, w)
	b.turnQ = make(chan turnItem, 2)

	for i := 0; i < 3; i++ {
		b.enqueueTurn(message.Envelope{
			ID: message.ID(fmt.Sprintf("e%d", i)), ChannelID: "ch-tt", Type: "human.text",
			Sender: message.Sender{Kind: actor.KindHuman, ID: "u"},
		})
	}
	// Oldest (e0) evicted; queue holds e1, e2.
	first := <-b.turnQ
	second := <-b.turnQ
	if first.env.ID != "e1" || second.env.ID != "e2" {
		t.Fatalf("queue order after eviction: %s, %s", first.env.ID, second.env.ID)
	}
	w.mu.Lock()
	noted := len(w.written)
	w.mu.Unlock()
	if noted != 1 {
		t.Fatalf("want 1 overflow note, got %d", noted)
	}
}

// TestExecuteChannelRequest_TimeoutArmsAuthor2Terminal pins the author#2
// closure with a tiny deadline: a request nobody answers gets an
// unanswered_timeout terminal WRITTEN BY THE CALLER'S OWN TIMER — the first
// production-grade exercise of behavior.Caller.
func TestExecuteChannelRequest_TimeoutArmsAuthor2Terminal(t *testing.T) {
	w := &stubWriter{}
	b := newToolTestBridge(t, w)
	b.caller = behavior.NewCaller(b.sender(), w, b.clock, nil)

	item := turnItem{env: message.Envelope{ID: "trig-t", ChannelID: "ch-tt"}}
	res := b.executeChannelRequest(context.Background(), item, metatool.RequestSpec{
		ToolName: "call_actor", EnvelopeType: "dead.op", HandlerActorID: "tool:dead",
		WaitMode: metatool.WaitNone, Timeout: 50 * time.Millisecond,
	})
	m, _ := res.Value.Value.(map[string]any)
	if m["status"] != "accepted" {
		t.Fatalf("want ack, got %+v", res.Value.Value)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		n := len(w.written)
		var last message.Envelope
		if n > 0 {
			last = w.written[n-1]
		}
		w.mu.Unlock()
		if n >= 2 {
			if last.Kind != message.KindResponse {
				t.Fatalf("author#2 terminal kind: %+v", last)
			}
			var p map[string]any
			_ = json.Unmarshal(last.Payload, &p)
			if p["status"] != "failed" || p["reason"] != string(message.TerminalUnansweredTimeout) {
				t.Fatalf("terminal payload: %+v", p)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("author#2 terminal never written (%d envelopes)", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestExtractRuntimeContext_OutsideTurn(t *testing.T) {
	rc := extractRuntimeContext(context.Background())
	if rc.InTurn() {
		t.Fatal("zero context must not count as in-turn")
	}
}

func TestToResultValue_WrapsNonMap(t *testing.T) {
	rv := toResultValue(types.ToolResult{Name: "x", Value: types.ToolReturnValue{Value: "plain"}})
	if rv.Value["result"] != "plain" {
		t.Fatalf("non-map wrap: %+v", rv.Value)
	}
}
