package harness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/internal/store"
)

func newTestChain(t *testing.T, cs *store.ChannelStores) *Chain {
	t.Helper()
	c, err := New(testDeps(t, cs))
	if err != nil {
		t.Fatalf("New chain: %v", err)
	}
	return c
}

// New rejects incomplete Deps (substrate refuses to assemble half-wired).
func TestChain_NewValidatesDeps(t *testing.T) {
	if _, err := New(Deps{}); err == nil {
		t.Fatalf("New with empty Deps should error")
	}
}

// Full accept path: a well-formed event writes a durable row and returns a seq.
func TestChain_WriteAcceptsEventDurably(t *testing.T) {
	cs := newTestStore(t)
	registerActor(t, cs, actor.ActorID("agent:p"), actor.KindAgent)
	c := newTestChain(t, cs)

	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindEvent, Type: "agent.text",
		Audience: message.Audience{"x"},
	}
	res, err := c.Write(ctxCaller("agent:p"), e)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("rejected: %q %q", res.RejectReason, res.RejectDetail)
	}
	if res.MessageID != "m1" {
		t.Fatalf("MessageID = %q, want m1", res.MessageID)
	}
	// Durable: row is queryable.
	row, ok, err := cs.Log.FindByID(context.Background(), "m1")
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v — row should be durable", ok, err)
	}
	// Engine filled ts_received + normalize filled visibility.
	if row.Envelope.TSReceived != fixedNowMs {
		t.Fatalf("ts_received = %d, want engine clock %d", row.Envelope.TSReceived, fixedNowMs)
	}
	if row.Envelope.Visibility != message.VisibilityPublic {
		t.Fatalf("visibility = %q, want public default", row.Envelope.Visibility)
	}
}

// Short-circuit: the FIRST failing step's reason is returned and no row lands.
func TestChain_WriteShortCircuitsOnFirstReject(t *testing.T) {
	cs := newTestStore(t)
	c := newTestChain(t, cs)

	tests := []struct {
		name   string
		ctx    context.Context
		mutate func(e *message.Envelope)
		reason HarnessRejectReason
	}{
		{
			name:   "step0 caller missing wins over later shape issues",
			ctx:    context.Background(), // no caller
			mutate: func(e *message.Envelope) { e.Kind = "" /* would also fail shape */ },
			reason: HarnessEngineACLDenied,
		},
		{
			name:   "step1 channel mismatch",
			ctx:    ctxCaller("agent:p"),
			mutate: func(e *message.Envelope) { e.ChannelID = "foreign" },
			reason: HarnessChannelMismatch,
		},
		{
			name:   "step4 sender mismatch",
			ctx:    ctxCaller("agent:p"),
			mutate: func(e *message.Envelope) { e.Sender = message.Sender{ID: "agent:other"} },
			reason: HarnessSenderMismatch,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := &message.Envelope{
				ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
				Sender: message.Sender{ID: "agent:p"}, Kind: message.KindEvent, Type: "agent.text",
				Audience: message.Audience{"x"},
			}
			tc.mutate(e)
			res, err := c.Write(tc.ctx, e)
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if res.Accepted() {
				t.Fatalf("expected reject %q, got accept", tc.reason)
			}
			if res.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q", res.RejectReason, tc.reason)
			}
			// No durable row on reject.
			if _, ok, _ := cs.Log.FindByID(context.Background(), "m1"); ok {
				t.Fatalf("rejected write left a durable row")
			}
		})
	}
}

// nil envelope is a hard error (not a reject).
func TestChain_WriteNilEnvelope(t *testing.T) {
	cs := newTestStore(t)
	c := newTestChain(t, cs)
	if _, err := c.Write(context.Background(), nil); err == nil {
		t.Fatalf("nil envelope should error")
	}
}

// End-to-end request → final response closure, carrying is_terminal through to
// the store. Then a SECOND final must reject (terminal uniqueness, end to end).
func TestChain_RequestThenFinalResponseClosure(t *testing.T) {
	cs := newTestStore(t)
	registerActor(t, cs, actor.ActorID("agent:caller"), actor.KindAgent)
	registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)
	c := newTestChain(t, cs)

	// 1. caller sends a request to tool:xhs.
	req := &message.Envelope{
		ID: "req1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:caller"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:xhs"}, Payload: json.RawMessage(`{}`),
	}
	if res, err := c.Write(ctxCaller("agent:caller"), req); err != nil || !res.Accepted() {
		t.Fatalf("request write: err=%v reason=%q", err, res.RejectReason)
	}

	// 2. tool:xhs answers final completed.
	resp := &message.Envelope{
		ID: "resp1", TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: "tool:xhs"}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{"agent:caller"},
		Payload: json.RawMessage(`{"status":"completed"}`),
	}
	res, err := c.Write(ctxCaller("tool:xhs"), resp)
	if err != nil || !res.Accepted() {
		t.Fatalf("response write: err=%v reason=%q", err, res.RejectReason)
	}
	// is_terminal must be persisted true.
	row, ok, err := cs.Log.FindByID(context.Background(), "resp1")
	if err != nil || !ok {
		t.Fatalf("response row missing: ok=%v err=%v", ok, err)
	}
	if !row.IsTerminal {
		t.Fatalf("is_terminal = false, want true for final response")
	}

	// 3. a SECOND final response must be rejected (terminal uniqueness).
	resp2 := &message.Envelope{
		ID: "resp2", TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: "tool:xhs"}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{"agent:caller"},
		Payload: json.RawMessage(`{"status":"failed","reason":"receiver_internal_error"}`),
	}
	res2, err := c.Write(ctxCaller("tool:xhs"), resp2)
	if err != nil {
		t.Fatalf("second response write err: %v", err)
	}
	if res2.Accepted() || res2.RejectReason != HarnessTerminalDuplicate {
		t.Fatalf("second final reason = %q accepted=%v, want terminal_duplicate", res2.RejectReason, res2.Accepted())
	}
}

// A duplicate envelope.id is a pure integrity reject surfaced at engine append.
func TestChain_DuplicateEnvelopeIDRejectsAtAppend(t *testing.T) {
	cs := newTestStore(t)
	registerActor(t, cs, actor.ActorID("agent:p"), actor.KindAgent)
	c := newTestChain(t, cs)

	mk := func() *message.Envelope {
		return &message.Envelope{
			ID: "dup", TS: fixedNowMs - 1000, ChannelID: testChannelID,
			Sender: message.Sender{ID: "agent:p"}, Kind: message.KindEvent, Type: "agent.text",
			Audience: message.Audience{"x"},
		}
	}
	if res, err := c.Write(ctxCaller("agent:p"), mk()); err != nil || !res.Accepted() {
		t.Fatalf("first write: err=%v reason=%q", err, res.RejectReason)
	}
	res, err := c.Write(ctxCaller("agent:p"), mk())
	if err != nil {
		t.Fatalf("second write err: %v", err)
	}
	if res.Accepted() {
		t.Fatalf("duplicate id should not produce a second durable row")
	}
}
