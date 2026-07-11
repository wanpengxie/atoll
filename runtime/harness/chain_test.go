package harness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
)

// newTestChain returns the package-internal bare chain. Tests drive the chain
// directly (steps 0..8 + append) for step-isolation: they set the caller via
// ctxWithCaller and pre-fill envelope.sender.id / channel_id, which the welded
// boundPen would otherwise own. The pen's fail-fast injection is exercised
// separately (platform emit-identity tests).
func newTestChain(t *testing.T, cs *store.ChannelStores) *chain {
	t.Helper()
	m, err := New(testDeps(t, cs))
	if err != nil {
		t.Fatalf("New chain: %v", err)
	}
	return m.(*minter).chain
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
	author := registerActor(t, cs, actor.ActorID("agent:p"), actor.KindAgent)
	c := newTestChain(t, cs)

	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: author}, Kind: message.KindEvent, Type: "agent.text",
		Audience: message.Audience{"x"},
	}
	res, err := c.write(ctxCallerKind(author, actor.KindAgent), e)
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
			res, err := c.write(tc.ctx, e)
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
	if _, err := c.write(context.Background(), nil); err == nil {
		t.Fatalf("nil envelope should error")
	}
}

// End-to-end request → final response closure, carrying is_terminal through to
// the store. Then a SECOND final must reject (terminal uniqueness, end to end).
func TestChain_RequestThenFinalResponseClosure(t *testing.T) {
	cs := newTestStore(t)
	callerID := registerActor(t, cs, actor.ActorID("agent:caller"), actor.KindAgent)
	toolID := registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)
	c := newTestChain(t, cs)

	// 1. caller sends a request to tool:xhs.
	req := &message.Envelope{
		ID: "req1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: callerID}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{toolID}, Payload: json.RawMessage(`{}`),
	}
	if res, err := c.write(ctxCallerKind(callerID, actor.KindAgent), req); err != nil || !res.Accepted() {
		t.Fatalf("request write: err=%v reason=%q", err, res.RejectReason)
	}

	// 2. tool:xhs answers final completed.
	resp := &message.Envelope{
		ID: "resp1", TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: toolID}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{callerID},
		Payload: json.RawMessage(`{"status":"completed"}`),
	}
	res, err := c.write(ctxCallerKind(toolID, actor.KindTool), resp)
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
		Sender: message.Sender{ID: toolID}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{callerID},
		Payload: json.RawMessage(`{"status":"failed","reason":"receiver_internal_error"}`),
	}
	res2, err := c.write(ctxCallerKind(toolID, actor.KindTool), resp2)
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
	author := registerActor(t, cs, actor.ActorID("agent:p"), actor.KindAgent)
	c := newTestChain(t, cs)

	mk := func() *message.Envelope {
		return &message.Envelope{
			ID: "dup", TS: fixedNowMs - 1000, ChannelID: testChannelID,
			Sender: message.Sender{ID: author}, Kind: message.KindEvent, Type: "agent.text",
			Audience: message.Audience{"x"},
		}
	}
	if res, err := c.write(ctxCallerKind(author, actor.KindAgent), mk()); err != nil || !res.Accepted() {
		t.Fatalf("first write: err=%v reason=%q", err, res.RejectReason)
	}
	res, err := c.write(ctxCallerKind(author, actor.KindAgent), mk())
	if err != nil {
		t.Fatalf("second write err: %v", err)
	}
	if res.Accepted() {
		t.Fatalf("duplicate id should not produce a second durable row")
	}
}
