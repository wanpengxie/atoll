package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// newTestChain returns the package-internal bare chain. Tests drive the chain
// directly (steps 0..8 + append) for step-isolation: they set the caller via
// ctxWithCaller and pre-fill envelope.sender.id / channel_id, which the welded
// boundPen would otherwise own. The pen's fail-fast injection is exercised
// separately (platform emit-identity tests).
func newTestChain(t *testing.T, cs *store.ChannelStores) *chain {
	t.Helper()
	m, _, err := New(testDeps(t, cs))
	if err != nil {
		t.Fatalf("New chain: %v", err)
	}
	return m.(*minter).chain
}

// New rejects incomplete Deps (substrate refuses to assemble half-wired).
func TestChain_NewValidatesDeps(t *testing.T) {
	if _, _, err := New(Deps{}); err == nil {
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
		Audience: nil, CorrelationID: "m1",
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
	if row.Envelope.Audience == nil || len(row.Envelope.Audience) != 0 {
		t.Fatalf("durable audience = %#v, want non-nil []", row.Envelope.Audience)
	}
	wire, err := json.Marshal(row.Envelope)
	if err != nil || !bytes.Contains(wire, []byte(`"audience":[]`)) {
		t.Fatalf("durable wire audience must be []: %s err=%v", wire, err)
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
				Audience: message.Audience{"x"}, CorrelationID: "m1",
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
		Audience: message.Audience{toolID}, Payload: json.RawMessage(`{"body":{}}`),
		CorrelationID: "req1",
	}
	if res, err := c.write(ctxCallerKind(callerID, actor.KindAgent), req); err != nil || !res.Accepted() {
		t.Fatalf("request write: err=%v reason=%q", err, res.RejectReason)
	}

	// 2. tool:xhs answers final completed.
	resp := &message.Envelope{
		ID: "resp1", TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: toolID}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{callerID},
		Payload: json.RawMessage(`{"status":"completed"}`), CorrelationID: "req1",
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
		CorrelationID: "req1",
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
			Audience: message.Audience{"x"}, CorrelationID: "dup",
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

// stepName falls back to a numbered label for ids outside the known table.
func TestStepName_DefaultUnknownID(t *testing.T) {
	if got := stepName(stepID(99)); got != "step_99" {
		t.Fatalf("stepName(99) = %q, want step_99", got)
	}
}

// chainWith builds the internal chain with stub deps.
func chainWith(t *testing.T, lg storespec.MessageLog) *chain {
	t.Helper()
	m, _, err := New(Deps{
		ChannelID: testChannelID,
		Log:       lg,
		Presence:  testAuthority{},
		NowMs:     func() int64 { return fixedNowMs },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m.(*minter).chain
}

// A step that returns a hard error (not a reject) → Write maps it through
// observeError (chain.go step-error path) and returns the wrapped error. We
// trigger this at StepResponsePairing by making Log.FindByID fail for the
// parent lookup — StepSenderConsistent no longer has an error-producing seam
// of its own (no registry lookup left: identity is pen-welded + Admit()-gated
// by the pen-held authority, not registry-checked).
func TestWrite_StepError_ObservedAndReturned(t *testing.T) {
	findErr := errors.New("boom-find")
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, findErr },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	resp := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	res, err := c.write(ctxCallerKind("tool:xhs", actor.KindTool), resp)
	if err == nil {
		t.Fatalf("expected wrapped step error, got nil (res=%+v)", res)
	}
	if !errors.Is(err, findErr) {
		t.Fatalf("error = %v, want wrapping %v", err, findErr)
	}
}

// Append returns a typed *storespec.AppendError → mapped to a closed-set
// reject via observeReject.
func TestWrite_AppendTypedError_MapsToReject(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, &storespec.AppendError{
				Reason:           string(HarnessTerminalDuplicate),
				Detail:           "store says dup",
				PartialMessageID: "m-partial",
			}
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-agent", "agent:p"))
	if err != nil {
		t.Fatalf("typed AppendError should be a reject, not error: %v", err)
	}
	if res.Accepted() || res.RejectReason != HarnessTerminalDuplicate {
		t.Fatalf("reject = %q accepted=%v, want terminal_duplicate", res.RejectReason, res.Accepted())
	}
	if res.MessageID != "m-partial" {
		t.Fatalf("MessageID = %q, want m-partial", res.MessageID)
	}
}

// Append returns a PLAIN error (not *AppendError) → Write wraps it and
// observeError logs it.
func TestWrite_AppendPlainError_WrappedAsError(t *testing.T) {
	appendErr := errors.New("disk on fire")
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, appendErr
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	_, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-plain", "agent:p"))
	if err == nil || !errors.Is(err, appendErr) {
		t.Fatalf("plain append error = %v, want wrapping %v", err, appendErr)
	}
}

// A step that panics → Write's deferred recover converts it to an error. We
// make Log.FindByID panic at StepResponsePairing (StepSenderConsistent no
// longer has a seam to panic through — no registry lookup left).
func TestWrite_PanicRecovered(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID: func(context.Context, message.ID) (*storespec.StoredRow, bool, error) {
			panic("step blew up")
		},
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	resp := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	_, err := c.write(ctxCallerKind("tool:xhs", actor.KindTool), resp)
	if err == nil {
		t.Fatalf("panic should be recovered into an error")
	}
}

// observeReject's reason=="" early-return is reachable only through the
// engine-append path: a typed *AppendError carrying an EMPTY Reason makes
// Chain.Write call observeReject with reason "".
func TestWrite_AppendEmptyReason_ObserveRejectNoOp(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, &storespec.AppendError{Reason: "", Detail: "no reason"}
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-empty", "agent:p"))
	if err != nil {
		t.Fatalf("empty-reason AppendError should map to a WriteResult, got err=%v", err)
	}
	// An empty Reason produces a WriteResult with empty RejectReason, so
	// Accepted() is degenerately true — but the partial message id is carried
	// and observeReject returns immediately for an empty reason.
	if res.MessageID != "" {
		t.Fatalf("empty-reason result MessageID = %q, want empty (no PartialMessageID set)", res.MessageID)
	}
}
