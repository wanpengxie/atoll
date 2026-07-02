package behavior

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// rejectWriter is a harness.Pen double that reports a reject reason. (It is a
// relay-only stub: it never injects identity, so envelopes it sees keep the
// builder's zero Sender/ChannelID — the real boundPen does the welding.)
type rejectWriter struct {
	reason string
	detail string
}

func (w *rejectWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{MessageID: env.ID, RejectReason: harness.HarnessRejectReason(w.reason), RejectDetail: w.detail}, nil
}

// Respond commits a final response and returns the written message id.
func TestRespond_FinalSuccess(t *testing.T) {
	w := &recordingWriter{}
	req := newRequest("r1", nil)
	id, err := Respond(context.Background(), w, fixedClock(1234), req, ResponseSpec{
		Status: "completed",
	})
	if err != nil {
		t.Fatalf("Respond err: %v", err)
	}
	if id == "" {
		t.Fatal("Respond must return the written message id")
	}
	term := w.last()
	if term == nil || term.Kind != message.KindResponse {
		t.Fatalf("expected a response envelope, got %+v", term)
	}
	if term.ParentID != "r1" {
		t.Fatalf("parent_id = %q, want r1", term.ParentID)
	}
	// Sender is welded by the pen, not the builder — the relay stub leaves it
	// zero (sealed-pen).
	if term.Sender != (message.Sender{}) {
		t.Fatalf("sender = %+v, want zero (pen-injected)", term.Sender)
	}
	if term.TS != 1234 {
		t.Fatalf("ts = %d, want 1234 from clock", term.TS)
	}
	var p struct {
		Status string `json:"status"`
	}
	if e := json.Unmarshal(term.Payload, &p); e != nil {
		t.Fatalf("payload unmarshal: %v", e)
	}
	if p.Status != "completed" {
		t.Fatalf("status = %q, want completed", p.Status)
	}
}

// An empty status defaults to "completed".
func TestRespond_EmptyStatusDefaultsCompleted(t *testing.T) {
	w := &recordingWriter{}
	id, err := Respond(context.Background(), w, fixedClock(1), newRequest("r1", nil), ResponseSpec{})
	if err != nil {
		t.Fatalf("Respond err: %v", err)
	}
	if id == "" {
		t.Fatal("want a message id")
	}
	var p struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(w.last().Payload, &p)
	if p.Status != "completed" {
		t.Fatalf("empty status must default to completed, got %q", p.Status)
	}
}

// A nil request is rejected.
func TestRespond_NilRequest(t *testing.T) {
	_, err := Respond(context.Background(), &recordingWriter{}, fixedClock(1), nil, ResponseSpec{})
	if err == nil {
		t.Fatal("nil request must error")
	}
}

// A non-final status ("processing") is rejected — Respond is final-only.
func TestRespond_NonFinalStatusRejected(t *testing.T) {
	_, err := Respond(context.Background(), &recordingWriter{}, fixedClock(1), newRequest("r1", nil), ResponseSpec{
		Status: "processing",
	})
	if err == nil {
		t.Fatal("non-final status must error")
	}
}

// A build failure (non-object payload) surfaces as an error before any write.
func TestRespond_BuildFailurePropagates(t *testing.T) {
	w := &recordingWriter{}
	_, err := Respond(context.Background(), w, fixedClock(1), newRequest("r1", nil), ResponseSpec{
		Status:  "completed",
		Payload: json.RawMessage(`5`), // non-object payload -> MergeResponsePayload errors
	})
	if err == nil {
		t.Fatal("non-object payload must error")
	}
	if w.count() != 0 {
		t.Fatalf("build failure must not write, writes=%d", w.count())
	}
}

// A writer error is wrapped and returned.
func TestRespond_WriteErrorPropagates(t *testing.T) {
	w := &recordingWriter{err: errors.New("boom")}
	_, err := Respond(context.Background(), w, fixedClock(1), newRequest("r1", nil), ResponseSpec{
		Status: "completed",
	})
	if err == nil {
		t.Fatal("writer error must propagate")
	}
}

// A terminal-duplicate reject (lost one-terminal-per-request race) returns the
// message id with no error — benign.
func TestRespond_DuplicateBenign(t *testing.T) {
	w := &recordingWriter{duplicate: true}
	id, err := Respond(context.Background(), w, fixedClock(1), newRequest("r1", nil), ResponseSpec{
		Status: "completed",
	})
	if err != nil {
		t.Fatalf("duplicate must be benign, got err %v", err)
	}
	if id == "" {
		t.Fatal("duplicate must still return the message id")
	}
}

// A RejectReason outcome surfaces as an error carrying the opaque diagnostic.
func TestRespond_RejectReasonErrors(t *testing.T) {
	w := &rejectWriter{reason: "harness_bad_audience", detail: "x"}
	_, err := Respond(context.Background(), w, fixedClock(1), newRequest("r1", nil), ResponseSpec{
		Status: "failed",
	})
	if err == nil {
		t.Fatal("a reject reason must surface as an error")
	}
}

// CollapseInternalError closes a request with a receiver_internal_error final,
// carrying the detail in the payload.
func TestCollapseInternalError_WithDetail(t *testing.T) {
	w := &recordingWriter{}
	id, err := CollapseInternalError(context.Background(), w, fixedClock(7), newRequest("r1", nil), "panic in handler")
	if err != nil {
		t.Fatalf("CollapseInternalError err: %v", err)
	}
	if id == "" {
		t.Fatal("want a message id")
	}
	var p struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
		Detail string `json:"detail"`
	}
	if e := json.Unmarshal(w.last().Payload, &p); e != nil {
		t.Fatalf("payload unmarshal: %v", e)
	}
	if p.Status != "failed" {
		t.Fatalf("status = %q, want failed", p.Status)
	}
	if p.Reason != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason = %q, want receiver_internal_error", p.Reason)
	}
	if p.Detail != "panic in handler" {
		t.Fatalf("detail = %q, want carried", p.Detail)
	}
}

// CollapseInternalError with empty detail writes no detail field but still
// closes with receiver_internal_error.
func TestCollapseInternalError_EmptyDetail(t *testing.T) {
	w := &recordingWriter{}
	_, err := CollapseInternalError(context.Background(), w, fixedClock(7), newRequest("r1", nil), "")
	if err != nil {
		t.Fatalf("CollapseInternalError err: %v", err)
	}
	var p struct {
		Reason string `json:"reason"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(w.last().Payload, &p)
	if p.Reason != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason = %q", p.Reason)
	}
	if p.Detail != "" {
		t.Fatalf("empty detail must not appear, got %q", p.Detail)
	}
}

// CollapseInternalError rejects a nil request.
func TestCollapseInternalError_NilRequest(t *testing.T) {
	_, err := CollapseInternalError(context.Background(), &recordingWriter{}, fixedClock(1), nil, "x")
	if err == nil {
		t.Fatal("nil request must error")
	}
}

// EmitEvent emits a kind=event message; the authoring identity is welded onto
// the pen (the relay stub leaves Sender/ChannelID zero on the built envelope).
func TestEmitEvent_Success(t *testing.T) {
	w := &recordingWriter{}
	id, err := EmitEvent(context.Background(), w, fixedClock(99),
		"agent.text", json.RawMessage(`{"hi":1}`), message.Visibility("channel"), message.Audience{actor.ActorID("a")})
	if err != nil {
		t.Fatalf("EmitEvent err: %v", err)
	}
	if id == "" {
		t.Fatal("want a message id")
	}
	ev := w.last()
	if ev.Kind != message.KindEvent {
		t.Fatalf("kind = %q, want event", ev.Kind)
	}
	if ev.Type != "agent.text" {
		t.Fatalf("type = %q", ev.Type)
	}
	// Sender / ChannelID are pen-injected, not builder-filled (sealed-pen).
	if ev.Sender != (message.Sender{}) {
		t.Fatalf("sender = %+v, want zero (pen-injected)", ev.Sender)
	}
	if ev.ChannelID != "" {
		t.Fatalf("channel_id = %q, want empty (pen-injected)", ev.ChannelID)
	}
	if ev.TS != 99 {
		t.Fatalf("ts = %d, want 99", ev.TS)
	}
}

// EmitEvent rejects an empty event type.
func TestEmitEvent_EmptyType(t *testing.T) {
	_, err := EmitEvent(context.Background(), &recordingWriter{}, fixedClock(1),
		"", nil, message.Visibility("channel"), nil)
	if err == nil {
		t.Fatal("empty type must error")
	}
}

// EmitEvent surfaces a writer error.
func TestEmitEvent_WriteError(t *testing.T) {
	w := &recordingWriter{err: errors.New("boom")}
	_, err := EmitEvent(context.Background(), w, fixedClock(1),
		"agent.text", nil, message.Visibility("channel"), nil)
	if err == nil {
		t.Fatal("writer error must propagate")
	}
}

// EmitEvent surfaces a reject reason as an error.
func TestEmitEvent_RejectReason(t *testing.T) {
	w := &rejectWriter{reason: "harness_bad_visibility"}
	_, err := EmitEvent(context.Background(), w, fixedClock(1),
		"agent.text", nil, message.Visibility("channel"), nil)
	if err == nil {
		t.Fatal("reject reason must surface as an error")
	}
}
