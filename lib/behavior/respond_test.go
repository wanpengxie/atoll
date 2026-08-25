package behavior

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func TestProgressAcceptsOnlyCoreStatus(t *testing.T) {
	for _, status := range []string{message.StatusQueued, message.StatusProcessing} {
		w := &recordingWriter{}
		if _, err := Progress(context.Background(), w, fixedClock(1), newRequest("r1", nil), status, map[string]any{"ok": true}); err != nil {
			t.Fatalf("core status %q rejected: %v", status, err)
		}
		if w.count() != 1 {
			t.Fatalf("core status %q wrote %d envelopes", status, w.count())
		}
	}
	for _, status := range []string{"", message.StatusCompleted, message.StatusFailed, "provider-specific"} {
		w := &recordingWriter{}
		if _, err := Progress(context.Background(), w, fixedClock(1), newRequest("r1", nil), status, nil); err == nil {
			t.Fatalf("non-core status %q accepted", status)
		}
		if w.count() != 0 {
			t.Fatalf("non-core status %q wrote %d envelopes", status, w.count())
		}
	}
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

func TestFailCarriesApplicationFieldsWithoutReplacingCoreFields(t *testing.T) {
	w := &recordingWriter{}
	_, err := Fail(context.Background(), w, fixedClock(1), newRequest("r1", nil), "dependency_missing", "not present", map[string]any{
		"missing":    []string{"mcp:github"},
		"error_code": "forged",
		"status":     "completed",
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Status    string   `json:"status"`
		ErrorCode string   `json:"error_code"`
		Detail    string   `json:"detail"`
		Missing   []string `json:"missing"`
	}
	if err := json.Unmarshal(w.last().Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != message.StatusFailed || payload.ErrorCode != "dependency_missing" || payload.Detail != "not present" || len(payload.Missing) != 1 {
		t.Fatalf("unexpected structured failure: %+v", payload)
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

// BuildEvent fills the kind=event envelope's own fields and leaves the
// pen-injected ones (Sender / ChannelID) zero — the sealed-pen contract.
//
// This used to be asserted through EmitEvent, the pen-writing wrapper that
// lived alongside it. That wrapper is gone: it flattened a harness rejection
// into a formatted error, so a caller that has to map verdicts onto protocol
// codes could not recover the reason. The write moved to the Emit verb, which
// carries the rejection typed; what stays here is the construction contract.
func TestBuildEvent_FillsOwnFieldsAndLeavesPenInjectedZero(t *testing.T) {
	ev, err := BuildEvent(fixedClock(99), EventSpec{
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"hi":1}`),
		Visibility: message.Visibility("channel"),
		Audience:   message.Audience{actor.ActorID("a")},
		Cause:      message.Root(),
	})
	if err != nil {
		t.Fatalf("BuildEvent err: %v", err)
	}
	if ev.ID == "" {
		t.Fatal("want a generated id")
	}
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

// BuildEvent rejects an empty event type.
func TestBuildEvent_EmptyType(t *testing.T) {
	// Cause is supplied so the only thing missing is the type this test names.
	if _, err := BuildEvent(fixedClock(1), EventSpec{Cause: message.Root()}); err == nil {
		t.Fatal("empty type must error")
	}
}

// EventSpecJSON is the narrow three-argument sugar: it marshals a Go value
// into the spec's RawMessage payload and folds the variadic audience. A
// payload that cannot be marshalled is an error, never a silently empty spec.
func TestEventSpecJSON(t *testing.T) {
	trigger := message.Envelope{ID: "req-1", CorrelationID: "errand-1"}
	spec, err := EventSpecJSON(message.From(trigger), "agent.text", map[string]string{"text": "hi"}, actor.ActorID("a"))
	if err != nil {
		t.Fatalf("EventSpecJSON err: %v", err)
	}
	if spec.Type != "agent.text" {
		t.Fatalf("type = %q", spec.Type)
	}
	// The sugar carries the cause through rather than handing back a spec with
	// a required answer still blank for the caller to remember.
	built, err := BuildEvent(func() time.Time { return time.UnixMilli(1) }, spec)
	if err != nil {
		t.Fatalf("BuildEvent on the sugar's spec: %v", err)
	}
	if built.ParentID != "req-1" || built.CorrelationID != "errand-1" {
		t.Fatalf("built parent %q correlation %q, want req-1 / errand-1", built.ParentID, built.CorrelationID)
	}
	if string(spec.Payload) != `{"text":"hi"}` {
		t.Fatalf("payload = %s, want the marshalled map", spec.Payload)
	}
	if len(spec.Audience) != 1 || spec.Audience[0] != actor.ActorID("a") {
		t.Fatalf("audience = %v, want [a]", spec.Audience)
	}

	// No audience at all stays nil rather than becoming an empty slice: an
	// event addressed to nobody in particular is a real shape.
	spec, err = EventSpecJSON(message.Root(), "agent.text", nil)
	if err != nil {
		t.Fatalf("EventSpecJSON(no audience) err: %v", err)
	}
	if spec.Audience != nil {
		t.Fatalf("audience = %v, want nil", spec.Audience)
	}

	if _, err := EventSpecJSON(message.Root(), "agent.text", make(chan int)); err == nil {
		t.Fatal("an unmarshallable payload must error")
	}
}

// TestProgressAbsorbsBothTerminalAlreadyLandedVerdicts pins that a provisional
// losing the race to the final settles benignly regardless of WHICH word the
// harness uses to say so.
//
// stepResponsePairing hands a provisional-after-final its OWN verdict
// (harness_provisional_after_final) rather than the generic
// harness_terminal_duplicate. Progress used to absorb only the latter, so the
// former surfaced as an error — contradicting Progress's own doc promise that
// a lost race is "benign, not an error". Both words mean the same thing from
// this caller's seat: the terminal landed first.
func TestProgressAbsorbsBothTerminalAlreadyLandedVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reject harness.HarnessRejectReason
	}{
		{"terminal duplicate", harness.HarnessTerminalDuplicate},
		{"provisional after final", harness.HarnessProvisionalAfterFinal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := newRequest("req-progress", nil)
			w := &recordingWriter{reject: tc.reject}

			id, err := Progress(context.Background(), w, fixedClock(0), req, message.StatusProcessing, map[string]any{"pct": 50})
			if err != nil {
				t.Fatalf("Progress under %s: want benign settle, got error: %v", tc.reject, err)
			}
			if id == "" {
				t.Fatalf("Progress under %s: want the message id back, got empty", tc.reject)
			}
			if w.count() != 1 {
				t.Fatalf("Progress under %s: want exactly one write attempt, got %d", tc.reject, w.count())
			}
		})
	}
}

// TestProgressStillSurfacesUnrelatedRejects guards the absorption above from
// widening into "swallow every reject": a verdict that is NOT about the
// terminal already having landed must still reach the caller.
func TestProgressStillSurfacesUnrelatedRejects(t *testing.T) {
	req := newRequest("req-progress-2", nil)
	w := &recordingWriter{reject: harness.HarnessResponseParentNotFound}

	if _, err := Progress(context.Background(), w, fixedClock(0), req, message.StatusProcessing, map[string]any{}); err == nil {
		t.Fatal("Progress: an unrelated harness reject must surface as an error, got nil")
	}
}
