package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// seedRequest appends a real kind=request parent row so response-pairing can
// resolve it. Returns the parent id. caller/receiver set up the closure
// authorization fabric: parent.sender=caller, parent.audience=[receiver].
func seedRequest(t *testing.T, cs *store.ChannelStores, id message.ID, caller, receiver actor.ActorID) {
	t.Helper()
	parent := &message.Envelope{
		ID: id, TS: fixedNowMs - 5000, ChannelID: testChannelID,
		Sender: message.Sender{ID: caller, Kind: actor.KindAgent},
		Kind:   message.KindRequest, Type: "xhs.publish",
		Audience:   message.Audience{receiver},
		Visibility: message.VisibilityPublic,
		Payload:    json.RawMessage(`{}`),
	}
	if _, err := cs.Log.Append(context.Background(), parent, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("seed request %q: %v", id, err)
	}
}

// seedNonRequestParent appends a kind=event row to exercise parent_not_request.
func seedEvent(t *testing.T, cs *store.ChannelStores, id message.ID) {
	t.Helper()
	ev := &message.Envelope{
		ID: id, TS: fixedNowMs - 5000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p", Kind: actor.KindAgent},
		Kind:   message.KindEvent, Type: "agent.text",
		Audience:   message.Audience{"x"},
		Visibility: message.VisibilityPublic,
		Payload:    json.RawMessage(`{}`),
	}
	if _, err := cs.Log.Append(context.Background(), ev, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("seed event %q: %v", id, err)
	}
}

// response builds a kind=response envelope from receiver answering parent,
// addressed back at the request sender `caller`.
func response(parentID message.ID, sender, caller actor.ActorID, payload string) *message.Envelope {
	return &message.Envelope{
		ID: "resp-" + parentID, TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: sender, Kind: actor.KindTool},
		Kind:   message.KindResponse, Type: "xhs.publish",
		ParentID:   parentID,
		Audience:   message.Audience{caller},
		Visibility: message.VisibilityPublic,
		Payload:    json.RawMessage(payload),
	}
}

// non-response envelopes are a no-op (the step only governs kind=response).
func TestStepResponsePairing_NonResponseNoop(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	e := validEvent("m1", "a")
	out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !out.Continue() || out.IsTerminal {
		t.Fatalf("event must pass non-terminal, got reason=%q terminal=%v", out.RejectReason, out.IsTerminal)
	}
}

func TestStepResponsePairing_ParentChecks(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	seedEvent(t, cs, "ev1")

	t.Run("parent not found", func(t *testing.T) {
		e := response("missing-parent", "tool:xhs", "agent:caller", `{"status":"completed"}`)
		out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.RejectReason != HarnessResponseParentNotFound {
			t.Fatalf("reason = %q, want parent_not_found", out.RejectReason)
		}
	})

	t.Run("parent not a request", func(t *testing.T) {
		e := response("ev1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
		out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.RejectReason != HarnessResponseParentNotRequest {
			t.Fatalf("reason = %q, want parent_not_request", out.RejectReason)
		}
	})
}

// status classification: L1 final / L2 provisional / L3 namespaced / invalid.
func TestStepResponsePairing_StatusClassification(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	seedRequest(t, cs, "req1", "agent:caller", "tool:xhs")

	tests := []struct {
		name     string
		payload  string
		reason   HarnessRejectReason
		terminal bool
	}{
		{"L1 completed final", `{"status":"completed"}`, "", true},
		{"L1 failed final with valid reason", `{"status":"failed","reason":"receiver_internal_error"}`, "", true},
		{"L2 processing provisional", `{"status":"processing"}`, "", false},
		{"L2 received provisional", `{"status":"received"}`, "", false},
		{"L3 namespaced matching sender local-name", `{"status":"xhs.uploading"}`, "", false},
		{"missing status invalid", `{}`, HarnessResponseStatusInvalid, false},
		{"non-string status invalid", `{"status":123}`, HarnessResponseStatusInvalid, false},
		{"unknown bare status invalid", `{"status":"wibble"}`, HarnessResponseStatusInvalid, false},
		{"L3 namespace colliding with L2 name invalid", `{"status":"processing.more"}`, HarnessResponseStatusInvalid, false},
		{"L3 namespace colliding with L1 name invalid", `{"status":"completed.x"}`, HarnessResponseStatusInvalid, false},
		{"L3 namespace not equal sender local-name mismatch", `{"status":"other.thing"}`, HarnessResponseStatusNamespaceMismatch, false},
		{"failed with invalid reason rejected", `{"status":"failed","reason":"made_up"}`, HarnessResponseReasonInvalid, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := response("req1", "tool:xhs", "agent:caller", tc.payload)
			out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
			if tc.reason == "" && out.IsTerminal != tc.terminal {
				t.Fatalf("is_terminal = %v, want %v", out.IsTerminal, tc.terminal)
			}
		})
	}
}

// Closure authorization — three authors (receiver / caller-self-close / substrate-death).
func TestStepResponsePairing_ClosureAuthors(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	seedRequest(t, cs, "req1", "agent:caller", "tool:xhs")

	tests := []struct {
		name    string
		sender  actor.ActorID
		payload string
		reason  HarnessRejectReason
	}{
		{
			name:    "receiver voluntary (sender in parent.audience) authorized",
			sender:  "tool:xhs",
			payload: `{"status":"completed"}`,
			reason:  "",
		},
		{
			name:    "caller self-close unanswered_timeout authorized",
			sender:  "agent:caller",
			payload: `{"status":"failed","reason":"unanswered_timeout"}`,
			reason:  "",
		},
		{
			name:    "substrate death receiver_unavailable from system authorized",
			sender:  actor.SystemActorID,
			payload: `{"status":"failed","reason":"receiver_unavailable"}`,
			reason:  "",
		},
		{
			name:    "unrelated sender not authorized",
			sender:  "agent:stranger",
			payload: `{"status":"completed"}`,
			reason:  HarnessResponseUnauthorizedSender,
		},
		{
			name:    "caller self-close but with non-timeout reason not authorized",
			sender:  "agent:caller",
			payload: `{"status":"failed","reason":"receiver_internal_error"}`,
			reason:  HarnessResponseUnauthorizedSender,
		},
		{
			// 期12 义务归位: the expiry reaper materialises a declared-deadline
			// terminal — system is the second authorized observer of the
			// deadline fact (was a reject before the reaper existed).
			name:    "substrate expiry unanswered_timeout from system authorized",
			sender:  actor.SystemActorID,
			payload: `{"status":"failed","reason":"unanswered_timeout"}`,
			reason:  "",
		},
		{
			name:    "system with receiver's word (receiver_internal_error) not authorized",
			sender:  actor.SystemActorID,
			payload: `{"status":"failed","reason":"receiver_internal_error"}`,
			reason:  HarnessResponseUnauthorizedSender,
		},
		{
			name:    "receiver failed with its own word receiver_internal_error authorized",
			sender:  "tool:xhs",
			payload: `{"status":"failed","reason":"receiver_internal_error"}`,
			reason:  "",
		},
		{
			name:    "receiver forging receiver_unavailable (substrate's word) not authorized",
			sender:  "tool:xhs",
			payload: `{"status":"failed","reason":"receiver_unavailable"}`,
			reason:  HarnessResponseUnauthorizedSender,
		},
		{
			name:    "receiver forging unanswered_timeout (caller's word) not authorized",
			sender:  "tool:xhs",
			payload: `{"status":"failed","reason":"unanswered_timeout"}`,
			reason:  HarnessResponseUnauthorizedSender,
		},
		{
			name:    "failed without reason rejected (§2.6: failed MUST carry reason)",
			sender:  "tool:xhs",
			payload: `{"status":"failed"}`,
			reason:  HarnessResponseReasonInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := response("req1", tc.sender, "agent:caller", tc.payload)
			out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}

// Response audience must equal the parent request sender exactly.
func TestStepResponsePairing_AudienceMismatch(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	seedRequest(t, cs, "req1", "agent:caller", "tool:xhs")

	e := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	e.Audience = message.Audience{"someone-else"} // not parent.sender
	out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.RejectReason != HarnessResponseAudienceMismatch {
		t.Fatalf("reason = %q, want audience_mismatch", out.RejectReason)
	}
}

// Terminal uniqueness: a stored final response closes the request. A second
// final → terminal_duplicate; a provisional after final → provisional_after_final.
// (late_final is DELETED — a late final is just a terminal_duplicate.)
func TestStepResponsePairing_TerminalUniqueness(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	seedRequest(t, cs, "req1", "agent:caller", "tool:xhs")

	// Append a real final response so HasFinalResponse(parent) is true.
	final := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	final.ID = "final-1"
	if _, err := cs.Log.Append(context.Background(), final, true, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("seed final response: %v", err)
	}

	t.Run("second final rejects terminal_duplicate", func(t *testing.T) {
		e := response("req1", "tool:xhs", "agent:caller", `{"status":"failed","reason":"receiver_internal_error"}`)
		e.ID = "final-2"
		out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.RejectReason != HarnessTerminalDuplicate {
			t.Fatalf("reason = %q, want terminal_duplicate", out.RejectReason)
		}
	})

	t.Run("provisional after final rejects provisional_after_final", func(t *testing.T) {
		e := response("req1", "tool:xhs", "agent:caller", `{"status":"processing"}`)
		e.ID = "prov-1"
		out, err := runStep(t, newStepResponsePairing, deps, context.Background(), e)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.RejectReason != HarnessProvisionalAfterFinal {
			t.Fatalf("reason = %q, want provisional_after_final", out.RejectReason)
		}
	})
}

// responsePairingDeps builds Deps whose Log returns a request parent for the
// given parentID and lets the test control HasFinalResponse — the stub seam for
// the store-fault branches the real store won't produce on demand.
func responsePairingDeps(parent *storespec.StoredRow, findErr error, hasFinal func(context.Context, message.ID) (bool, error)) Deps {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID: func(_ context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
			if findErr != nil {
				return nil, false, findErr
			}
			if parent == nil {
				return nil, false, nil
			}
			return parent, true, nil
		},
		hasFinalFn: hasFinal,
	}
	return Deps{ChannelID: testChannelID, Log: lg, Presence: testAuthority{}}
}

func requestParent() *storespec.StoredRow {
	return &storespec.StoredRow{Envelope: message.Envelope{
		ID: "req1", ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:caller"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:xhs"},
	}}
}

func TestStepResponsePairing_FindByIDError(t *testing.T) {
	wantErr := errors.New("find down")
	deps := responsePairingDeps(nil, wantErr, func(context.Context, message.ID) (bool, error) { return false, nil })
	resp := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	_, err := runStep(t, newStepResponsePairing, deps, ctxCaller("tool:xhs"), resp)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("FindByID error = %v, want wrapping %v", err, wantErr)
	}
}

func TestStepResponsePairing_HasFinalResponseError(t *testing.T) {
	wantErr := errors.New("hasfinal down")
	deps := responsePairingDeps(requestParent(), nil, func(context.Context, message.ID) (bool, error) {
		return false, wantErr
	})
	resp := response("req1", "tool:xhs", "agent:caller", `{"status":"completed"}`)
	_, err := runStep(t, newStepResponsePairing, deps, ctxCaller("tool:xhs"), resp)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("HasFinalResponse error = %v, want wrapping %v", err, wantErr)
	}
}

// checkFailedResponseReason branches: empty payload, malformed JSON, missing
// status, non-failed status, failed without/with-invalid/with-valid reason.
func TestCheckFailedResponseReason_Branches(t *testing.T) {
	if c := checkFailedResponseReason(nil); c.failed {
		t.Fatalf("nil payload should not be failed")
	}
	if c := checkFailedResponseReason([]byte("{bad")); c.failed {
		t.Fatalf("malformed payload should not be failed")
	}
	if c := checkFailedResponseReason([]byte(`{}`)); c.failed {
		t.Fatalf("no status should not be failed")
	}
	// status present but not failed (e.g. completed) → status unmarshal ok, !=failed.
	if c := checkFailedResponseReason([]byte(`{"status":"completed"}`)); c.failed {
		t.Fatalf("completed should not be failed")
	}
	// status non-string → unmarshal into string fails → not failed.
	if c := checkFailedResponseReason([]byte(`{"status":123}`)); c.failed {
		t.Fatalf("non-string status should not be failed")
	}
	// failed, no reason → failed=true, hasReason=false, invalid=false.
	c := checkFailedResponseReason([]byte(`{"status":"failed"}`))
	if !c.failed || c.hasReason || c.invalid {
		t.Fatalf("failed-no-reason = %+v", c)
	}
	// failed, reason non-string → invalid=true.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":123}`))
	if !c.invalid || c.detail == "" {
		t.Fatalf("failed-nonstring-reason = %+v, want invalid", c)
	}
	// failed, reason out of closed set → invalid via terminalFailureReasonAllowed.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":"not_a_reason"}`))
	if !c.invalid {
		t.Fatalf("failed-bad-reason should be invalid: %+v", c)
	}
	// failed, valid reason → valid.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":"receiver_internal_error"}`))
	if c.invalid || c.reason != "receiver_internal_error" {
		t.Fatalf("failed-good-reason = %+v", c)
	}
}

// extractPayloadStatus arms: empty, malformed, missing, non-string, present.
func TestExtractPayloadStatus_Branches(t *testing.T) {
	if _, ok := extractPayloadStatus(nil); ok {
		t.Fatalf("empty payload should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte("{nope")); ok {
		t.Fatalf("malformed payload should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte(`{}`)); ok {
		t.Fatalf("missing status should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte(`{"status":5}`)); ok {
		t.Fatalf("non-string status should be ok=false")
	}
	s, ok := extractPayloadStatus([]byte(`{"status":"processing"}`))
	if !ok || s != "processing" {
		t.Fatalf("extract = %q %v", s, ok)
	}
}

// senderLocalName fallback when no ':' present.
func TestSenderLocalName_NoColonFallback(t *testing.T) {
	if got := senderLocalName(actor.ActorID("daemon")); got != "daemon" {
		t.Fatalf("no-colon = %q, want daemon", got)
	}
	if got := senderLocalName(actor.ActorID("a:b:c")); got != "c" {
		t.Fatalf("nested = %q, want c", got)
	}
}
