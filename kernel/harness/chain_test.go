package harness_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestAllStepIDs covers the StepID closed set + ordering.
func TestAllStepIDs(t *testing.T) {
	if len(harness.AllStepIDs) != 10 {
		t.Fatalf("AllStepIDs len=%d, want 10 (Step 0..9)", len(harness.AllStepIDs))
	}
	want := []harness.StepID{
		harness.StepNormalize,
		harness.StepCallerAuth,
		harness.StepRequiredFields,
		harness.StepSenderConsistent,
		harness.StepTypeRegistered,
		harness.StepKindAndAudience,
		harness.StepPayloadSchema,
		harness.StepDocRefs,
		harness.StepResponsePairing,
		harness.StepEngineAppend,
	}
	for i, id := range want {
		if harness.AllStepIDs[i] != id {
			t.Errorf("step[%d] = %d, want %d", i, harness.AllStepIDs[i], id)
		}
	}
}

// TestAllRejectReasons enforces the L1 §10.3.1 closed-set contract.
// channel_mismatch was removed in FIX-T1 — count must be 19.
func TestAllRejectReasons(t *testing.T) {
	want := []message.HarnessRejectReason{
		message.HarnessAuthFailed,
		message.HarnessMissingRequiredField,
		message.HarnessKindInvalid,
		message.HarnessResponseMissingParentID,
		message.HarnessSenderMismatch,
		message.HarnessSenderKindMismatch,
		message.HarnessSenderDeregistered,
		message.HarnessUnknownType,
		message.HarnessKindNotAllowed,
		message.HarnessRequestAudienceInvalid,
		message.HarnessAudienceActorNotRegistered,
		message.HarnessAudienceHandlerMismatch,
		message.HarnessPayloadSchemaViolation,
		message.HarnessDocRefsInvalid,
		message.HarnessResponseParentInvalid,
		message.HarnessTerminalDuplicate,
		message.HarnessWorkerFencingStale,
		message.HarnessEngineACLDenied,
		message.HarnessMessageIDConflict,
	}
	if len(harness.AllRejectReasons) != len(want) {
		t.Fatalf("AllRejectReasons len=%d want=%d (L1 §10.3.1 closed set)",
			len(harness.AllRejectReasons), len(want))
	}
	seen := map[message.HarnessRejectReason]bool{}
	for _, r := range harness.AllRejectReasons {
		if seen[r] {
			t.Errorf("duplicate reason %s", r)
		}
		seen[r] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("missing reason %s", w)
		}
	}
}

// TestOutcomeContinue distinguishes accept vs reject outcomes.
func TestOutcomeContinue(t *testing.T) {
	if !(harness.Outcome{}).Continue() {
		t.Error("zero outcome should Continue")
	}
	o := harness.Outcome{RejectReason: message.HarnessAuthFailed}
	if o.Continue() {
		t.Error("rejected outcome should not Continue")
	}
}

// TestWriteResultAccepted covers Accepted / Deduped flag semantics.
func TestWriteResultAccepted(t *testing.T) {
	if !(harness.WriteResult{}).Accepted() {
		t.Error("zero WriteResult should be Accepted (no reject)")
	}
	r := harness.WriteResult{RejectReason: message.HarnessTerminalDuplicate}
	if r.Accepted() {
		t.Error("rejected WriteResult should not be Accepted")
	}
	d := harness.WriteResult{Deduped: true}
	if !d.Accepted() {
		t.Error("dedupe WriteResult should be Accepted")
	}
}

// TestStepIDIsTerminalAppend — StepEngineAppend must be the last numbered
// step (index 9) so chain runners can pattern-match the terminal step.
func TestStepIDIsTerminalAppend(t *testing.T) {
	last := harness.AllStepIDs[len(harness.AllStepIDs)-1]
	if last != harness.StepEngineAppend {
		t.Errorf("AllStepIDs[last]=%d want StepEngineAppend(%d)",
			last, harness.StepEngineAppend)
	}
}

// TestStepDedupeOutsideOrdinalRange — StepDedupe sits between Normalize
// and CallerAuth conceptually but is NOT part of AllStepIDs (it is
// referenced by name).
func TestStepDedupeOutsideOrdinalRange(t *testing.T) {
	if harness.StepDedupe >= 0 {
		t.Errorf("StepDedupe=%d should be sentinel < 0", harness.StepDedupe)
	}
	for _, id := range harness.AllStepIDs {
		if id == harness.StepDedupe {
			t.Errorf("AllStepIDs contains StepDedupe; should be sentinel only")
		}
	}
}

// TestOutcomeDetailTransparency — Detail is informative; it travels
// through Outcome without being interpreted by Continue().
func TestOutcomeDetailTransparency(t *testing.T) {
	o := harness.Outcome{
		RejectReason: message.HarnessDocRefsInvalid,
		Detail:       "missing scheme",
	}
	if o.Continue() {
		t.Error("rejected Outcome should not Continue regardless of Detail")
	}
	if o.Detail != "missing scheme" {
		t.Errorf("Detail=%q corrupted by Outcome accessors", o.Detail)
	}
}

// TestOutcomePartialMessageIDLateReject — late-stage rejects (step 8)
// surface PartialMessageID so callers can correlate the failed id.
func TestOutcomePartialMessageIDLateReject(t *testing.T) {
	o := harness.Outcome{
		RejectReason:     message.HarnessTerminalDuplicate,
		PartialMessageID: "m-abc",
	}
	if o.Continue() {
		t.Error("late-reject Outcome should not Continue")
	}
	if o.PartialMessageID != "m-abc" {
		t.Errorf("PartialMessageID=%q want m-abc", o.PartialMessageID)
	}
}

// TestWriteResultPartialMessageIDPreserved — late reject (step 8) must
// expose PartialMessageID; MessageID stays empty since no durable row.
func TestWriteResultPartialMessageIDPreserved(t *testing.T) {
	r := harness.WriteResult{
		RejectReason:     message.HarnessTerminalDuplicate,
		PartialMessageID: "m-late",
	}
	if r.Accepted() {
		t.Error("late-reject WriteResult should not be Accepted")
	}
	if r.PartialMessageID != "m-late" {
		t.Errorf("PartialMessageID=%q want m-late", r.PartialMessageID)
	}
	if r.MessageID != "" {
		t.Errorf("MessageID=%q want empty (no durable row on reject)", r.MessageID)
	}
}

// TestWriteResultDedupeRoundTrip — dedupe path: Accepted + Deduped both
// true; MessageID populated with the existing row's id.
func TestWriteResultDedupeRoundTrip(t *testing.T) {
	r := harness.WriteResult{
		MessageID: "m-existing",
		Seq:       42,
		Deduped:   true,
	}
	if !r.Accepted() {
		t.Error("dedupe WriteResult should be Accepted")
	}
	if !r.Deduped {
		t.Error("Deduped flag corrupted")
	}
	if r.Seq != 42 {
		t.Errorf("Seq=%d want 42 (dedupe surfaces seq of the original row)", r.Seq)
	}
}

// TestRejectReasonAliasMatchesMessageReason — kernel/harness.RejectReason
// is an alias for kernel/message.HarnessRejectReason; callers must be
// able to compare against either form without conversion.
func TestRejectReasonAliasMatchesMessageReason(t *testing.T) {
	r := harness.RejectAuthFailed
	if r != message.HarnessAuthFailed {
		t.Errorf("alias mismatch: %v != %v", r, message.HarnessAuthFailed)
	}
}

// TestRejectReasonReExports — every closed-set value MUST have a
// `harness.Reject*` alias so call sites can stay inside the harness
// package namespace.
func TestRejectReasonReExports(t *testing.T) {
	pairs := map[message.HarnessRejectReason]message.HarnessRejectReason{
		harness.RejectAuthFailed:                 message.HarnessAuthFailed,
		harness.RejectMissingRequiredField:       message.HarnessMissingRequiredField,
		harness.RejectKindInvalid:                message.HarnessKindInvalid,
		harness.RejectResponseMissingParentID:    message.HarnessResponseMissingParentID,
		harness.RejectSenderMismatch:             message.HarnessSenderMismatch,
		harness.RejectSenderKindMismatch:         message.HarnessSenderKindMismatch,
		harness.RejectSenderDeregistered:         message.HarnessSenderDeregistered,
		harness.RejectUnknownType:                message.HarnessUnknownType,
		harness.RejectKindNotAllowed:             message.HarnessKindNotAllowed,
		harness.RejectRequestAudienceInvalid:     message.HarnessRequestAudienceInvalid,
		harness.RejectAudienceActorNotRegistered: message.HarnessAudienceActorNotRegistered,
		harness.RejectAudienceHandlerMismatch:    message.HarnessAudienceHandlerMismatch,
		harness.RejectPayloadSchemaViolation:     message.HarnessPayloadSchemaViolation,
		harness.RejectDocRefsInvalid:             message.HarnessDocRefsInvalid,
		harness.RejectResponseParentInvalid:      message.HarnessResponseParentInvalid,
		harness.RejectTerminalDuplicate:          message.HarnessTerminalDuplicate,
		harness.RejectWorkerFencingStale:         message.HarnessWorkerFencingStale,
		harness.RejectEngineACLDenied:            message.HarnessEngineACLDenied,
		harness.RejectMessageIDConflict:          message.HarnessMessageIDConflict,
	}
	for got, want := range pairs {
		if got != want {
			t.Errorf("re-export drift: %v != %v", got, want)
		}
	}
}

// TestRejectReasonHTTPStatus spot-checks the L2 §3.6.1 status map for
// the closed set (covers the categories: auth=401, sender=403,
// deregistered/fence=410, conflict=409, malformed=400).
func TestRejectReasonHTTPStatus(t *testing.T) {
	cases := []struct {
		r    message.HarnessRejectReason
		want int
	}{
		{message.HarnessAuthFailed, 401},
		{message.HarnessSenderMismatch, 403},
		{message.HarnessSenderKindMismatch, 403},
		{message.HarnessEngineACLDenied, 403},
		{message.HarnessSenderDeregistered, 410},
		{message.HarnessWorkerFencingStale, 410},
		{message.HarnessTerminalDuplicate, 409},
		{message.HarnessMessageIDConflict, 409},
		{message.HarnessMissingRequiredField, 400},
		{message.HarnessKindInvalid, 400},
		{message.HarnessDocRefsInvalid, 400},
	}
	for _, tc := range cases {
		if got := tc.r.HTTPStatus(); got != tc.want {
			t.Errorf("%s HTTPStatus=%d want=%d", tc.r, got, tc.want)
		}
	}
}
