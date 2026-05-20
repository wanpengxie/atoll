package harness_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestAllStepIDs covers the StepID closed set + ordering. The chain
// now runs in single-loop ascending order with StepDedupe sitting
// between StepEnvelopeShape and StepNormalize per proto-layer1 §2.3.
func TestAllStepIDs(t *testing.T) {
	if len(harness.AllStepIDs) != 10 {
		t.Fatalf("AllStepIDs len=%d, want 10 (proto-layer1 §2.0)", len(harness.AllStepIDs))
	}
	want := []harness.StepID{
		harness.StepCallerAuth,
		harness.StepEnvelopeShape,
		harness.StepDedupe,
		harness.StepNormalize,
		harness.StepSenderConsistent,
		harness.StepTypeRegistered,
		harness.StepKindAndAudience,
		harness.StepPayloadSchema,
		harness.StepResponsePairing,
		harness.StepEngineAppend,
	}
	for i, id := range want {
		if harness.AllStepIDs[i] != id {
			t.Errorf("step[%d] = %d, want %d", i, harness.AllStepIDs[i], id)
		}
	}
}

// TestStepIDsAreStrictlyAscending — chain.go relies on ascending IDs
// for the single-loop runner.
func TestStepIDsAreStrictlyAscending(t *testing.T) {
	for i := 1; i < len(harness.AllStepIDs); i++ {
		if harness.AllStepIDs[i] <= harness.AllStepIDs[i-1] {
			t.Errorf("AllStepIDs not strictly ascending at index %d: %d <= %d",
				i, harness.AllStepIDs[i], harness.AllStepIDs[i-1])
		}
	}
}

// TestStepDedupeOrdering — StepDedupe MUST sit between StepEnvelopeShape
// and StepNormalize so canonical_hash sees sender-provided fields
// (proto-layer1 §2.3).
func TestStepDedupeOrdering(t *testing.T) {
	if harness.StepDedupe <= harness.StepEnvelopeShape {
		t.Errorf("StepDedupe (%d) must run AFTER StepEnvelopeShape (%d)",
			harness.StepDedupe, harness.StepEnvelopeShape)
	}
	if harness.StepDedupe >= harness.StepNormalize {
		t.Errorf("StepDedupe (%d) must run BEFORE StepNormalize (%d) so canonical_hash sees sender-provided fields",
			harness.StepDedupe, harness.StepNormalize)
	}
}

// TestAllRejectReasons enforces the proto-layer1 §2.11.1 closed-set
// contract.
func TestAllRejectReasons(t *testing.T) {
	want := []message.HarnessRejectReason{
		message.HarnessWorkerFencingStale,
		message.HarnessEnvelopeFieldMissing,
		message.HarnessChannelMismatch,
		message.HarnessKindInvalid,
		message.HarnessVisibilityInvalid,
		message.HarnessVisibilityAudienceInvalid,
		message.HarnessEnvelopeUnknownField,
		message.HarnessIDDuplicateConflict,
		message.HarnessTimeInvalid,
		message.HarnessTypeUnknown,
		message.HarnessKindNotAllowedForType,
		message.HarnessReservedTypeUnauthorizedSender,
		message.HarnessSenderDeregistered,
		message.HarnessSenderKindMismatch,
		message.HarnessSenderMismatch,
		message.HarnessAudienceEmpty,
		message.HarnessAudienceMixedWildcard,
		message.HarnessAudienceMemberNotActive,
		message.HarnessRequestAudienceInvalid,
		message.HarnessResponseAudienceInvalid,
		message.HarnessAudienceHandlerMismatch,
		message.HarnessSchemaMissing,
		message.HarnessPayloadSchemaInvalid,
		message.HarnessResponseMissingParent,
		message.HarnessResponseParentNotFound,
		message.HarnessResponseParentNotRequest,
		message.HarnessResponseStatusInvalid,
		message.HarnessResponseReasonInvalid,
		message.HarnessResponseUnauthorizedSender,
		message.HarnessResponseAudienceMismatch,
		message.HarnessTerminalDuplicate,
		message.HarnessEngineACLDenied,
	}
	if len(message.AllHarnessRejectReasons) != len(want) {
		t.Fatalf("AllRejectReasons len=%d want=%d (proto-layer1 §2.11.1 closed set)",
			len(message.AllHarnessRejectReasons), len(want))
	}
	seen := map[message.HarnessRejectReason]bool{}
	for _, r := range message.AllHarnessRejectReasons {
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
	o := harness.Outcome{RejectReason: message.HarnessEngineACLDenied}
	if o.Continue() {
		t.Error("rejected outcome should not Continue")
	}
	d := harness.Outcome{Deduped: true}
	if d.Continue() {
		t.Error("dedupe outcome should not Continue (short-circuit)")
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

// TestStepIDIsTerminalAppend — StepEngineAppend must be the last
// numbered step so chain runners can pattern-match the terminal step.
func TestStepIDIsTerminalAppend(t *testing.T) {
	last := harness.AllStepIDs[len(harness.AllStepIDs)-1]
	if last != harness.StepEngineAppend {
		t.Errorf("AllStepIDs[last]=%d want StepEngineAppend(%d)",
			last, harness.StepEngineAppend)
	}
}

// TestOutcomeDetailTransparency — Detail is informative; it travels
// through Outcome without being interpreted by Continue().
func TestOutcomeDetailTransparency(t *testing.T) {
	o := harness.Outcome{
		RejectReason: message.HarnessEnvelopeFieldMissing,
		Detail:       "missing channel_id",
	}
	if o.Continue() {
		t.Error("rejected Outcome should not Continue regardless of Detail")
	}
	if o.Detail != "missing channel_id" {
		t.Errorf("Detail=%q corrupted by Outcome accessors", o.Detail)
	}
}

// TestOutcomePartialMessageIDLateReject — late-stage rejects (step 9)
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

// TestWriteResultPartialMessageIDPreserved — late reject (step 9) must
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
	assertReason := func(r harness.RejectReason) {
		if r != message.HarnessEngineACLDenied {
			t.Errorf("alias mismatch: %v != %v", r, message.HarnessEngineACLDenied)
		}
	}
	assertReason(message.HarnessEngineACLDenied)
}

// TestRejectReasonHTTPStatus spot-checks the L2 §3.6.1 status map for
// the closed set (covers the categories: sender/auth=403,
// deregistered/fence=410, conflict=409, malformed=400).
func TestRejectReasonHTTPStatus(t *testing.T) {
	cases := []struct {
		r    message.HarnessRejectReason
		want int
	}{
		{message.HarnessSenderMismatch, 403},
		{message.HarnessSenderKindMismatch, 403},
		{message.HarnessEngineACLDenied, 403},
		{message.HarnessResponseUnauthorizedSender, 403},
		{message.HarnessReservedTypeUnauthorizedSender, 403},
		{message.HarnessSenderDeregistered, 410},
		{message.HarnessWorkerFencingStale, 410},
		{message.HarnessTerminalDuplicate, 409},
		{message.HarnessIDDuplicateConflict, 409},
		{message.HarnessEnvelopeFieldMissing, 400},
		{message.HarnessKindInvalid, 400},
		{message.HarnessResponseStatusInvalid, 400},
		{message.HarnessResponseParentNotFound, 400},
		{message.HarnessResponseParentNotRequest, 400},
		{message.HarnessResponseAudienceMismatch, 400},
		{message.HarnessSchemaMissing, 400},
		{message.HarnessPayloadSchemaInvalid, 400},
		{message.HarnessReservedTypeUnauthorizedSender, 403},
	}
	for _, tc := range cases {
		if got := tc.r.HTTPStatus(); got != tc.want {
			t.Errorf("%s HTTPStatus=%d want=%d", tc.r, got, tc.want)
		}
	}
}
