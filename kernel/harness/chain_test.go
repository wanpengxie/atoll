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
