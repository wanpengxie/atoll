package v4types

import "testing"

// TestThreeClassesDisjoint asserts that the wire strings of the three
// reason closed sets (L1 §10.3) do not overlap. This is the formal
// "三类不相交" acceptance gate from ticket M1.3-T2.
func TestThreeClassesDisjoint(t *testing.T) {
	t.Parallel()

	seen := make(map[string]string)
	check := func(class, s string) {
		t.Helper()
		if prev, ok := seen[s]; ok {
			t.Errorf("reason %q collides across classes: %s vs %s", s, prev, class)
		}
		seen[s] = class
	}
	for _, r := range AllHarnessRejectReasons {
		check("harness_reject", r.String())
	}
	for _, r := range AllInstallReasons {
		check("install", r.String())
	}
	for _, r := range AllTerminalFailureReasons {
		check("terminal_failure", r.String())
	}
}

// TestClassTags confirms each closed set reports a stable Class().
func TestClassTags(t *testing.T) {
	t.Parallel()
	if got := HarnessAuthFailed.Class(); got != "harness_reject" {
		t.Errorf("HarnessRejectReason.Class() = %q, want %q", got, "harness_reject")
	}
	if got := InstallWorkerLockHeld.Class(); got != "install" {
		t.Errorf("InstallReason.Class() = %q, want %q", got, "install")
	}
	if got := TerminalUnansweredTimeout.Class(); got != "terminal_failure" {
		t.Errorf("TerminalFailureReason.Class() = %q, want %q", got, "terminal_failure")
	}
}

// TestHarnessRejectHTTPStatus spot-checks the L2 §3.6.1 mapping table.
// All named harness reject reasons must map to a non-zero HTTP status.
func TestHarnessRejectHTTPStatus(t *testing.T) {
	t.Parallel()
	cases := map[HarnessRejectReason]int{
		HarnessAuthFailed:                 401,
		HarnessMissingRequiredField:       400,
		HarnessKindInvalid:                400,
		HarnessResponseMissingParentID:    400,
		HarnessSenderMismatch:             403,
		HarnessSenderKindMismatch:         403,
		HarnessSenderDeregistered:         410,
		HarnessUnknownType:                400,
		HarnessKindNotAllowed:             400,
		HarnessRequestAudienceInvalid:     400,
		HarnessAudienceActorNotRegistered: 400,
		HarnessAudienceHandlerMismatch:    400,
		HarnessPayloadSchemaViolation:     400,
		HarnessDocRefsInvalid:             400,
		HarnessResponseParentInvalid:      400,
		HarnessTerminalDuplicate:          409,
		HarnessWorkerFencingStale:         410,
		HarnessEngineACLDenied:            403,
		HarnessMessageIDConflict:          409,
		HarnessChannelMismatch:            400,
	}
	for r, want := range cases {
		if got := r.HTTPStatus(); got != want {
			t.Errorf("%s.HTTPStatus() = %d, want %d", r, got, want)
		}
	}
	// Every named reason must have an HTTP status mapping (>= 400).
	for _, r := range AllHarnessRejectReasons {
		if got := r.HTTPStatus(); got < 400 || got > 499 {
			t.Errorf("%s.HTTPStatus() = %d, want a 4xx code", r, got)
		}
	}
}

// TestInstallHTTPStatus checks the install-time + spawn-API table.
func TestInstallHTTPStatus(t *testing.T) {
	t.Parallel()
	cases := map[InstallReason]int{
		InstallAdapterTimeoutMissing:         400,
		InstallFallbackResponseSchemaInvalid: 400,
		InstallHandlerActorNotRegistered:     400,
		InstallHandlerActorBindingMismatch:   400,
		InstallTypeRegistryInvalid:           400,
		InstallWorkerLockHeld:                409,
		InstallBootstrapInProgress:           409,
	}
	for r, want := range cases {
		if got := r.HTTPStatus(); got != want {
			t.Errorf("%s.HTTPStatus() = %d, want %d", r, got, want)
		}
	}
}

// TestTerminalFailureHTTPStatusZero asserts terminal failure reasons
// never carry an HTTP status — they live in payload.reason on otherwise
// successful kind=response messages.
func TestTerminalFailureHTTPStatusZero(t *testing.T) {
	t.Parallel()
	for _, r := range AllTerminalFailureReasons {
		if got := r.HTTPStatus(); got != 0 {
			t.Errorf("%s.HTTPStatus() = %d, want 0", r, got)
		}
	}
}

// TestReasonInterface confirms all three concrete types satisfy the
// Reason interface (compile-time check via _ = Reason(x)).
func TestReasonInterface(t *testing.T) {
	t.Parallel()
	var _ Reason = HarnessAuthFailed
	var _ Reason = InstallWorkerLockHeld
	var _ Reason = TerminalUnansweredTimeout
}

// TestReasonCardinalities pins the closed-set sizes to the L1 §10.3 spec
// counts. If a future protocol revision adds / removes a reason it
// MUST update this test to acknowledge the closed-set change.
func TestReasonCardinalities(t *testing.T) {
	t.Parallel()
	if got, want := len(AllHarnessRejectReasons), 20; got != want {
		t.Errorf("len(AllHarnessRejectReasons) = %d, want %d", got, want)
	}
	if got, want := len(AllInstallReasons), 7; got != want {
		t.Errorf("len(AllInstallReasons) = %d, want %d", got, want)
	}
	if got, want := len(AllTerminalFailureReasons), 4; got != want {
		t.Errorf("len(AllTerminalFailureReasons) = %d, want %d", got, want)
	}
}
