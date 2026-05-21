package message

import "testing"

func TestAllTerminalFailureReasonsSpecClosedSet(t *testing.T) {
	want := []TerminalFailureReason{
		TerminalUnansweredTimeout,
		TerminalReceiverInternalError,
		TerminalReceiverUnavailable,
	}
	if len(AllTerminalFailureReasons) != len(want) {
		t.Fatalf("AllTerminalFailureReasons len=%d want %d", len(AllTerminalFailureReasons), len(want))
	}
	for i := range want {
		if AllTerminalFailureReasons[i] != want[i] {
			t.Fatalf("AllTerminalFailureReasons[%d]=%q want %q", i, AllTerminalFailureReasons[i], want[i])
		}
	}
}

func TestHarnessRejectReasonHTTPStatus(t *testing.T) {
	cases := []struct {
		reason HarnessRejectReason
		want   int
	}{
		{HarnessSenderKindMismatch, 400},
		{HarnessAudienceMemberNotActive, 403},
		{HarnessSenderDeregistered, 410},
		{HarnessWorkerFencingStale, 410},
		{HarnessEngineACLDenied, 500},
		{HarnessIDDuplicateConflict, 409},
		{HarnessTerminalDuplicate, 409},
	}

	for _, tc := range cases {
		if got := tc.reason.HTTPStatus(); got != tc.want {
			t.Errorf("%s HTTPStatus=%d want=%d", tc.reason, got, tc.want)
		}
	}
}

func TestAllHarnessRejectReasonsHaveHTTPStatus(t *testing.T) {
	for _, reason := range AllHarnessRejectReasons {
		if got := reason.HTTPStatus(); got == 0 {
			t.Errorf("%s HTTPStatus=0", reason)
		}
	}
}

func TestAllInstallReasonsSpecClosedSet(t *testing.T) {
	want := []InstallReason{
		InstallAdapterTimeoutMissing,
		InstallHandlerActorNotRegistered,
		InstallHandlerActorBindingMismatch,
		InstallTypeRegistryInvalid,
		InstallTypeRegistryReservedNamespace,
		InstallWorkerLockHeld,
		InstallBootstrapInProgress,
	}
	if len(AllInstallReasons) != len(want) {
		t.Fatalf("AllInstallReasons len=%d want %d", len(AllInstallReasons), len(want))
	}
	for i := range want {
		if AllInstallReasons[i] != want[i] {
			t.Fatalf("AllInstallReasons[%d]=%q want %q", i, AllInstallReasons[i], want[i])
		}
	}
}

func TestAllInstallReasonsHaveHTTPStatus(t *testing.T) {
	for _, reason := range AllInstallReasons {
		if got := reason.HTTPStatus(); got == 0 {
			t.Errorf("%s HTTPStatus=0", reason)
		}
	}
}
