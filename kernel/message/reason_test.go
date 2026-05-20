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
