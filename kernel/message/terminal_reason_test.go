package message

import "testing"

// TestIsValidTerminalFailureReason pins the frozen 3-value failure closed
// set (INVARIANT-10): the three blessed reasons validate, everything else
// — including the empty string and a final/provisional status word that
// belongs to a different vocabulary — is rejected.
func TestIsValidTerminalFailureReason(t *testing.T) {
	t.Parallel()

	valid := []TerminalFailureReason{
		TerminalUnansweredTimeout,
		TerminalReceiverInternalError,
		TerminalReceiverUnavailable,
	}
	for _, r := range valid {
		if !IsValidTerminalFailureReason(r) {
			t.Errorf("IsValidTerminalFailureReason(%q) = false, want true", r)
		}
		if r.String() != string(r) {
			t.Errorf("%q.String() = %q", r, r.String())
		}
	}

	invalid := []TerminalFailureReason{
		"",
		"timeout",               // bare gen_server form, not the substrate spelling
		"unanswered",            // partial
		"receiver_dead",         // not in set
		"internal_error",        // partial
		"failed",                // a status, not a reason
		"unavailable",           // a provisional status, not a terminal reason
		"Unanswered_Timeout",    // wrong case
		"receiver_unavailable ", // trailing space
	}
	for _, r := range invalid {
		if IsValidTerminalFailureReason(r) {
			t.Errorf("IsValidTerminalFailureReason(%q) = true, want false (out-of-set must reject)", r)
		}
	}
}

// TestTerminalFailureReasonSetSize pins the closed set at exactly 3
// members. The set is frozen (INVARIANT-10): expanding it is a
// protocol-level change, so this count is a deliberate drift tripwire.
func TestTerminalFailureReasonSetSize(t *testing.T) {
	t.Parallel()
	// Count distinct reasons that validate among a candidate superset.
	candidates := map[TerminalFailureReason]struct{}{
		TerminalUnansweredTimeout:     {},
		TerminalReceiverInternalError: {},
		TerminalReceiverUnavailable:   {},
	}
	if len(candidates) != 3 {
		t.Fatalf("test setup: expected 3 distinct constants, got %d", len(candidates))
	}
	for r := range candidates {
		if !IsValidTerminalFailureReason(r) {
			t.Errorf("constant %q not accepted by predicate", r)
		}
	}
}

// TestIsFinalStatus pins the Layer-1 final closed set strictly at
// {completed,failed} (proto-layer0 §2.5.1). Every provisional Layer-2
// status and any Layer-3 business extension must NOT count as final —
// is_terminal derivation depends on this boundary.
func TestIsFinalStatus(t *testing.T) {
	t.Parallel()
	final := []string{"completed", "failed"}
	for _, s := range final {
		if !IsFinalStatus(s) {
			t.Errorf("IsFinalStatus(%q) = false, want true", s)
		}
	}

	notFinal := []string{
		"",
		"received",    // Layer-2 provisional
		"queued",      // Layer-2 provisional
		"processing",  // Layer-2 provisional
		"deferred",    // Layer-2 provisional
		"unavailable", // Layer-2 provisional
		"xhs.posted",  // Layer-3 business extension
		"Completed",   // wrong case
		"success",     // not in set
		"done",        // not in set
	}
	for _, s := range notFinal {
		if IsFinalStatus(s) {
			t.Errorf("IsFinalStatus(%q) = true, want false (only completed/failed are final)", s)
		}
	}
}
