package access

import "testing"

// TestIsValidFailureReason pins the frozen 5-value access-failure closed set
// (the resolve→authorize→execute→return pipeline, §2.2/§3.3): the five blessed
// reasons validate and round-trip their string; everything else — empty string,
// casing variants, partials, not-in-set words, a word from a different
// vocabulary (a message terminal reason), and trailing whitespace — is rejected.
func TestIsValidFailureReason(t *testing.T) {
	t.Parallel()

	valid := []FailureReason{
		ResourceNotFound,
		AlreadyExists,
		AccessDenied,
		DriverError,
		OutcomeUnknown,
	}
	for _, r := range valid {
		if !IsValidFailureReason(r) {
			t.Errorf("IsValidFailureReason(%q) = false, want true", r)
		}
		if r.String() != string(r) {
			t.Errorf("%q.String() = %q", r, r.String())
		}
	}

	invalid := []FailureReason{
		"",
		"not_found",            // partial
		"exists",               // partial
		"denied",               // partial
		"driver",               // partial
		"unknown",              // partial
		"malformed",            // door-level reject, deliberately NOT a FailureReason (§3.3)
		"receiver_unavailable", // a message terminal reason, different vocabulary
		"ResourceNotFound",     // wrong case
		"Access_Denied",        // wrong case
		"access_denied ",       // trailing space
	}
	for _, r := range invalid {
		if IsValidFailureReason(r) {
			t.Errorf("IsValidFailureReason(%q) = true, want false (out-of-set must reject)", r)
		}
	}
}

// TestFailureReasonSetSize pins the closed set at exactly 5 members. The set is
// frozen (it exhausts the access pipeline's failure stages): expanding it is a
// protocol-level change, so this count is a deliberate drift tripwire. It asserts
// on the unexported backing slice allFailureReasons directly (not a re-listed
// literal), so a 6th constant wired into the const block + allFailureReasons
// trips this test.
func TestFailureReasonSetSize(t *testing.T) {
	t.Parallel()
	if len(allFailureReasons) != 5 {
		t.Fatalf("FailureReason closed set drifted: allFailureReasons has %d members, want 5 — expanding it is a protocol-level change; update this sentinel deliberately", len(allFailureReasons))
	}
	for _, r := range allFailureReasons {
		if !IsValidFailureReason(r) {
			t.Errorf("backing-slice member %q not accepted by IsValidFailureReason", r)
		}
	}
}
