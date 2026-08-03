package harness

import "testing"

// HarnessRejectReason renders as its wire string — the reason IS the vocabulary
// callers match on, so String() must be the identity, not a display form.
func TestHarnessRejectReason_String(t *testing.T) {
	if got := HarnessEngineACLDenied.String(); got != "harness_engine_acl_denied" {
		t.Fatalf("String() = %q", got)
	}
}
