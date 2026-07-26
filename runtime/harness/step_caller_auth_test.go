package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// StepCallerAuth contract (addressing / ACL entry gate):
//   - missing caller context → harness_engine_acl_denied
//   - a welded caller        → accept
//
// There is no channel comparison left to test: the mint takes no channel, so a
// caller welded to "another channel" is not a constructible state.
func TestStepCallerAuth(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	tests := []struct {
		name   string
		ctx    context.Context
		reason HarnessRejectReason // "" = accept
	}{
		{
			name:   "no caller context rejects acl_denied",
			ctx:    context.Background(),
			reason: HarnessEngineACLDenied,
		},
		{
			name:   "welded caller accepts",
			ctx:    ctxCaller(actor.ActorID("a")),
			reason: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runStep(t, newStepCallerAuth, deps, tc.ctx, validEvent("m1", "a"))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}
