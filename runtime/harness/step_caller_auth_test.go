package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// StepCallerAuth contract (addressing / ACL entry gate):
//   - missing caller context     → harness_engine_acl_denied
//   - caller bound to a different channel → harness_engine_acl_denied
//   - caller authenticated for this channel (or channel-agnostic) → accept
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
			name: "caller bound to a different channel rejects acl_denied",
			ctx: ctxWithCaller(context.Background(), caller{
				actorID: actor.ActorID("a"),
				chID:    channel.ID("other-channel"),
			}),
			reason: HarnessEngineACLDenied,
		},
		{
			name:   "caller bound to this channel accepts",
			ctx:    ctxCaller(actor.ActorID("a")),
			reason: "",
		},
		{
			name: "caller with empty channel (channel-agnostic edge) accepts",
			ctx: ctxWithCaller(context.Background(), caller{
				actorID: actor.ActorID("a"),
			}),
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
