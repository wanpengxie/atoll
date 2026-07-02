package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// StepSenderConsistent contract (A3 真实 — the pen weld is the single identity
// truth; there is no registry lookup left in this step —
// incarnation-dynamics-build-spec §3.2/§1.4). The two registry-backed
// rejection cases this file used to exercise ("sender not in registry",
// "deregistered sender rejects deregistered") are gone: that correctness now
// lives one layer up, in livePen.IsLive() (platform/internal/link/livepen.go,
// ErrWriterNotLive), which runs before the chain and cannot be exercised from
// this package's step-isolation harness. See livepen_test.go for its coverage.
func TestStepSenderConsistent(t *testing.T) {
	deps := Deps{}

	tests := []struct {
		name   string
		ctx    context.Context
		sender message.Sender
		reason HarnessRejectReason
	}{
		{
			name:   "no caller context defensively acl_denied",
			ctx:    context.Background(),
			sender: message.Sender{ID: "agent:live"},
			reason: HarnessEngineACLDenied,
		},
		{
			name:   "sender.id != caller rejects sender_mismatch",
			ctx:    ctxCallerKind("agent:live", actor.KindAgent),
			sender: message.Sender{ID: "agent:other"},
			reason: HarnessSenderMismatch,
		},
		{
			name:   "caller-provided kind conflicting with welded kind rejects kind_mismatch",
			ctx:    ctxCallerKind("agent:live", actor.KindAgent),
			sender: message.Sender{ID: "agent:live", Kind: actor.KindTool},
			reason: HarnessSenderKindMismatch,
		},
		{
			name:   "live sender, no provided kind accepts",
			ctx:    ctxCallerKind("agent:live", actor.KindAgent),
			sender: message.Sender{ID: "agent:live"},
			reason: "",
		},
		{
			name:   "live sender, matching provided kind accepts",
			ctx:    ctxCallerKind("agent:live", actor.KindAgent),
			sender: message.Sender{ID: "agent:live", Kind: actor.KindAgent},
			reason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent("m1", tc.sender.ID)
			e.Sender = tc.sender
			out, err := runStep(t, newStepSenderConsistent, deps, tc.ctx, e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}

// On accept the welded kind is force-overwritten onto the envelope (truth wins).
func TestStepSenderConsistent_ForcedKindOverwrite(t *testing.T) {
	deps := Deps{}

	e := validEvent("m1", "tool:xhs")
	e.Sender = message.Sender{ID: "tool:xhs"} // no kind provided
	out, err := runStep(t, newStepSenderConsistent, deps, ctxCallerKind("tool:xhs", actor.KindTool), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !out.Continue() {
		t.Fatalf("unexpected reject %q", out.RejectReason)
	}
	if e.Sender.Kind != actor.KindTool {
		t.Fatalf("sender.kind = %q, want welded truth tool", e.Sender.Kind)
	}
}
