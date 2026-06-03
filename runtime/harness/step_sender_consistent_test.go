package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepSenderConsistent contract (A3 真实 — registry is the single identity truth).
func TestStepSenderConsistent(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	registerActor(t, cs, actor.ActorID("agent:p"), actor.KindAgent)
	deregisterActor(t, cs, actor.ActorID("agent:p")) // for the deregistered case below
	registerActor(t, cs, actor.ActorID("agent:live"), actor.KindAgent)

	tests := []struct {
		name   string
		caller actor.ActorID
		sender message.Sender
		reason HarnessRejectReason
	}{
		{
			name:   "no caller context defensively acl_denied",
			caller: "",
			sender: message.Sender{ID: "agent:live"},
			reason: HarnessEngineACLDenied,
		},
		{
			name:   "sender.id != caller rejects sender_mismatch",
			caller: "agent:live",
			sender: message.Sender{ID: "agent:other"},
			reason: HarnessSenderMismatch,
		},
		{
			name:   "sender not in registry rejects deregistered",
			caller: "agent:ghost",
			sender: message.Sender{ID: "agent:ghost"},
			reason: HarnessSenderDeregistered,
		},
		{
			name:   "deregistered sender rejects deregistered",
			caller: "agent:p",
			sender: message.Sender{ID: "agent:p"},
			reason: HarnessSenderDeregistered,
		},
		{
			name:   "caller-provided kind conflicting with registry rejects kind_mismatch",
			caller: "agent:live",
			sender: message.Sender{ID: "agent:live", Kind: actor.KindTool},
			reason: HarnessSenderKindMismatch,
		},
		{
			name:   "live sender, no provided kind accepts",
			caller: "agent:live",
			sender: message.Sender{ID: "agent:live"},
			reason: "",
		},
		{
			name:   "live sender, matching provided kind accepts",
			caller: "agent:live",
			sender: message.Sender{ID: "agent:live", Kind: actor.KindAgent},
			reason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent("m1", tc.sender.ID)
			e.Sender = tc.sender
			ctx := context.Background()
			if tc.caller != "" {
				ctx = ctxCaller(tc.caller)
			}
			out, err := runStep(t, newStepSenderConsistent, deps, ctx, e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}

// On accept the registry kind is force-overwritten onto the envelope (truth wins).
func TestStepSenderConsistent_ForcedKindOverwrite(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)

	e := validEvent("m1", "tool:xhs")
	e.Sender = message.Sender{ID: "tool:xhs"} // no kind provided
	out, err := runStep(t, newStepSenderConsistent, deps, ctxCaller("tool:xhs"), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !out.Continue() {
		t.Fatalf("unexpected reject %q", out.RejectReason)
	}
	if e.Sender.Kind != actor.KindTool {
		t.Fatalf("sender.kind = %q, want registry truth tool", e.Sender.Kind)
	}
}
