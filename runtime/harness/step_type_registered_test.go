package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepTypeRegistered contract — substrate is TYPE-AGNOSTIC. It only guards the
// reserved system.* namespace (anti-forgery: per-name authority). Everything
// else — business, core, AND actor.* introspection — passes (actor.* is a
// lib/introspect convention, not substrate-gated).
func TestStepTypeRegistered(t *testing.T) {
	tests := []struct {
		name       string
		typ        string
		senderID   actor.ActorID
		senderKind actor.Kind
		reason     HarnessRejectReason
	}{
		// Business types: any vocabulary passes (no registry lookup).
		{"arbitrary business type passes", "xhs.publish", "tool:xhs", actor.KindTool, ""},
		{"typo'd business type still passes", "totally.made.up.type", "agent:p", actor.KindAgent, ""},
		{"core type passes", "agent.text", "agent:p", actor.KindAgent, ""},

		// system.* reserved bootstrap types: only the channel system actor.
		{
			name:       "reserved system type from non-system sender rejected",
			typ:        actor.ReservedSystemActorRegistered,
			senderID:   "agent:p",
			senderKind: actor.KindAgent,
			reason:     HarnessReservedTypeUnauthorizedSender,
		},
		{
			name:       "reserved system type from system actor accepted",
			typ:        actor.ReservedSystemActorRegistered,
			senderID:   actor.SystemActorID,
			senderKind: actor.KindSystem,
			reason:     "",
		},
		{
			name:       "non-reserved system.* namespace not installable",
			typ:        "system.made.up",
			senderID:   actor.SystemActorID,
			senderKind: actor.KindSystem,
			reason:     HarnessTypeUnknown,
		},

		// actor.* introspection: NOT substrate-gated — passes like any type.
		{
			name:       "actor.describe passes (type-agnostic; introspection is a lib convention)",
			typ:        "actor.describe",
			senderID:   "agent:p",
			senderKind: actor.KindAgent,
			reason:     "",
		},
		{
			name:       "any actor.* passes (no substrate whitelist)",
			typ:        "actor.made_up",
			senderID:   "agent:p",
			senderKind: actor.KindAgent,
			reason:     "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent("m1", tc.senderID)
			e.Type = tc.typ
			e.Sender = message.Sender{ID: tc.senderID, Kind: tc.senderKind}
			// type step takes no deps; pass empty Deps via constructor.
			out, err := runStep(t, newStepTypeRegistered, Deps{}, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}
