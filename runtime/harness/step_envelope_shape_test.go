package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// StepEnvelopeShape contract — wire-level structural guards (proto-layer1 §2.2).
func TestStepEnvelopeShape_FieldMissing(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	base := func() *message.Envelope { return validEvent("m1", "a") }

	tests := []struct {
		name   string
		mutate func(e *message.Envelope)
	}{
		{"missing id", func(e *message.Envelope) { e.ID = "" }},
		{"missing channel_id", func(e *message.Envelope) { e.ChannelID = "" }},
		{"missing kind", func(e *message.Envelope) { e.Kind = "" }},
		{"missing type", func(e *message.Envelope) { e.Type = "" }},
		{"missing sender", func(e *message.Envelope) { e.Sender.ID = "" }},
		{"missing ts", func(e *message.Envelope) { e.TS = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := base()
			tc.mutate(e)
			out, err := runStep(t, newStepEnvelopeShape, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != HarnessEnvelopeFieldMissing {
				t.Fatalf("reason = %q, want field_missing", out.RejectReason)
			}
		})
	}
}

// The hardened, UNCONDITIONAL channel_mismatch: env.channel_id != deps.ChannelID
// must reject regardless of caller context — even with NO caller attached. This
// is the substrate-truth-integrity guard, not an ACL concern.
func TestStepEnvelopeShape_ChannelMismatchUnconditional(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	e := validEvent("m1", "a")
	e.ChannelID = channel.ID("foreign-channel")

	// No caller in context — the guard must still fire.
	out, err := runStep(t, newStepEnvelopeShape, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.RejectReason != HarnessChannelMismatch {
		t.Fatalf("reason = %q, want channel_mismatch (must fire without caller context)", out.RejectReason)
	}
}

func TestStepEnvelopeShape_KindVisibilityAudienceResponse(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	tests := []struct {
		name   string
		mutate func(e *message.Envelope)
		reason HarnessRejectReason // "" = accept
	}{
		{
			name:   "illegal kind rejected (closed set)",
			mutate: func(e *message.Envelope) { e.Kind = message.Kind("gossip") },
			reason: HarnessKindInvalid,
		},
		{
			name:   "illegal visibility rejected (closed set)",
			mutate: func(e *message.Envelope) { e.Visibility = message.Visibility("secret") },
			reason: HarnessVisibilityInvalid,
		},
		{
			name:   "empty visibility legal here (normalize defaults later)",
			mutate: func(e *message.Envelope) { e.Visibility = "" },
			reason: "",
		},
		{
			name:   "valid private visibility accepts",
			mutate: func(e *message.Envelope) { e.Visibility = message.VisibilityPrivate },
			reason: "",
		},
		{
			name:   "audience wildcard forbidden",
			mutate: func(e *message.Envelope) { e.Audience = message.Audience{actor.ActorID("*")} },
			reason: HarnessAudienceWildcardForbidden,
		},
		{
			name: "response without parent_id rejected",
			mutate: func(e *message.Envelope) {
				e.Kind = message.KindResponse
				e.ParentID = ""
			},
			reason: HarnessResponseMissingParent,
		},
		{
			name: "response with parent_id passes shape",
			mutate: func(e *message.Envelope) {
				e.Kind = message.KindResponse
				e.ParentID = "p1"
			},
			reason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent("m1", "a")
			tc.mutate(e)
			out, err := runStep(t, newStepEnvelopeShape, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q", out.RejectReason, tc.reason)
			}
		})
	}
}

// (The unknown-top-level-field tests moved to protocol/message — the §7.3
// fail-closed check rides Envelope.UnmarshalJSON now, not a harness step.)
