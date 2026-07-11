package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// StepKindAndAudience contract — structure-only addressing checks.
func TestStepKindAndAudience_AudienceCardinality(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	toolID := registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)

	tests := []struct {
		name     string
		kind     message.Kind
		typ      string
		audience message.Audience
		reason   HarnessRejectReason
	}{
		{
			name:     "empty audience rejected",
			kind:     message.KindEvent,
			typ:      "agent.text",
			audience: nil,
			reason:   HarnessAudienceEmpty,
		},
		{
			name:     "event with multiple audience members ok (no cardinality)",
			kind:     message.KindEvent,
			typ:      "agent.text",
			audience: message.Audience{"x", "y"},
			reason:   "",
		},
		{
			name:     "response audience cardinality must be 1",
			kind:     message.KindResponse,
			typ:      "agent.text",
			audience: message.Audience{"x", "y"},
			reason:   HarnessResponseAudienceInvalid,
		},
		{
			name:     "request audience cardinality must be 1",
			kind:     message.KindRequest,
			typ:      "xhs.publish",
			audience: message.Audience{toolID, "extra"},
			reason:   HarnessRequestAudienceInvalid,
		},
		{
			name:     "request with cardinality-1 audience accepts (no liveness check)",
			kind:     message.KindRequest,
			typ:      "xhs.publish",
			audience: message.Audience{toolID},
			reason:   "",
		},
		{
			name:     "request to unregistered actor still accepts (reachability is a delivery-seam concern, 根4)",
			kind:     message.KindRequest,
			typ:      "xhs.publish",
			audience: message.Audience{"tool:ghost"},
			reason:   "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := &message.Envelope{
				ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
				Sender: message.Sender{ID: "agent:p"}, Kind: tc.kind, Type: tc.typ,
				Audience: tc.audience,
			}
			if tc.kind == message.KindResponse {
				e.ParentID = "p1"
			}
			out, err := runStep(t, newStepKindAndAudience, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q (detail=%q)", out.RejectReason, tc.reason, out.Detail)
			}
		})
	}
}

// Default TTL: a request with no caller-stamped expires_at gets the global
// fallback closure deadline (now + defaultRequestTTLMs).
func TestStepKindAndAudience_DefaultRequestTTL(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	toolID := registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)

	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{toolID},
	}
	out, err := runStep(t, newStepKindAndAudience, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !out.Continue() {
		t.Fatalf("unexpected reject %q", out.RejectReason)
	}
	if e.ExpiresAt == nil {
		t.Fatalf("expires_at = nil, want default TTL filled")
	}
	want := fixedNowMs + defaultRequestTTLMs
	if *e.ExpiresAt != want {
		t.Fatalf("expires_at = %d, want now+defaultTTL %d", *e.ExpiresAt, want)
	}
}

// Caller-supplied expires_at is preserved (not overwritten by the fallback).
func TestStepKindAndAudience_CallerExpiresPreserved(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	toolID := registerActor(t, cs, actor.ActorID("tool:xhs"), actor.KindTool)

	custom := fixedNowMs + 5000
	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{toolID}, ExpiresAt: &custom,
	}
	if _, err := runStep(t, newStepKindAndAudience, deps, context.Background(), e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.ExpiresAt == nil || *e.ExpiresAt != custom {
		t.Fatalf("expires_at = %v, want caller value %d preserved", e.ExpiresAt, custom)
	}
}

// NB: the core-type AllowOverride=false constraint branch (step_kind_audience:
// "!rule.AllowOverride && env.Kind != rule.DefaultKind") currently has no
// test subject, since both remaining core types (human.text, agent.text) are
// AllowOverride=true. Whether to keep that branch as additive-ready constraint
// machinery or remove it is an open decision; the reserved-bootstrap kind rule
// below covers the sibling enforcement path.

// Reserved bootstrap system.* type allows only kind=event.
func TestStepKindAndAudience_ReservedBootstrapKindRule(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender:   message.Sender{ID: actor.SystemActorID, Kind: actor.KindSystem},
		Kind:     message.KindRequest, // illegal: reserved system event must be event
		Type:     actor.ReservedSystemActorRegistered,
		Audience: message.Audience{"x"},
	}
	out, err := runStep(t, newStepKindAndAudience, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.RejectReason != HarnessKindNotAllowedForType {
		t.Fatalf("reason = %q, want kind_not_allowed_for_type", out.RejectReason)
	}
}
