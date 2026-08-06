package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

// StepNormalize contract — default-fill + time-relation guard.
func TestStepNormalize_Defaults(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	e := &message.Envelope{
		ID:        "m1",
		TS:        fixedNowMs - 1000,
		ChannelID: testChannelID,
		Kind:      message.KindEvent,
		Type:      "agent.text",
	}
	out, err := runStep(t, newStepNormalize, deps, context.Background(), e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !out.Continue() {
		t.Fatalf("unexpected reject %q", out.RejectReason)
	}
	if e.Visibility != message.VisibilityPublic {
		t.Fatalf("visibility = %q, want public default", e.Visibility)
	}
	if e.CorrelationID != "m1" {
		t.Fatalf("correlation_id = %q, want self-rooted to id", e.CorrelationID)
	}
	if string(e.Payload) != "{}" {
		t.Fatalf("payload = %q, want {} baseline", e.Payload)
	}
	if e.Audience == nil || len(e.Audience) != 0 {
		t.Fatalf("event audience = %#v, want non-nil empty slice", e.Audience)
	}
	// ts_received is engine-owned and filled at the append sink (Chain.Write),
	// not by normalize — the full-Write contract is pinned in chain_test.go.
}

// TestStepNormalize_DoesNotFillKind pins the invariant: normalize NEVER
// fills kind for any type. kind is sender-required and enforced upstream by
// stepEnvelopeShape (empty kind → field_missing, short-circuit), so a
// kind-fill in normalize is dead code.
func TestStepNormalize_DoesNotFillKind(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	for _, typ := range []string{"agent.text", "human.text", "xhs.publish"} {
		typ := typ
		t.Run(typ, func(t *testing.T) {
			e := &message.Envelope{ID: "m1", TS: 1, ChannelID: testChannelID, Type: typ}
			if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
				t.Fatalf("err: %v", err)
			}
			if e.Kind != "" {
				t.Fatalf("kind = %q, want empty (normalize must not author kind for any type)", e.Kind)
			}
		})
	}
}

// ts is a caller-set CONSTRAINT, not a normalize fill: normalize must leave it
// untouched (step_envelope_shape rejects ts==0 before this step). Symmetric with
// DoesNotFillKind.
func TestStepNormalize_DoesNotFillTS(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	e := &message.Envelope{ID: "m1", TS: 42, ChannelID: testChannelID, Kind: message.KindEvent, Type: "agent.text"}
	if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.TS != 42 {
		t.Fatalf("ts = %d, want caller value 42 (normalize must not author ts)", e.TS)
	}
}

// response expires_at is cleared (provisional/final have no deadline semantics).
func TestStepNormalize_ResponseExpiresCleared(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	future := fixedNowMs + 100000
	e := &message.Envelope{
		ID: "m1", TS: fixedNowMs, ChannelID: testChannelID,
		Kind: message.KindResponse, Type: "agent.text", ParentID: "p1",
		ExpiresAt: &future,
	}
	if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.ExpiresAt != nil {
		t.Fatalf("expires_at = %v, want nil for response", *e.ExpiresAt)
	}
}

// time-relation guard: expires_at must be strictly after ts.
func TestStepNormalize_TimeRelationGuard(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	tests := []struct {
		name      string
		ts        int64
		expiresAt int64
		reason    HarnessRejectReason
	}{
		{"expires before ts", 1000, 500, HarnessTimeInvalid},
		{"expires equal ts", 1000, 1000, HarnessTimeInvalid},
		{"expires after ts ok", 1000, 1001, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exp := tc.expiresAt
			e := &message.Envelope{
				ID: "m1", TS: tc.ts, ChannelID: testChannelID,
				Kind: message.KindRequest, Type: "xhs.publish", ExpiresAt: &exp,
			}
			out, err := runStep(t, newStepNormalize, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if out.RejectReason != tc.reason {
				t.Fatalf("reason = %q, want %q", out.RejectReason, tc.reason)
			}
		})
	}
}

// nil envelope short-circuit: Chain.Write guards nil before the loop, so the
// step's own nil branch is only reachable by calling the step directly.
func TestStepNormalize_NilEnvelope(t *testing.T) {
	out, err := newStepNormalize(Deps{NowMs: func() int64 { return fixedNowMs }}).Run(context.Background(), nil)
	if err != nil || !out.Continue() {
		t.Fatalf("nil envelope normalize = out=%+v err=%v, want continue/no-error", out, err)
	}
}
