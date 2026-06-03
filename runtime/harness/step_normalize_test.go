package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepNormalize contract — default-fill + time-relation guard (proto-layer1 §2.4).
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
	// ts_received is engine-owned and filled at the append sink (Chain.Write),
	// not by normalize — the full-Write contract is pinned in chain_test.go.
}

// kind default-fill applies only to core types (business types must declare).
func TestStepNormalize_KindDefaultCoreOnly(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	t.Run("core type fills default kind", func(t *testing.T) {
		e := &message.Envelope{ID: "m1", TS: 1, ChannelID: testChannelID, Type: "agent.text"}
		if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
			t.Fatalf("err: %v", err)
		}
		if e.Kind != message.KindEvent {
			t.Fatalf("kind = %q, want event (core default)", e.Kind)
		}
	})

	t.Run("business type kind left empty", func(t *testing.T) {
		e := &message.Envelope{ID: "m1", TS: 1, ChannelID: testChannelID, Type: "xhs.publish"}
		if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
			t.Fatalf("err: %v", err)
		}
		if e.Kind != "" {
			t.Fatalf("kind = %q, want empty (substrate doesn't author business kind)", e.Kind)
		}
	})
}

// ts default-fill uses the engine clock when caller omits ts.
func TestStepNormalize_TSDefault(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)
	e := &message.Envelope{ID: "m1", ChannelID: testChannelID, Kind: message.KindEvent, Type: "agent.text"}
	if _, err := runStep(t, newStepNormalize, deps, context.Background(), e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.TS != fixedNowMs {
		t.Fatalf("ts = %d, want engine clock %d", e.TS, fixedNowMs)
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
