package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// StepEnvelopeShape contract — wire-level structural guards.
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

// The channel stamp has ONE source: this harness's own binding constant. The
// pen writes it, so nothing downstream compares it against anything — the guard
// that used to do so was the harness checking its own output. What remains is
// the property that made the guard unnecessary: a caller cannot supply a
// channel at all, and what lands on the row is deps.ChannelID.
func TestChannelStampComesFromTheHarnessBindingAlone(t *testing.T) {
	cs := newTestStore(t)
	_, mint, err := New(testDeps(t, cs))
	if err != nil {
		t.Fatal(err)
	}
	// One admission, one write. There is no pen to hold between the two writes
	// below — each states the verdict it is writing under, which is the whole
	// point of the seam.
	admission := storespec.IdentityAdmission{ID: "agent:a", Kind: actor.KindAgent}

	stamped := &message.Envelope{
		ID: "stamped", TS: fixedNowMs - 1000, Kind: message.KindEvent,
		Type: "agent.text", Audience: message.Audience{"agent:b"},
	}
	result, err := mint.WriteAdmitted(context.Background(), admission, stamped)
	if err != nil || !result.Accepted() {
		t.Fatalf("write: result=%+v err=%v", result, err)
	}
	if stamped.ChannelID != testChannelID {
		t.Fatalf("channel stamp = %q, want the harness binding %q", stamped.ChannelID, testChannelID)
	}

	// A caller-supplied channel is refused outright, loudly — it is never
	// quietly corrected and never reaches a shape comparison.
	forged := &message.Envelope{
		ID: "forged", TS: fixedNowMs - 1000, ChannelID: channel.ID("foreign-channel"),
		Kind: message.KindEvent, Type: "agent.text", Audience: message.Audience{"agent:b"},
	}
	result, err = mint.WriteAdmitted(context.Background(), admission, forged)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if result.RejectReason != HarnessIdentityNotCallerSettable {
		t.Fatalf("reason = %q, want identity_not_caller_settable", result.RejectReason)
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
			name:   "private visibility rejected",
			mutate: func(e *message.Envelope) { e.Visibility = message.Visibility("private") },
			reason: HarnessVisibilityInvalid,
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

// (Unknown-top-level-field tests live in protocol/message — that fail-closed
// check rides Envelope.UnmarshalJSON, not a harness step.)

// Payload wellformedness (guard 2): non-empty payload must be valid JSON and
// not the null literal — truth is append-only, so a malformed payload
// admitted once is a protocol-illegal row forever. Empty payload stays legal
// (Step Normalize fills the {} default).
func TestStepEnvelopeShape_PayloadWellformedness(t *testing.T) {
	cs := newTestStore(t)
	deps := testDeps(t, cs)

	cases := map[string]struct {
		payload string
		reject  bool
	}{
		"invalid JSON":       {payload: `{bad`, reject: true},
		"null literal":       {payload: `null`, reject: true},
		"padded null":        {payload: ` null `, reject: true},
		"empty object":       {payload: `{}`, reject: false},
		"empty (normalized)": {payload: ``, reject: false},
		"non-object JSON":    {payload: `"opaque"`, reject: false}, // opaque by axiom; only wellformedness + non-null are shape law
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := validEvent(message.ID("m-"+name), "a")
			e.Payload = []byte(tc.payload)
			out, err := runStep(t, newStepEnvelopeShape, deps, context.Background(), e)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if tc.reject && out.RejectReason != HarnessPayloadInvalid {
				t.Fatalf("reason = %q, want harness_payload_invalid", out.RejectReason)
			}
			if !tc.reject && !out.Continue() {
				t.Fatalf("reason = %q, want accept", out.RejectReason)
			}
		})
	}
}
