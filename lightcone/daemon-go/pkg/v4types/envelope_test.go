package v4types

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestEnvelopeRoundTripMinimal exercises the minimal valid envelope —
// only the L0 §3.1 invariant-required fields are populated.
func TestEnvelopeRoundTripMinimal(t *testing.T) {
	t.Parallel()
	src := Envelope{
		ID:         "msg-1",
		TS:         1700000000000,
		ChannelID:  "chan-A",
		Sender:     Sender{Kind: SenderAgent, ID: "agent-1", Name: ""},
		Kind:       KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: VisibilityPublic,
		Audience:   []string{"*"},
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got.Audience, src.Audience) {
		t.Errorf("audience: got %v, want %v", got.Audience, src.Audience)
	}
	if got.Kind != KindEvent {
		t.Errorf("kind round-trip lost: %q", got.Kind)
	}
	if got.Sender.Name != "" {
		t.Errorf("Sender.Name should round-trip as empty string, got %q", got.Sender.Name)
	}
}

// TestEnvelopeRoundTripFullyPopulated exercises every optional field,
// covering the tri-state behaviours for DocRefs / NotBefore / ExpiresAt.
func TestEnvelopeRoundTripFullyPopulated(t *testing.T) {
	t.Parallel()
	notBefore := int64(1700000010000)
	expiresAt := int64(1700000020000)
	delivered := int64(1700000005000)
	failed := int64(1700000006000)
	docs := []string{"work/a.md", "work/b.md"}

	src := Envelope{
		ID:               "msg-full",
		TS:               1700000000000,
		TSReceived:       1700000000123,
		ChannelID:        "chan-A",
		Sender:           Sender{Kind: SenderAgent, ID: "agent-1", Name: "Alice"},
		Kind:             KindRequest,
		Type:             "xhs.publish.requested",
		Payload:          json.RawMessage(`{"body":{"title":"hi"}}`),
		ParentID:         "msg-parent",
		CorrelationID:    "corr-1",
		DocRefs:          &docs,
		Visibility:       VisibilityPublic,
		Audience:         []string{"tool:xhs-adapter"},
		NotBefore:        &notBefore,
		ExpiresAt:        &expiresAt,
		DeliveredAt:      &delivered,
		DeliveryFailedAt: &failed,
		LastError:        "expired",
		Attempts:         2,
		IsTerminal:       true,
		Seq:              42,
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("envelope round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestDocRefsTriState makes sure the three legal DocRefs states survive
// JSON marshal/unmarshal cleanly.
func TestDocRefsTriState(t *testing.T) {
	t.Parallel()
	mk := func(refs *[]string) Envelope {
		return Envelope{
			ID:         "id",
			TS:         1,
			ChannelID:  "c",
			Sender:     Sender{Kind: SenderAgent, ID: "a"},
			Kind:       KindEvent,
			Type:       "file.updated",
			Payload:    json.RawMessage(`{}`),
			Visibility: VisibilityPublic,
			Audience:   []string{"*"},
			DocRefs:    refs,
		}
	}
	empty := []string{}
	cases := map[string]*[]string{
		"nil pointer (NULL on wire)":     nil,
		"explicit empty slice ([] wire)": &empty,
		"populated slice":                {"work/x.md"},
	}
	for name, refs := range cases {
		t.Run(name, func(t *testing.T) {
			src := mk(refs)
			got := roundTrip(t, src)
			if (got.DocRefs == nil) != (refs == nil) {
				t.Errorf("DocRefs nil-ness mismatch: got %v, want %v", got.DocRefs, refs)
			}
			if refs != nil && !reflect.DeepEqual(*got.DocRefs, *refs) {
				t.Errorf("DocRefs content mismatch: got %v, want %v", *got.DocRefs, *refs)
			}
		})
	}
}

// TestAllEnumSets covers the three exported enum slice helpers — the
// counts double as guard against accidental closed-set drift.
func TestAllEnumSets(t *testing.T) {
	t.Parallel()
	if got := len(AllKinds); got != 3 {
		t.Errorf("AllKinds len = %d, want 3", got)
	}
	if got := len(AllSenderKinds); got != 4 {
		t.Errorf("AllSenderKinds len = %d, want 4", got)
	}
	if got := len(AllVisibilities); got != 3 {
		t.Errorf("AllVisibilities len = %d, want 3", got)
	}
	if got := len(HashInputFields); got != 14 {
		t.Errorf("HashInputFields len = %d, want 14", got)
	}
}

func roundTrip(t *testing.T, src Envelope) Envelope {
	t.Helper()
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, raw)
	}
	return got
}
