package message

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// TestEnvelopeRoundTripMinimal exercises the minimal valid envelope —
// only the L0 §3.1 invariant-required fields are populated.
func TestEnvelopeRoundTripMinimal(t *testing.T) {
	t.Parallel()
	src := Envelope{
		ID:         "msg-1",
		TS:         1700000000000,
		ChannelID:  "chan-A",
		Sender:     Sender{Kind: actor.KindAgent, ID: "agent-1", Name: ""},
		Kind:       KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: VisibilityPublic,
		Audience:   Audience{"*"},
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
		Sender:           Sender{Kind: actor.KindAgent, ID: "agent-1", Name: "Alice"},
		Kind:             KindRequest,
		Type:             "xhs.publish",
		Payload:          json.RawMessage(`{"body":{"title":"hi"}}`),
		ParentID:         "msg-parent",
		CorrelationID:    "corr-1",
		DocRefs:          &docs,
		Visibility:       VisibilityPublic,
		Audience:         Audience{"tool:xhs-adapter"},
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
			Sender:     Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:       KindEvent,
			Type:       "file.updated",
			Payload:    json.RawMessage(`{}`),
			Visibility: VisibilityPublic,
			Audience:   Audience{"*"},
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
	if got := len(actor.AllKinds); got != 4 {
		t.Errorf("actor.AllKinds len = %d, want 4", got)
	}
	if got := len(AllVisibilities); got != 3 {
		t.Errorf("AllVisibilities len = %d, want 3 (proto-layer0 §2.4 closed set: public/private/system)", got)
	}
	if got := len(HashInputFields); got != 14 {
		t.Errorf("HashInputFields len = %d, want 14", got)
	}
}

// TestEnvelopeFieldSet1To1WithSpec is the L0 §3 normative table guard.
//
// It marshals a fully populated envelope, lists the JSON keys present
// in the wire form, and asserts they are EXACTLY the union of:
//
//	ContentFields           (17 from L0 §2.1, with sender.* flattened)
//	DeliveryMetadataFields  (4 from L0 §2.5)
//	StoreDerivedFields      (2 from L2 §1.4.1)
//
// Any drift on either side (Go struct adds a JSON field but spec table
// doesn't, or spec exports a field but Go struct ignores it) trips the
// test. This is the verification artifact called out by m1.5-tickets.md
// §T1 acceptance criteria.
func TestEnvelopeFieldSet1To1WithSpec(t *testing.T) {
	t.Parallel()

	// Build the canonical "fully populated" envelope so every optional
	// field fires. Use real values (not zero) so omitempty does not hide
	// any field.
	notBefore := int64(1700000010000)
	expiresAt := int64(1700000020000)
	delivered := int64(1700000005000)
	failed := int64(1700000006000)
	docs := []string{"work/a.md"}

	env := Envelope{
		ID:               "msg-full",
		TS:               1700000000000,
		TSReceived:       1700000000123,
		ChannelID:        "chan-A",
		Sender:           Sender{Kind: actor.KindAgent, ID: "agent-1", Name: "Alice"},
		Kind:             KindRequest,
		Type:             "xhs.publish",
		Payload:          json.RawMessage(`{"x":1}`),
		ParentID:         "msg-parent",
		CorrelationID:    "corr-1",
		DocRefs:          &docs,
		Visibility:       VisibilityPublic,
		Audience:         Audience{"tool:xhs-adapter"},
		NotBefore:        &notBefore,
		ExpiresAt:        &expiresAt,
		DeliveredAt:      &delivered,
		DeliveryFailedAt: &failed,
		LastError:        "expired",
		Attempts:         2,
		IsTerminal:       true,
		Seq:              42,
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("unmarshal map: %v", err)
	}

	// Flatten sender.* to match ContentFields list (sender is a nested
	// object on the wire; spec lists kind/id/name as 3 dotted keys).
	gotKeys := make([]string, 0, len(asMap)+2)
	for k, v := range asMap {
		if k == "sender" {
			var s map[string]json.RawMessage
			if err := json.Unmarshal(v, &s); err != nil {
				t.Fatalf("unmarshal sender: %v", err)
			}
			for sk := range s {
				gotKeys = append(gotKeys, "sender."+sk)
			}
			continue
		}
		gotKeys = append(gotKeys, k)
	}

	wantKeys := append([]string{}, ContentFields...)
	wantKeys = append(wantKeys, DeliveryMetadataFields...)
	wantKeys = append(wantKeys, StoreDerivedFields...)

	sort.Strings(gotKeys)
	sort.Strings(wantKeys)

	if !reflect.DeepEqual(gotKeys, wantKeys) {
		extra := diff(gotKeys, wantKeys)
		missing := diff(wantKeys, gotKeys)
		t.Errorf("envelope JSON field set drifted from spec:\n  extra in struct (not in spec): %v\n  missing from struct (in spec): %v\n  full got:  %v\n  full want: %v",
			extra, missing, gotKeys, wantKeys)
	}
}

func TestCanonicalHashStableAcrossSenderIdentityTyping(t *testing.T) {
	t.Parallel()

	ts := int64(1700000000000)
	nb := int64(1700000060000)
	ex := int64(1700000999999)
	delivered := int64(1700000010000)
	failed := int64(1700000020000)
	refs := []string{"work/draft.md", "notes/raw.txt"}
	env := Envelope{
		ID:               "fixed-id",
		TS:               ts,
		TSReceived:       1700000000123,
		ChannelID:        "ch-1",
		Sender:           Sender{Kind: actor.KindAgent, ID: actor.ActorID("agent:alice"), Name: ""},
		Kind:             KindEvent,
		Type:             "agent.text",
		Payload:          json.RawMessage(`{"z":1,"a":{"y":2,"b":[3,1,2]},"m":"hi"}`),
		ParentID:         "parent-99",
		CorrelationID:    "corr-77",
		DocRefs:          &refs,
		Visibility:       VisibilityPublic,
		Audience:         Audience{"*", "agent:bob"},
		NotBefore:        &nb,
		ExpiresAt:        &ex,
		DeliveredAt:      &delivered,
		DeliveryFailedAt: &failed,
		LastError:        "ignored by hash",
		Attempts:         3,
		IsTerminal:       true,
		Seq:              42,
	}
	got, err := CanonicalHash(env)
	if err != nil {
		t.Fatalf("CanonicalHash: %v", err)
	}
	// Hash is computed over sender-provided fields only (proto-layer1
	// §2.3): sender.kind is excluded because it is runtime-derived
	// (forced overwrite from actor_registry at StepSenderConsistent).
	const wantHex = "616b69cbad986d12eebadc836ea8898e171010105361a9fef28336fe66a73917"
	if got != wantHex {
		t.Errorf("CanonicalHash mismatch:\n got  = %q\n want = %q", got, wantHex)
	}
}

// TestContentFieldsCount17 asserts the ContentFields slice matches the
// L0 §2.1 table cardinality (17 envelope content fields).
func TestContentFieldsCount17(t *testing.T) {
	t.Parallel()
	if got := len(ContentFields); got != 17 {
		t.Errorf("ContentFields len = %d, want 17 (L0 §2.1 table cardinality)", got)
	}
	// Every entry must be either a top-level key or a sender.* dotted
	// key — no other shape is valid.
	for _, f := range ContentFields {
		if strings.Contains(f, ".") && !strings.HasPrefix(f, "sender.") {
			t.Errorf("ContentFields entry %q has unexpected dotted shape", f)
		}
	}
}

// TestDeliveryMetadataFieldsCount4 asserts the L0 §2.5 cardinality.
func TestDeliveryMetadataFieldsCount4(t *testing.T) {
	t.Parallel()
	if got := len(DeliveryMetadataFields); got != 4 {
		t.Errorf("DeliveryMetadataFields len = %d, want 4 (L0 §2.5 table cardinality)", got)
	}
}

// TestStoreDerivedFieldsCount2 asserts the L2 §1.4.1 cardinality.
func TestStoreDerivedFieldsCount2(t *testing.T) {
	t.Parallel()
	if got := len(StoreDerivedFields); got != 2 {
		t.Errorf("StoreDerivedFields len = %d, want 2 (L2 §1.4.1 store-derived cols)", got)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

// diff returns elements in a not in b.
func diff(a, b []string) []string {
	seen := make(map[string]struct{}, len(b))
	for _, s := range b {
		seen[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := seen[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
