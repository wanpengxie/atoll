package message

import (
	"errors"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// TestEnvelopeRoundTripMinimal exercises the minimal valid envelope: only
// the always-present (non-omitempty) fields are populated. It pins the wire
// contract that a bare envelope survives marshal/unmarshal without the
// substrate inventing or dropping fields (substrate must not lie about the
// message it carries).
func TestEnvelopeRoundTripMinimal(t *testing.T) {
	t.Parallel()
	src := Envelope{
		ID:         "msg-1",
		TS:         1700000000000,
		ChannelID:  "chan-A",
		Sender:     Sender{Kind: actor.KindAgent, ID: "agent-1"},
		Kind:       KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: VisibilityPublic,
		Audience:   Audience{"agent:channel-agent"},
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("minimal envelope round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestEnvelopeRoundTripFullyPopulated populates every field, including all
// three tri-state slots (ParentID/CorrelationID empty-vs-set, ExpiresAt
// pointer), and asserts a byte-for-byte equal struct after a JSON round
// trip. This is the structural fidelity contract for the whole envelope.
func TestEnvelopeRoundTripFullyPopulated(t *testing.T) {
	t.Parallel()
	expiresAt := int64(1700000020000)
	src := Envelope{
		ID:            "msg-full",
		TS:            1700000000000,
		TSReceived:    1700000000123,
		ChannelID:     "chan-A",
		Sender:        Sender{Kind: actor.KindAgent, ID: "agent-1"},
		Kind:          KindRequest,
		Type:          "example.publish",
		Payload:       json.RawMessage(`{"body":{"title":"hi"}}`),
		ParentID:      "msg-parent",
		CorrelationID: "corr-1",
		Visibility:    VisibilityPrivate,
		Audience:      Audience{"tool:example", "agent:bob"},
		ExpiresAt:     &expiresAt,
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("envelope round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestExpiresAtTriState pins the documented tri-state of ExpiresAt
// (*int64): nil means NULL on the wire, a set pointer means the timestamp.
func TestExpiresAtTriState(t *testing.T) {
	t.Parallel()
	mk := func(p *int64) Envelope {
		return Envelope{
			ID:         "id",
			TS:         1,
			ChannelID:  "c",
			Sender:     Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:       KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: VisibilityPublic,
			Audience:   Audience{"agent:channel-agent"},
			ExpiresAt:  p,
		}
	}
	val := int64(1700000099999)
	cases := map[string]*int64{
		"nil pointer (NULL on wire)": nil,
		"set pointer":                &val,
	}
	for name, p := range cases {
		p := p
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := roundTrip(t, mk(p))
			if (got.ExpiresAt == nil) != (p == nil) {
				t.Fatalf("ExpiresAt nil-ness mismatch: got %v, want %v", got.ExpiresAt, p)
			}
			if p != nil && *got.ExpiresAt != *p {
				t.Errorf("ExpiresAt value mismatch: got %d, want %d", *got.ExpiresAt, *p)
			}
		})
	}
	// nil ExpiresAt must be omitted entirely from the wire (omitempty),
	// not serialized as `"expires_at":null`.
	raw, err := json.Marshal(mk(nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "expires_at") {
		t.Errorf("nil ExpiresAt should be omitted from wire, got: %s", raw)
	}
}

// TestSenderIsStructuralIdentityOnly pins that Sender carries exactly the
// two structural-identity fields {kind,id} and nothing else. Mutable /
// presentation attributes (display name, role) are domain, not substrate
// (inode vs filename). A drift here means a domain attribute leaked into
// the substrate envelope.
func TestSenderIsStructuralIdentityOnly(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(Sender{Kind: actor.KindAgent, ID: "agent-1"})
	if err != nil {
		t.Fatalf("marshal sender: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("unmarshal sender: %v", err)
	}
	got := make([]string, 0, len(keys))
	for k := range keys {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"id", "kind"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Sender wire keys = %v, want %v (structural identity only; name/role are domain)", got, want)
	}
}

// TestEnvelopeFieldSet1To1WithContentFields is the normative table guard:
// the JSON keys emitted by a fully populated envelope (with sender.*
// flattened) must be EXACTLY the ContentFields list. Drift on either side
// — Go struct grows a field the list lacks, or the list names a field the
// struct dropped — trips this test.
func TestEnvelopeFieldSet1To1WithContentFields(t *testing.T) {
	t.Parallel()
	expiresAt := int64(1700000020000)
	env := Envelope{
		ID:            "msg-full",
		TS:            1700000000000,
		TSReceived:    1700000000123,
		ChannelID:     "chan-A",
		Sender:        Sender{Kind: actor.KindAgent, ID: "agent-1"},
		Kind:          KindRequest,
		Type:          "example.publish",
		Payload:       json.RawMessage(`{"x":1}`),
		ParentID:      "msg-parent",
		CorrelationID: "corr-1",
		Visibility:    VisibilityPublic,
		Audience:      Audience{"tool:example"},
		ExpiresAt:     &expiresAt,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("unmarshal map: %v", err)
	}

	gotKeys := make([]string, 0, len(asMap)+1)
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

	wantKeys := append([]string{}, contentFields...)
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Errorf("envelope JSON field set drifted from ContentFields:\n  extra in struct: %v\n  missing from struct: %v\n  got:  %v\n  want: %v",
			diff(gotKeys, wantKeys), diff(wantKeys, gotKeys), gotKeys, wantKeys)
	}
}

// TestContentFieldsShape guards the ContentFields list's own internal
// shape: every entry is either a top-level key or a sender.* dotted key
// (no other nesting is part of the wire contract).
func TestContentFieldsShape(t *testing.T) {
	t.Parallel()
	if len(contentFields) == 0 {
		t.Fatal("contentFields is empty")
	}
	for _, f := range contentFields {
		if strings.Contains(f, ".") && !strings.HasPrefix(f, "sender.") {
			t.Errorf("ContentFields entry %q has unexpected dotted shape", f)
		}
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

// ---------------------------------------------------------------------
// Wire field-set closure — UnmarshalJSON fail-closed tests.
// ---------------------------------------------------------------------

const validEnvelopeJSON = `{"id":"m1","ts":1,"channel_id":"ch","sender":{"kind":"agent","id":"a"},"kind":"event","type":"agent.text","payload":{},"visibility":"public","audience":["x"]}`

// TestEnvelopeUnmarshalRejectsUnknownTopLevelField pins the closed-set
// invariant: any decode of an envelope carrying a top-level key outside the
// closed field set fails with a typed UnknownFieldError — no binding-side
// plumbing involved.
func TestEnvelopeUnmarshalRejectsUnknownTopLevelField(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"typo/smuggle key": `{"id":"m1","ts":1,"channel_id":"ch","sender":{"id":"a"},"kind":"event","type":"t","bogus":1}`,
		// Store-derived columns are NOT wire proto fields:
		// submitting them is exactly the drift this check fail-closes on.
		"store-derived seq":         `{"id":"m1","ts":1,"channel_id":"ch","sender":{"id":"a"},"kind":"event","type":"t","seq":9}`,
		"store-derived is_terminal": `{"id":"m1","ts":1,"channel_id":"ch","sender":{"id":"a"},"kind":"event","type":"t","is_terminal":1}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var e Envelope
			err := json.Unmarshal([]byte(raw), &e)
			var uf UnknownFieldError
			if !errorsAs(err, &uf) {
				t.Fatalf("err = %v, want UnknownFieldError", err)
			}
			if len(uf.Keys) != 1 {
				t.Fatalf("keys = %v, want exactly one offending key", uf.Keys)
			}
		})
	}
}

// TestEnvelopeUnmarshalAllKnownKeys decodes an envelope using every legal
// top-level key and asserts acceptance — the closed set derived from the
// struct tags admits precisely the struct's own fields.
func TestEnvelopeUnmarshalAllKnownKeys(t *testing.T) {
	t.Parallel()
	full := `{"id":"m1","ts":1,"ts_received":2,"channel_id":"ch","sender":{"kind":"agent","id":"a"},"kind":"event","type":"t","payload":{},"parent_id":"p","correlation_id":"c","visibility":"public","audience":["x"],"expires_at":5}`
	var e Envelope
	if err := json.Unmarshal([]byte(full), &e); err != nil {
		t.Fatalf("full-key envelope should decode: %v", err)
	}
	if e.ID != "m1" || e.TSReceived != 2 || e.ExpiresAt == nil || *e.ExpiresAt != 5 {
		t.Fatalf("decoded fields lost: %+v", e)
	}
}

// TestEnvelopeUnmarshalNestedUnknownAllowed documents the deliberate scope:
// the unknown-field check is TOP-LEVEL only. An unknown key inside the nested
// sender object is the nested vocabulary's concern, and payload is opaque by
// axiom.
func TestEnvelopeUnmarshalNestedUnknownAllowed(t *testing.T) {
	t.Parallel()
	raw := `{"id":"m1","ts":1,"channel_id":"ch","sender":{"kind":"agent","id":"a","x":1},"kind":"event","type":"t","payload":{"anything":"goes"}}`
	var e Envelope
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("nested unknown keys must not trip the top-level check: %v", err)
	}
}

// TestEnvelopeUnmarshalMultipleUnknownSorted pins deterministic reporting:
// all offending keys, sorted, independent of map iteration order.
func TestEnvelopeUnmarshalMultipleUnknownSorted(t *testing.T) {
	t.Parallel()
	raw := `{"id":"m1","zzz":1,"aaa":2,"kind":"event"}`
	var e Envelope
	err := json.Unmarshal([]byte(raw), &e)
	var uf UnknownFieldError
	if !errorsAs(err, &uf) {
		t.Fatalf("err = %v, want UnknownFieldError", err)
	}
	if !reflect.DeepEqual(uf.Keys, []string{"aaa", "zzz"}) {
		t.Fatalf("keys = %v, want [aaa zzz] (all offenders, sorted)", uf.Keys)
	}
}

// TestEnvelopeUnmarshalMalformed pins that malformed JSON still surfaces as a
// plain decode error, not an UnknownFieldError.
func TestEnvelopeUnmarshalMalformed(t *testing.T) {
	t.Parallel()
	var e Envelope
	err := json.Unmarshal([]byte("{not-json"), &e)
	if err == nil {
		t.Fatalf("malformed JSON should error")
	}
	var uf UnknownFieldError
	if errorsAs(err, &uf) {
		t.Fatalf("malformed JSON must not classify as UnknownFieldError")
	}
}

// TestEnvelopeUnmarshalValidRoundTrip re-asserts the fidelity contract still
// holds through the custom UnmarshalJSON (the shadow-type decode must not
// change any field semantics).
func TestEnvelopeUnmarshalValidRoundTrip(t *testing.T) {
	t.Parallel()
	var e Envelope
	if err := json.Unmarshal([]byte(validEnvelopeJSON), &e); err != nil {
		t.Fatalf("valid envelope: %v", err)
	}
	if e.Sender.ID != "a" || e.Kind != KindEvent || len(e.Audience) != 1 {
		t.Fatalf("decode lost fields: %+v", e)
	}
}

// errorsAs is a local alias so this file keeps its stdlib-only import set
// tight (errors is imported here on first use).
func errorsAs(err error, target any) bool { return errors.As(err, target) }
