package message

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// TestEnvelopeRoundTripMinimal exercises the minimal valid envelope: only
// the always-present (non-omitempty) fields are populated. It pins the wire
// contract that a bare envelope survives marshal/unmarshal without the
// substrate inventing or dropping fields (A3: substrate does not lie about
// the message it carries).
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
		Audience:      Audience{"tool:xhs", "agent:bob"},
		ExpiresAt:     &expiresAt,
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("envelope round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestExpiresAtTriState pins the documented tri-state of ExpiresAt
// (*int64): nil means NULL on the wire, a set pointer means the timestamp.
// (proto §"Tri-state semantics").
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
		Audience:      Audience{"tool:xhs"},
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

	wantKeys := append([]string{}, ContentFields...)
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
	if len(ContentFields) == 0 {
		t.Fatal("ContentFields is empty")
	}
	for _, f := range ContentFields {
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
