package access

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// TestGrantRoundTrip pins structural fidelity of the set operand: GranteeKind +
// Grantee + Ops survive a JSON round trip byte-for-byte, for both grantee kinds.
func TestGrantRoundTrip(t *testing.T) {
	t.Parallel()
	cases := map[string]Grant{
		"actor grantee": {
			GranteeKind: GranteeActor,
			Grantee:     actor.ActorID("agent:bob"),
			Ops:         []Operation{OpRead, OpWrite},
		},
		"members grantee (no id)": {
			GranteeKind: GranteeMembers,
			Ops:         []Operation{OpRead},
		},
	}
	for name, src := range cases {
		src := src
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := roundTrip(t, src)
			if !reflect.DeepEqual(got, src) {
				t.Errorf("Grant round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
			}
		})
	}
}

// TestGranteeKindClosedSet pins the grantee principal-class closed set: exactly
// {actor, members} parse, near-misses are rejected, and the backing slice has
// cardinality 2 (drift sentinel — adding a kind, e.g. the deferred `group`, is a
// protocol revision and must consciously bump this).
func TestGranteeKindClosedSet(t *testing.T) {
	t.Parallel()
	if len(allGranteeKinds) != 2 {
		t.Fatalf("GranteeKind cardinality = %d, want 2 (protocol revision required to grow the set)", len(allGranteeKinds))
	}
	for _, k := range allGranteeKinds {
		got, ok := ParseGranteeKind(string(k))
		if !ok || got != k {
			t.Errorf("ParseGranteeKind(%q) = (%q, %v), want (%q, true)", k, got, ok, k)
		}
	}
	for _, raw := range []string{"", "Actor", "MEMBERS", "member", "group", "actor "} {
		if _, ok := ParseGranteeKind(raw); ok {
			t.Errorf("ParseGranteeKind(%q) accepted, want reject (closed set)", raw)
		}
	}
}

// TestGrantOpsExpressible pins that the Ops field can express the full wire
// shape the substrate authz manager needs: ∅ = revoke (both nil and empty
// non-nil are valid revoke spellings and round-trip preserving nil-ness), and a
// multi-op grant preserves order and duplicates (the proto type carries the wire
// form faithfully; day-1 narrowing Ops ⊆ {read,write} is the runtime门's
// ValidateGrant policy, NOT proto — §3.2.1/§9).
func TestGrantOpsExpressible(t *testing.T) {
	t.Parallel()
	cases := map[string][]Operation{
		"nil ops (∅ = revoke)":   nil,
		"empty ops (∅ = revoke)": {},
		"single":                 {OpRead},
		"multi (order kept)":     {OpWrite, OpRead},
		"duplicates kept":        {OpRead, OpRead},
	}
	for name, ops := range cases {
		ops := ops
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			src := Grant{GranteeKind: GranteeActor, Grantee: actor.ActorID("agent:bob"), Ops: ops}
			got := roundTrip(t, src)
			if !reflect.DeepEqual(got.Ops, ops) {
				t.Errorf("Ops round-trip mismatch: got %#v (nil=%v), want %#v (nil=%v)",
					got.Ops, got.Ops == nil, ops, ops == nil)
			}
		})
	}
}

// TestGrantWireKeys pins the wire key set per grantee kind: an actor grant emits
// exactly {grantee_kind, grantee, ops}; a members grant emits exactly
// {grantee_kind, ops} (grantee omitted — the set needs no id). A drift here
// means a domain attribute leaked into the substrate-typed grant operand, or
// the absent/present signal of Grantee broke.
func TestGrantWireKeys(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		grant Grant
		want  []string
	}{
		"actor grant": {
			grant: Grant{GranteeKind: GranteeActor, Grantee: actor.ActorID("agent:bob"), Ops: []Operation{OpRead}},
			want:  []string{"grantee", "grantee_kind", "ops"},
		},
		"members grant omits grantee": {
			grant: Grant{GranteeKind: GranteeMembers, Ops: []Operation{OpRead}},
			want:  []string{"grantee_kind", "ops"},
		},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(tc.grant)
			if err != nil {
				t.Fatalf("marshal grant: %v", err)
			}
			var keys map[string]json.RawMessage
			if err := json.Unmarshal(raw, &keys); err != nil {
				t.Fatalf("unmarshal grant: %v", err)
			}
			got := make([]string, 0, len(keys))
			for k := range keys {
				got = append(got, k)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Grant wire keys = %v, want %v", got, tc.want)
			}
		})
	}
}
