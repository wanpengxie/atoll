package access

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
)

// TestGrantRoundTrip pins structural fidelity of the set operand: Grantee +
// Ops survive a JSON round trip byte-for-byte.
func TestGrantRoundTrip(t *testing.T) {
	t.Parallel()
	src := Grant{
		Grantee: actor.ActorID("agent:bob"),
		Ops:     []Operation{OpRead, OpWrite},
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("Grant round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
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
			src := Grant{Grantee: actor.ActorID("agent:bob"), Ops: ops}
			got := roundTrip(t, src)
			if !reflect.DeepEqual(got.Ops, ops) {
				t.Errorf("Ops round-trip mismatch: got %#v (nil=%v), want %#v (nil=%v)",
					got.Ops, got.Ops == nil, ops, ops == nil)
			}
		})
	}
}

// TestGrantWireKeys pins that Grant emits exactly {grantee, ops} on the wire and
// nothing else — a drift here means a domain attribute leaked into the
// substrate-typed grant operand.
func TestGrantWireKeys(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(Grant{Grantee: actor.ActorID("agent:bob"), Ops: []Operation{OpRead}})
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
	want := []string{"grantee", "ops"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Grant wire keys = %v, want %v (substrate-typed operand only; no domain attrs)", got, want)
	}
}
