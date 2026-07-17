package accessdoor

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// TestPublicSetArmDayOneMatrix is the PUBLIC-INTEGRATION half of the
// authorization-decay-law layer (期11 spec §2 item 4): unlike decay_test.go
// (which calls d.invoke directly, past the sealed Minter, to reach the full
// {read,write,set,delete} range), this test goes through the actual public
// surface — New/Mint/AccessHandle.Invoke — so it ALSO exercises
// day1OpsOverreach (the {read,write} share narrowing, ingress.go) — the two
// checks the spec calls out as independent ("day-1 share 缩窄...两条独立检查"):
// a grant can pass the escalation check here and still be reachable only
// within day-1's own ⊆{read,write} ceiling.
func TestPublicSetArmDayOneMatrix(t *testing.T) {
	cs := newDecayStore(t)
	m, err := New(Deps{
		Registry:  cs.Resources,
		Drivers:   DriverTable{resourcespec.KindKV: cs.KVDriver},
		Authority: decayMembership{registry: cs.Registry},
		Overlay:   &fakeGrantOverlay{},
		State:     cs.State,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	aliceID := seedMember(t, cs, "alice")
	bobID := seedMember(t, cs, "bob")
	carolID := seedMember(t, cs, "carol")
	daveID := seedMember(t, cs, "dave")

	// alice creates r1 — day-1 reachable birth: creator's full-rights grant is
	// what every day-1 set/revoke below rides.
	alice := m.Mint(accessStamp(aliceID))
	out, err := alice.Create(context.Background(), "r1", resourcespec.CreateSpec{Kind: resourcespec.KindKV}, []byte("v"))
	mustAccept(t, out, err)

	t.Run("day-1 reachable grant: {read,write}", func(t *testing.T) {
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: bobID, Ops: []access.Operation{access.OpRead, access.OpWrite}}
		out, err := alice.Invoke(context.Background(), access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)
	})

	t.Run("revoke", func(t *testing.T) {
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: bobID, Ops: nil}
		out, err := alice.Invoke(context.Background(), access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)

		hasRead, err := cs.Resources.ActorAllows(context.Background(), bobID, "r1", access.OpRead)
		if err != nil {
			t.Fatalf("ActorAllows: %v", err)
		}
		if hasRead {
			t.Fatalf("bob's read should have been revoked")
		}
	})

	t.Run("non-holder cannot grant read it does not itself hold", func(t *testing.T) {
		// carol holds {write,set} on r1 (seeded directly — day-1's door-level
		// share narrowing governs what the DOOR can grant on the wire, not
		// what the resource's grant table can independently hold), but NOT
		// read. She can invoke set (she holds set-right) but the escalation
		// check must deny granting an op she does not herself hold.
		seedActorGrant(t, cs, "r1", carolID, access.OpWrite, access.OpSet)
		carolH := m.Mint(accessStamp(carolID))

		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: daveID, Ops: []access.Operation{access.OpRead}}
		out, err := carolH.Invoke(context.Background(), access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})
}
