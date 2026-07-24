package accessdoor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// This file is the STORE-SEEDED half of the authorization-decay-law test
// layer (期11 spec §2 item 4): the escalation matrix needs several ops
// holding DIFFERENT truth values on the same resource at once (e.g. an actor
// holding {set} but not {read}), which the single-bool fakeRegistry
// (fakes_test.go) cannot express without becoming a second, drift-prone
// implementation of the union formula. A real per-channel sqlite store gives
// us that state shape for free and exercises the PRODUCTION
// ActorAllows/MembersAllow/SetGrant paths effectiveOps is defined over.
//
// These tests call d.invoke directly (past the sealed Minter, like
// fakes_test.go's newDoor helper) — bypassing boundHandle's day1OpsOverreach
// {read,write} narrowing on purpose: the "转移需满权"/"创建者满权回归" cases
// need to exercise the full {read,write,set,delete} range the RAW door
// supports, which is a decision-tree property independent of the day-1 wire
// narrowing (a SEPARATE check, tested at the public-integration layer in
// decay_public_test.go).

// newDecayStore opens a real per-channel sqlite store in a temp dir.
func newDecayStore(t *testing.T) *store.ChannelStores {
	t.Helper()
	cs, err := store.OpenChannel(context.Background(), "C-decay", filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// decayMembership adapts the declared-control read face the same way runtime.OpenChannel's
// (unexported) channelMembershipCheck does: Lookup + Record.IsActive, NOT
// Exists — a deregistered actor still has a row. Duplicated here rather than
// imported because that type lives unexported in package runtime (assembly
// stays confined to the root package).
type decayMembership struct {
	registry storespec.DeclaredControlReader
}

func (m decayMembership) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	_, ok, err := m.registry.LookupDeclaredActive(ctx, id)
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	if !ok {
		return storespec.ActorControlRow{}, false, nil
	}
	p := storespec.NewServerPlacement()
	return storespec.ActorControlRow{ID: id, CurrentDeclVersion: 1, Placement: p}, true, nil
}

func (m decayMembership) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}

func (m decayMembership) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	row, ok, err := m.LookupActive(ctx, id)
	if err != nil {
		return storespec.ResourceActorFacts{}, err
	}
	if !ok {
		return storespec.ResourceActorFacts{}, nil
	}
	return storespec.ResourceActorFacts{
		Active: row.ID != "",
		Owner:  row.Role == storespec.RoleOwner,
	}, nil
}

// newDecayDoor builds a bare door directly over a real store (past-the-Minter,
// same discipline as fakes_test.go's newDoor).
func newDecayDoor(cs *store.ChannelStores) *door {
	return &door{deps: Deps{
		Registry:  cs.Resources,
		Drivers:   DriverTable{resourcespec.KindKV: cs.KVDriver},
		Authority: decayMembership{registry: cs.Declared},
		State:     cs.State,
	}}
}

// seedResource creates id with creator's atomic birth-time full-rights grant
// (resourcespec.Registry.Create's own contract — not hand-seeded).
func seedResource(t *testing.T, cs *store.ChannelStores, id resource.ResourceID, creator actor.ActorID) {
	t.Helper()
	if err := cs.Resources.Create(context.Background(), id, resourcespec.KindKV, creator, "", "", nil, resourcespec.ResourceBirthPlan{}); err != nil {
		t.Fatalf("seed resource %q: %v", id, err)
	}
}

// seedActorGrant installs a grant directly (bypassing the door and its day-1
// narrowing) so a test can put a resource into a state day-1's own set arm
// could never reach on its own — e.g. an actor holding {set} without {read}.
func seedActorGrant(t *testing.T, cs *store.ChannelStores, id resource.ResourceID, grantee actor.ActorID, ops ...access.Operation) {
	t.Helper()
	if err := cs.Resources.SetGrant(context.Background(), id, access.Grant{GranteeKind: access.GranteeActor, Grantee: grantee, Ops: ops}); err != nil {
		t.Fatalf("seed actor grant %q/%q: %v", id, grantee, err)
	}
}

// seedMembersGrant installs a GranteeMembers-kind grant directly.
func seedMembersGrant(t *testing.T, cs *store.ChannelStores, id resource.ResourceID, ops ...access.Operation) {
	t.Helper()
	if err := cs.Resources.SetGrant(context.Background(), id, access.Grant{GranteeKind: access.GranteeMembers, Ops: ops}); err != nil {
		t.Fatalf("seed members grant %q: %v", id, err)
	}
}

// seedMember registers id as an active channel member.
func seedMember(t *testing.T, cs *store.ChannelStores, id actor.ActorID) actor.ActorID {
	t.Helper()
	result, err := cs.DeclAdmission.AdmitDeclared(context.Background(), storespec.AdmitBundle{
		Kind: actor.KindAgent, SourceDeclID: string(id), Class: "agent",
		Placement: storespec.NewServerPlacement(), CreatedAt: 1,
	})
	if err != nil {
		t.Fatalf("seed member %q: %v", id, err)
	}
	return result.ID
}

var fullObjectOps = []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete}

// --- 自升拒 / 同谋拒 ---

func TestSetArmEscalationDenied(t *testing.T) {
	t.Run("self-escalation: caller lacks an op it grants to another actor", func(t *testing.T) {
		cs := newDecayStore(t)
		d := newDecayDoor(cs)
		seedResource(t, cs, "r1", "creator")
		seedActorGrant(t, cs, "r1", "alice", access.OpRead, access.OpSet) // alice: read+set, NOT write

		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: []access.Operation{access.OpRead, access.OpWrite}}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("self-escalation: caller grants itself an op it does not hold", func(t *testing.T) {
		cs := newDecayStore(t)
		d := newDecayDoor(cs)
		seedResource(t, cs, "r1", "creator")
		seedActorGrant(t, cs, "r1", "alice", access.OpRead, access.OpSet) // no delete

		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "alice", Ops: []access.Operation{access.OpRead, access.OpSet, access.OpDelete}}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("collusion: the grantee's OWN independent rights do not widen the caller's grantable set", func(t *testing.T) {
		cs := newDecayStore(t)
		d := newDecayDoor(cs)
		seedResource(t, cs, "r1", "creator")
		seedActorGrant(t, cs, "r1", "alice", access.OpRead, access.OpSet) // alice: no write
		seedActorGrant(t, cs, "r1", "bob", access.OpWrite)                // bob already, separately, holds write

		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: []access.Operation{access.OpRead, access.OpWrite}}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})
}

// --- 撤权恒过 ---

func TestSetArmRevokeAlwaysLegal(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedActorGrant(t, cs, "r1", "alice", access.OpSet) // set-right ONLY — no read, no write
	seedActorGrant(t, cs, "r1", "bob", access.OpRead, access.OpWrite)

	g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: nil} // ∅ ⊆ anything
	out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
	mustAccept(t, out, err)

	allowed, err := cs.Resources.ActorAllows(context.Background(), "bob", "r1", access.OpRead)
	if err != nil {
		t.Fatalf("ActorAllows: %v", err)
	}
	if allowed {
		t.Fatalf("bob's grant should have been revoked")
	}
}

// --- 转移需满权 ---

func TestSetArmTransferNeedsFullRights(t *testing.T) {
	t.Run("partial rights cannot grant full rights (transfer denied)", func(t *testing.T) {
		cs := newDecayStore(t)
		d := newDecayDoor(cs)
		seedResource(t, cs, "r1", "creator")
		seedActorGrant(t, cs, "r1", "alice", access.OpRead, access.OpWrite, access.OpSet) // missing delete

		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: fullObjectOps}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("full rights transfer succeeds: set(Y,full) + set(self,∅)", func(t *testing.T) {
		cs := newDecayStore(t)
		d := newDecayDoor(cs)
		seedResource(t, cs, "r1", "alice") // creator ⟹ birth-time full rights

		give := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: fullObjectOps}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, give)
		mustAccept(t, out, err)

		selfRevoke := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "alice", Ops: nil}
		out, err = d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, selfRevoke)
		mustAccept(t, out, err)

		bobFull, err := cs.Resources.ActorAllows(context.Background(), "bob", "r1", access.OpDelete)
		if err != nil {
			t.Fatalf("ActorAllows: %v", err)
		}
		if !bobFull {
			t.Fatalf("bob should hold full rights after transfer")
		}
	})
}

// --- members行参与衰减基 ---

func TestSetArmMembersRowParticipatesInBasis(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedMembersGrant(t, cs, "r1", access.OpRead, access.OpSet) // any current member: read+set
	alice := seedMember(t, cs, "alice")                        // alice has NO direct actor entry

	t.Run("current member grants within the members row's ops", func(t *testing.T) {
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: []access.Operation{access.OpRead}}
		out, err := d.invoke(context.Background(), alice, access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)
	})

	t.Run("current member cannot grant beyond the members row's ops", func(t *testing.T) {
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "carol", Ops: []access.Operation{access.OpWrite}}
		out, err := d.invoke(context.Background(), alice, access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.AccessDenied)
	})
}

// --- 创建者满权回归 ---

func TestSetArmCreatorFullRightsRegression(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "alice") // Create's OWN atomic full-rights grant, not hand-seeded

	for _, ops := range [][]access.Operation{
		{access.OpRead},
		{access.OpRead, access.OpWrite},
		fullObjectOps,
		nil, // revoke
	} {
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: ops}
		out, err := d.invoke(context.Background(), "alice", access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)
	}
}

// --- actor 移出频道后 members 权利立即不再计入（三处共用 effectiveOps 的头一处；
//     Stat/List 的同断言随 §3 补，见 effectiveops.go 的文档注释） ---

func TestEffectiveOpsDropsDeregisteredMembersRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedMembersGrant(t, cs, "r1", access.OpRead, access.OpWrite)
	alice := seedMember(t, cs, "alice")

	eff, err := d.effectiveOps(context.Background(), alice, "r1")
	if err != nil {
		t.Fatalf("effectiveOps: %v", err)
	}
	if !eff[access.OpRead] || !eff[access.OpWrite] {
		t.Fatalf("active member should hold the members row's ops, got %v", eff)
	}

	if _, err := cs.Cascade.EndCascade(context.Background(), storespec.CascadeBundle{IDs: []actor.ActorID{alice}, EndedAt: 2}); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	eff, err = d.effectiveOps(context.Background(), alice, "r1")
	if err != nil {
		t.Fatalf("effectiveOps after deregister: %v", err)
	}
	if eff[access.OpRead] || eff[access.OpWrite] {
		t.Fatalf("deregistered actor must not retain members-row rights, got %v", eff)
	}
}

// TestStatDropsDeregisteredMembersRights is effectiveOps' SECOND locus (期11
// spec §3.6/§2 item 2's three-loci contract): Stat must stop echoing
// members-row rights the instant the caller deregisters, exactly like the
// set arm above.
func TestStatDropsDeregisteredMembersRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedMembersGrant(t, cs, "r1", access.OpRead, access.OpWrite)
	alice := seedMember(t, cs, "alice")

	res, err := d.stat(context.Background(), alice, "r1")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if len(res.Ops) == 0 {
		t.Fatalf("active member should see the members row's ops via Stat, got %+v", res)
	}

	if _, err := cs.Cascade.EndCascade(context.Background(), storespec.CascadeBundle{IDs: []actor.ActorID{alice}, EndedAt: 2}); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	res, err = d.stat(context.Background(), alice, "r1")
	if err != nil {
		t.Fatalf("stat after deregister: %v", err)
	}
	if res.Reject != QueryNotFound {
		t.Fatalf("deregistered actor's Stat must masquerade as not_found (zero rights), got %+v", res)
	}
}

// TestListDropsDeregisteredMembersRights is effectiveOps' THIRD locus: List's
// per-row projection must stop counting members-row rights the instant the
// caller deregisters, exactly like the set arm and Stat above.
func TestListDropsDeregisteredMembersRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedMembersGrant(t, cs, "r1", access.OpRead, access.OpWrite)
	alice := seedMember(t, cs, "alice")

	page, err := d.list(context.Background(), alice, ListQuery{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != "r1" {
		t.Fatalf("active member should see r1 via List, got %+v", page.Entries)
	}

	if _, err := cs.Cascade.EndCascade(context.Background(), storespec.CascadeBundle{IDs: []actor.ActorID{alice}, EndedAt: 2}); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	page, err = d.list(context.Background(), alice, ListQuery{})
	if err != nil {
		t.Fatalf("list after deregister: %v", err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("deregistered actor must not see r1 via List (members row no longer counts), got %+v", page.Entries)
	}
}

func TestSetArmDropsDeregisteredMembersRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	seedMembersGrant(t, cs, "r1", access.OpRead, access.OpSet)
	alice := seedMember(t, cs, "alice")

	g1 := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "bob", Ops: []access.Operation{access.OpRead}}
	out, err := d.invoke(context.Background(), alice, access.OpSet, "r1", nil, g1)
	mustAccept(t, out, err) // active member: members row still counts

	if _, err := cs.Cascade.EndCascade(context.Background(), storespec.CascadeBundle{IDs: []actor.ActorID{alice}, EndedAt: 2}); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	g2 := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "carol", Ops: []access.Operation{access.OpRead}}
	out, err = d.invoke(context.Background(), alice, access.OpSet, "r1", nil, g2)
	mustVerdict(t, out, err, access.AccessDenied) // deregistered: members row no longer counts
}
