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

// This file is the STORE-SEEDED half of the membership-decay test layer: the
// membrane-uniform formula (PM-D1) makes membership itself the read/write
// right, so the decay law under test is "a deregistered actor immediately
// holds ZERO ops" — asserted at every locus that consults the formula
// (invoke / Stat / List), over the PRODUCTION store paths a real channel
// runs. PM-D3's creator-delete distinction is asserted here too (real
// created_by column, not a fake's canned meta).
//
// These tests call d.invoke/d.stat/d.list directly (past the sealed Minter,
// like fakes_test.go's newDoor helper).

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
	registry storespec.ActorRegistryStore
}

func (m decayMembership) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	_, ok, err := m.registry.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ResourceActorFacts{}, err
	}
	// Owner is a genesis-pointer judgement made at the Platform door; this
	// runtime-level fixture never fabricates one.
	return storespec.ResourceActorFacts{Active: true}, nil
}

// newDecayDoor builds a bare door directly over a real store (past-the-Minter,
// same discipline as fakes_test.go's newDoor).
func newDecayDoor(cs *store.ChannelStores) *door {
	return &door{deps: Deps{
		Registry:  cs.Resources,
		Drivers:   DriverTable{resourcespec.KindKV: cs.KVDriver},
		Authority: decayMembership{registry: cs.Actors},
		State:     cs.State,
	}}
}

// seedResource creates id under creator (the created_by column IS the PM-D3
// delete predicate — resourcespec.Registry.Create's own contract).
func seedResource(t *testing.T, cs *store.ChannelStores, id resource.ResourceID, creator actor.ActorID) {
	t.Helper()
	if err := cs.Resources.Create(context.Background(), id, resourcespec.KindKV, creator, "", "", nil); err != nil {
		t.Fatalf("seed resource %q: %v", id, err)
	}
}

// seedMember registers id as an active channel member.
func seedMember(t *testing.T, cs *store.ChannelStores, id actor.ActorID) actor.ActorID {
	t.Helper()
	record, err := cs.Actors.Insert(context.Background(), storespec.ActorDraft{
		Kind: actor.KindAgent, SourceDeclID: string(id),
		Definition: storespec.ActorDefinition{Class: "agent"},
		Placement:  storespec.NewServerPlacement(), CreatedAt: 1,
	})
	if err != nil {
		t.Fatalf("seed member %q: %v", id, err)
	}
	return record.ID
}

// --- 成员资格即读写权（PM-D1），移出频道后立即失效：invoke 处 ---

func TestInvokeDropsDeregisteredMemberRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	alice := seedMember(t, cs, "alice")

	out, err := d.invoke(context.Background(), alice, access.OpRead, "r1", nil)
	mustAccept(t, out, err) // active member: membership IS the right — no grant needed, ever

	if err := cs.Actors.Deregister(context.Background(), []actor.ActorID{alice}, 2); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	out, err = d.invoke(context.Background(), alice, access.OpRead, "r1", nil)
	mustVerdict(t, out, err, access.AccessDenied) // deregistered: zero ops, immediately
}

// --- 同一条衰减律的第二处：Stat（零权伪装 not_found）---

func TestStatDropsDeregisteredMemberRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	alice := seedMember(t, cs, "alice")

	res, err := d.stat(context.Background(), alice, "r1")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if len(res.Ops) == 0 {
		t.Fatalf("active member should see ops via Stat (membrane-uniform), got %+v", res)
	}

	if err := cs.Actors.Deregister(context.Background(), []actor.ActorID{alice}, 2); err != nil {
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

// --- 第三处：List（非成员整页不可见）---

func TestListDropsDeregisteredMemberRights(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	seedResource(t, cs, "r1", "creator")
	alice := seedMember(t, cs, "alice")

	page, err := d.list(context.Background(), alice, ListQuery{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != "r1" {
		t.Fatalf("active member should see r1 via List, got %+v", page.Entries)
	}

	if err := cs.Actors.Deregister(context.Background(), []actor.ActorID{alice}, 2); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	page, err = d.list(context.Background(), alice, ListQuery{})
	if err != nil {
		t.Fatalf("list after deregister: %v", err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("deregistered actor must not see r1 via List, got %+v", page.Entries)
	}
}

// --- PM-D3：delete 单独一档 = creator ∨ 频道 owner（真 created_by 列上断言）---

func TestDeleteRequiresCreatorOrChannelOwner(t *testing.T) {
	cs := newDecayStore(t)
	d := newDecayDoor(cs)
	alice := seedMember(t, cs, "alice")
	bob := seedMember(t, cs, "bob")
	seedResource(t, cs, "r1", alice)

	// A plain member (not creator, not owner) reads and writes freely (PM-D1)…
	out, err := d.invoke(context.Background(), bob, access.OpRead, "r1", nil)
	mustAccept(t, out, err)
	out, err = d.invoke(context.Background(), bob, access.OpWrite, "r1", []byte("v"))
	mustAccept(t, out, err)

	// …but cannot delete what another member created.
	out, err = d.invoke(context.Background(), bob, access.OpDelete, "r1", nil)
	mustVerdict(t, out, err, access.AccessDenied)

	// The creator can.
	out, err = d.invoke(context.Background(), alice, access.OpDelete, "r1", nil)
	mustAccept(t, out, err)
}
