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

// This file pins what happens to a resource a SHORT-LIVED actor created once
// that actor is gone — the fork-child case: a parent forks a worker, the worker
// creates a channel-scoped resource as part of its job, and then the worker
// ends while the resource is still something the channel needs.
//
// The law (PM-D1/PM-D3, membrane-uniform):
//
//   - existence is CHANNEL-scoped, so the resource does not die with its
//     creator;
//   - read/write reach is MEMBERSHIP itself — creator death neither widens nor
//     narrows any member's reach (there is no grant ledger to inherit or leak);
//   - delete stays creator ∨ channel owner (PM-D3, the created_by column).
//     Creator death does NOT hand delete to the collective; the channel owner
//     root is the fallback delete authority, which is what makes an orphan a
//     removable end state rather than an immortal one;
//   - the dead creator's own capability is closed at the welded authority.

// successionFacts is the resource-policy projection with an OWNER: decayMembership
// (decay_test.go) hard-codes Owner=false, and the whole point here is what the
// owner root reaches after the creator is gone.
type successionFacts struct {
	registry storespec.ActorRegistryStore
	owner    actor.ActorID
}

func (f successionFacts) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	_, ok, err := f.registry.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ResourceActorFacts{}, err
	}
	return storespec.ResourceActorFacts{Active: true, Owner: id == f.owner}, nil
}

// successionAuthority is the welded capability an assembled handle carries. It
// admits exactly while its actor is an active registry row — the same verdict
// actorctl.IdentityAuthority makes through Controller.AuthorActive, reproduced
// here because assembly lives outside this package.
type successionAuthority struct {
	registry storespec.ActorRegistryStore
	id       actor.ActorID
}

func (a successionAuthority) ActorID() actor.ActorID { return a.id }

func (a successionAuthority) Admit() error {
	_, ok, err := a.registry.LookupActive(context.Background(), a.id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrAuthorInactive
	}
	return nil
}

func newSuccessionStore(t *testing.T) *store.ChannelStores {
	t.Helper()
	cs, err := store.OpenChannel(
		context.Background(),
		"C-succession",
		filepath.Join(t.TempDir(), "ch.sqlite"),
		store.OpenOptions{},
		nil,
	)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// newSuccessionMinter assembles the door through its real public face, so the
// handles below are the same capability objects a body receives.
func newSuccessionMinter(t *testing.T, cs *store.ChannelStores, owner actor.ActorID) AccessMinter {
	t.Helper()
	minted, err := New(Deps{
		Registry:  cs.Resources,
		Drivers:   DriverTable{resourcespec.KindKV: cs.KVDriver},
		Authority: successionFacts{registry: cs.Actors, owner: owner},
		State:     cs.State,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return minted
}

// newSuccessionDoor is the same assembly reached past the sealed minter, for
// the assertions that must observe the DOOR's own liveness gate rather than the
// welded handle's (same past-the-minter discipline as decay_test.go).
func newSuccessionDoor(cs *store.ChannelStores, owner actor.ActorID) *door {
	return &door{deps: Deps{
		Registry:  cs.Resources,
		Drivers:   DriverTable{resourcespec.KindKV: cs.KVDriver},
		Authority: successionFacts{registry: cs.Actors, owner: owner},
		State:     cs.State,
	}}
}

// seedSuccessionActor registers one active actor of an explicit kind (the owner
// must be a human, the fork child an agent — seedMember mints agents only).
func seedSuccessionActor(
	t *testing.T,
	cs *store.ChannelStores,
	seed string,
	kind actor.Kind,
) actor.ActorID {
	t.Helper()
	draft := storespec.ActorDraft{
		Kind:       kind,
		Definition: storespec.ActorDefinition{Class: string(kind)},
		Placement:  storespec.NewServerPlacement(),
		CreatedAt:  1,
	}
	// The two admission classes are structurally exclusive: a human is admitted
	// by login principal, a declaration-backed actor by declaration source.
	if kind == actor.KindHuman {
		draft.Principal = seed + "@example.test"
	} else {
		draft.SourceDeclID = seed
	}
	record, err := cs.Actors.Insert(context.Background(), draft)
	if err != nil {
		t.Fatalf("seed actor %q: %v", seed, err)
	}
	return record.ID
}

func endSuccessionActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID, at int64) {
	t.Helper()
	if err := cs.Actors.Deregister(context.Background(), []actor.ActorID{id}, at); err != nil {
		t.Fatalf("deregister %q: %v", id, err)
	}
}

// TestForkChildResourceOutlivesItsCreatorUnderOwnerRoot is the whole chain in
// one pass: a forked worker creates a resource, the worker ends, and the
// resource is still there, still reachable — by the channel owner, through the
// root axiom.
func TestForkChildResourceOutlivesItsCreatorUnderOwnerRoot(t *testing.T) {
	ctx := context.Background()
	cs := newSuccessionStore(t)
	owner := seedSuccessionActor(t, cs, "owner-human", actor.KindHuman)
	minter := newSuccessionMinter(t, cs, owner)

	child := seedSuccessionActor(t, cs, "parent-fork-child", actor.KindAgent)
	childHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: child})
	ownerHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: owner})

	const id = resource.ResourceID("resource:fork-child-work")
	out, err := childHandle.Create(ctx, id, CreateSpec{Kind: KindKV}, []byte("work product"))
	mustAccept(t, out, err)

	// created_by is recorded — the PM-D3 delete predicate AND audit provenance.
	stat, err := childHandle.Stat(ctx, id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Meta.CreatedBy != child {
		t.Fatalf("created_by = %q, want the forking child %q", stat.Meta.CreatedBy, child)
	}

	// Owner root, while the creator is still alive: full reach.
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := ownerHandle.Invoke(ctx, op, id, []byte("owner"))
		mustAccept(t, out, err)
	}

	// The fork child ends. Its work product must not end with it.
	endSuccessionActor(t, cs, child, 2)

	if _, exists, err := cs.Resources.Resolve(ctx, id); err != nil || !exists {
		t.Fatalf("resource vanished with its creator (exists=%v, err=%v) — existence is channel-scoped", exists, err)
	}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := ownerHandle.Invoke(ctx, op, id, []byte("after death"))
		mustAccept(t, out, err)
	}
	ownerStat, err := ownerHandle.Stat(ctx, id)
	if err != nil {
		t.Fatalf("owner Stat after creator death: %v", err)
	}
	if len(ownerStat.Ops) != len(objectOps) {
		t.Fatalf("owner effective ops = %v, want the full object set — the projection must not disagree with the root axiom", ownerStat.Ops)
	}

	// The dead creator's own capability is closed AT THE HANDLE: nothing can
	// ever again present that welded authority.
	out, err = childHandle.Invoke(ctx, access.OpRead, id, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	out, err = childHandle.Create(ctx, "resource:posthumous", CreateSpec{Kind: KindKV}, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	if _, exists, err := cs.Resources.Resolve(ctx, "resource:posthumous"); err != nil || exists {
		t.Fatalf("a dead creator landed a new resource (exists=%v, err=%v)", exists, err)
	}

	// Owner root is still the DELETE authority (PM-D3's 兜底 half), which is
	// what makes "creator-less resource" a removable end state.
	out, err = ownerHandle.Invoke(ctx, access.OpDelete, id, nil)
	mustAccept(t, out, err)
	if _, exists, err := cs.Resources.Resolve(ctx, id); err != nil || exists {
		t.Fatalf("owner delete did not land (exists=%v, err=%v)", exists, err)
	}
}

// TestCreatorDeathNeitherWidensNorNarrowsMemberReach pins the PM-D1/PM-D3
// succession law from an ordinary member's seat: the member's read/write reach
// is membership itself — identical before and after the creator dies — and
// delete stays denied to a non-creator member across the death (dying does NOT
// hand delete to the collective; the owner root is the fallback).
func TestCreatorDeathNeitherWidensNorNarrowsMemberReach(t *testing.T) {
	ctx := context.Background()
	cs := newSuccessionStore(t)
	owner := seedSuccessionActor(t, cs, "owner-human", actor.KindHuman)
	minter := newSuccessionMinter(t, cs, owner)

	child := seedSuccessionActor(t, cs, "doc-author", actor.KindAgent)
	peer := seedSuccessionActor(t, cs, "channel-peer", actor.KindAgent)
	childHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: child})
	peerHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: peer})

	const id = resource.ResourceID("resource:shared-doc")
	out, err := childHandle.Create(ctx, id, CreateSpec{Kind: KindKV}, []byte("v1"))
	mustAccept(t, out, err)

	// Membrane-uniform: a fellow member reads and writes with zero ceremony…
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := peerHandle.Invoke(ctx, op, id, []byte("peer"))
		mustAccept(t, out, err)
	}
	// …but cannot delete what it did not create (PM-D3).
	out, err = peerHandle.Invoke(ctx, access.OpDelete, id, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	endSuccessionActor(t, cs, child, 2)

	// After the creator's death: reach unchanged in BOTH directions.
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := peerHandle.Invoke(ctx, op, id, []byte("peer after author death"))
		mustAccept(t, out, err)
	}
	out, err = peerHandle.Invoke(ctx, access.OpDelete, id, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	// Leaving the channel cuts everything at once. Both gates that implement
	// the cut are asserted — the welded handle refuses to admit at all, and the
	// door's own membership judgment denies an inactive caller (the gate a
	// remote ingress mint would still land on).
	endSuccessionActor(t, cs, peer, 3)
	out, err = peerHandle.Invoke(ctx, access.OpRead, id, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	out, err = newSuccessionDoor(cs, owner).invoke(ctx, peer, access.OpRead, id, nil)
	mustVerdict(t, out, err, access.AccessDenied)
}
