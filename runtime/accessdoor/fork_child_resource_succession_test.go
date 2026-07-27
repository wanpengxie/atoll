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
// The law (control-model-harden §2/§3, "resource ownership/control 闭包") is:
//
//   - existence is CHANNEL-scoped, so the resource does not die with its
//     creator; created_by is audit provenance, never an authorization predicate;
//   - control after the creator is gone falls back to the CHANNEL OWNER ROOT
//     axiom — the owner is unconditionally authorized on every channel-scoped
//     resource without holding any grant row at all;
//   - the collective's own reach is decided AT BIRTH (a ChannelOwned birth
//     writes a durable members grant), never at death: §3's death path is
//     "只删不补" — deregistration must not mint any compensating grant. The
//     v1.x design that auto-granted members {read,write} when a creator ended
//     was withdrawn precisely because it would leak deliberately-private
//     resources (credentials) to the whole channel the moment their creator
//     died;
//   - the dead creator's own capability is closed at the welded authority, not
//     by rewriting the grants ledger.
//
// (The backlog entry that asked for this coverage described the invariant as
// "ownership transfers to the members collective on creator death"; that is the
// withdrawn v1.x rule. What is pinned below is the rule that replaced it.)
//
// SCOPE. What this file can pin is the DOOR's verdict given a creator that is
// no longer active — the resolve/authorize/project chain that lives in this
// package. It cannot pin the death path's own "只删不补" discipline: these tests
// reach death through the store's Deregister, one `UPDATE actor_registry` with
// no reach into resource_grants, so any "the ledger was not supplemented"
// assertion around it holds by construction rather than by the rule. That
// discipline belongs to the End COMMAND and is pinned over a real Home in
// platform/home/dead_identity_write_fence_test.go
// (TestEndingAnActorMintsNoGrantOnTheResourcesItCreated), which drives the
// production Controller.End and compares the resource's complete grant
// projection across it.

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
// root axiom, holding no grant row of its own.
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

	// created_by is audit provenance. It is recorded, and it is NOT what the
	// door consults.
	stat, err := childHandle.Stat(ctx, id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Meta.CreatedBy != child {
		t.Fatalf("created_by = %q, want the forking child %q", stat.Meta.CreatedBy, child)
	}

	// Owner root, while the creator is still alive: full reach with zero grants.
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := ownerHandle.Invoke(ctx, op, id, []byte("owner"), nil)
		mustAccept(t, out, err)
	}
	if allowed, err := cs.Resources.ActorAllows(ctx, owner, id, access.OpRead); err != nil || allowed {
		t.Fatalf("owner reached the resource through a grant row (allowed=%v, err=%v) — the root axiom must need none", allowed, err)
	}

	// The fork child ends. Its work product must not end with it.
	endSuccessionActor(t, cs, child, 2)

	if _, exists, err := cs.Resources.Resolve(ctx, id); err != nil || !exists {
		t.Fatalf("resource vanished with its creator (exists=%v, err=%v) — existence is channel-scoped", exists, err)
	}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := ownerHandle.Invoke(ctx, op, id, []byte("after death"), nil)
		mustAccept(t, out, err)
	}
	ownerStat, err := ownerHandle.Stat(ctx, id)
	if err != nil {
		t.Fatalf("owner Stat after creator death: %v", err)
	}
	if len(ownerStat.Ops) != len(objectOps) {
		t.Fatalf("owner effective ops = %v, want the full object set — the projection must not disagree with the root axiom", ownerStat.Ops)
	}

	// The dead creator's own capability is closed AT THE HANDLE. This is where
	// the liveness cut lives: the door never has to consult the grants ledger
	// for liveness, and the creator's birth-time grant row is left as inert
	// residue (same family as a deregistered author's timers and state rows) —
	// unreachable because nothing can ever again present that welded authority.
	out, err = childHandle.Invoke(ctx, access.OpRead, id, nil, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	out, err = childHandle.Create(ctx, "resource:posthumous", CreateSpec{Kind: KindKV}, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	if _, exists, err := cs.Resources.Resolve(ctx, "resource:posthumous"); err != nil || exists {
		t.Fatalf("a dead creator landed a new resource (exists=%v, err=%v)", exists, err)
	}

	// Owner root is still the DELETE authority, which is what makes "root-only
	// orphan" a tolerable end state rather than an unremovable one.
	out, err = ownerHandle.Invoke(ctx, access.OpDelete, id, nil, nil)
	mustAccept(t, out, err)
	if _, exists, err := cs.Resources.Resolve(ctx, id); err != nil || exists {
		t.Fatalf("owner delete did not land (exists=%v, err=%v)", exists, err)
	}
}

// TestPrivateResourceStaysPrivateOnceItsCreatorIsInactive is the DOOR's half of
// the withdrawn-v1.x-rule story: given a resource born under the CreatorIdentity
// form (no members row) whose creator is no longer an active registry row, the
// door still denies an ordinary active member — and denies it invisibly. What
// this pins is the door's own verdict over that state, which is the part that
// lives in this package.
//
// It deliberately does NOT claim to pin "the death path mints no compensating
// members grant". Death is reached here by calling the store's Deregister, and
// Deregister is one `UPDATE actor_registry` with no reach into resource_grants
// — so a "no members grant appeared" assertion around it would hold by
// construction of the function being called, and would stay green in a world
// where the End command had the withdrawn auto-grant put back. That rule lives
// at the command layer and is pinned where the command actually runs:
// platform/home's TestEndingAnActorMintsNoGrantOnTheResourcesItCreated, which
// drives the real Controller.End. (This package cannot drive it — actorctl
// depends on accessdoor via lib/actorcaps, so importing it here would close a
// cycle.)
func TestPrivateResourceStaysPrivateOnceItsCreatorIsInactive(t *testing.T) {
	ctx := context.Background()
	cs := newSuccessionStore(t)
	owner := seedSuccessionActor(t, cs, "owner-human", actor.KindHuman)
	minter := newSuccessionMinter(t, cs, owner)

	child := seedSuccessionActor(t, cs, "credential-holder", actor.KindAgent)
	bystander := seedSuccessionActor(t, cs, "other-member", actor.KindAgent)
	childHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: child})
	bystanderHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: bystander})

	const id = resource.ResourceID("resource:private-credential")
	out, err := childHandle.Create(ctx, id, CreateSpec{Kind: KindKV}, []byte("secret"))
	mustAccept(t, out, err)

	out, err = bystanderHandle.Invoke(ctx, access.OpRead, id, nil, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	endSuccessionActor(t, cs, child, 2)

	out, err = bystanderHandle.Invoke(ctx, access.OpRead, id, nil, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	// Invisible, not merely unusable: an active member with no rights must not
	// even learn the resource exists.
	stat, err := bystanderHandle.Stat(ctx, id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Reject != QueryNotFound {
		t.Fatalf("zero-rights Stat after creator death = %+v, want the not-found disguise", stat)
	}
}

// TestChannelOwnedBirthGrantCarriesTheCollectiveAcrossCreatorDeath is the other
// half of the same law: when a resource IS meant for the channel, the members
// grant written at birth is what keeps it reachable — and creator death neither
// widens nor narrows it. Membership itself remains the live gate, so a member
// who leaves loses the collective's rights immediately.
func TestChannelOwnedBirthGrantCarriesTheCollectiveAcrossCreatorDeath(t *testing.T) {
	ctx := context.Background()
	cs := newSuccessionStore(t)
	owner := seedSuccessionActor(t, cs, "owner-human", actor.KindHuman)
	minter := newSuccessionMinter(t, cs, owner)

	child := seedSuccessionActor(t, cs, "shared-doc-author", actor.KindAgent)
	peer := seedSuccessionActor(t, cs, "channel-peer", actor.KindAgent)
	peerHandle := minter.MintAuthority(successionAuthority{registry: cs.Actors, id: peer})

	const id = resource.ResourceID("resource:channel-owned-doc")
	seedResource(t, cs, id, child)
	seedMembersGrant(t, cs, id, access.OpRead, access.OpWrite) // the ChannelOwned birth form

	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := peerHandle.Invoke(ctx, op, id, []byte("peer"), nil)
		mustAccept(t, out, err)
	}

	endSuccessionActor(t, cs, child, 2)

	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		out, err := peerHandle.Invoke(ctx, op, id, []byte("peer after author death"), nil)
		mustAccept(t, out, err)
	}
	// Not widened: the birth grant said {read,write}, and dying did not add
	// delete to it.
	out, err := peerHandle.Invoke(ctx, access.OpDelete, id, nil, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	// Membership is the live half of the members grant: leaving the channel
	// cuts the rights at once, with no grant-row change at all. Both gates that
	// implement that cut are asserted — the welded handle refuses to admit at
	// all, and the door's own union step stops counting the members row for an
	// inactive caller (the gate a remote ingress mint would still land on).
	endSuccessionActor(t, cs, peer, 3)
	out, err = peerHandle.Invoke(ctx, access.OpRead, id, nil, nil)
	mustVerdict(t, out, err, access.OwnerInactive)
	out, err = newSuccessionDoor(cs, owner).invoke(ctx, peer, access.OpRead, id, nil, nil)
	mustVerdict(t, out, err, access.AccessDenied)

	// The cut above came from the membership check alone: nothing the door did
	// on the way to those two denials rewrote the durable row. (That the DEATH
	// PATH leaves the row alone is a different claim, about a different layer —
	// deregistration here is a store call that structurally cannot reach
	// resource_grants, so it could not disprove it. The command layer's version
	// is pinned in platform/home.)
	allowed, err := cs.Resources.MembersAllow(ctx, id, access.OpRead)
	if err != nil {
		t.Fatalf("MembersAllow: %v", err)
	}
	if !allowed {
		t.Fatal("a door read path rewrote the members grant — membership is the live gate, the row is durable truth")
	}
}
