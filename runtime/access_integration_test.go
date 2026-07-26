package runtime

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// kvSpec is the day-1 CreateSpec every integration test in this file creates
// through — inline kv value, no placement axis.
var kvSpec = resourcespec.CreateSpec{Kind: resourcespec.KindKV}

// TestAccessDoorVerticalSlice drives the whole plane-2 door assembled by
// OpenChannel over a real sqlite file — every branch (two loci, the
// grantee-kind union, day-1 Ops narrowing, non-lossy delete, fresh re-birth,
// and dynamic membership) against the actual resourceRegistry + kvDriver +
// membership adapter, not fakes. It is the integration counterpart to
// accessdoor's in-package unit tests: those exercise the tree with injected
// fakes; this proves the runtime wiring hands the door its real
// collaborators.
func TestAccessDoorVerticalSlice(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	// A, B, C are channel members; X is not (never registered); E joins late.
	var (
		A = actor.ActorID("A")
		B = actor.ActorID("B")
		C = actor.ActorID("C")
		E = actor.ActorID("E")
	)
	A = seedMember(t, cs, A)
	B = seedMember(t, cs, B)
	C = seedMember(t, cs, C)

	hA := cs.Access.MintAdmitted(identityAdmission(A))
	hB := cs.Access.MintAdmitted(identityAdmission(B))
	hC := cs.Access.MintAdmitted(identityAdmission(C))
	// Platform cannot obtain an admission for a non-member; model that failed
	// source-boundary admission with the zero value.
	hX := cs.Access.MintAdmitted(storespec.IdentityAdmission{})

	const rid = resource.ResourceID("kv:doc")
	const ridX = resource.ResourceID("kv:docX")
	v1 := []byte("hello")
	v2 := []byte("world")

	// ---- Step 1: create locus (channel membership) ----
	out, err := hA.Create(ctx, rid, kvSpec, v1)
	expectAccepted(t, "A create", out, err)

	out, err = hX.Create(ctx, ridX, kvSpec, nil)
	if !errors.Is(err, accessdoor.ErrAuthorInactive) {
		t.Fatalf("non-member X create = (%+v,%v)", out, err)
	}

	out, err = hA.Create(ctx, rid, kvSpec, v1)
	expectReason(t, "A re-create same id", out, err, access.AlreadyExists)

	// ---- Step 2: object op via R (creator has full rights; B has none) ----
	out, err = hA.Invoke(ctx, access.OpWrite, rid, v2, nil)
	expectAccepted(t, "A write", out, err)

	out, err = hB.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "B read (no grant)", out, err, access.AccessDenied)

	// ---- Step 3: actor grant {read} to B ----
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, actorGrant(B, access.OpRead))
	expectAccepted(t, "A set(actor:B,{read})", out, err)

	out, err = hB.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "B read (granted)", out, err)
	expectBytes(t, "B read value", out, v2)

	out, err = hB.Invoke(ctx, access.OpWrite, rid, v1, nil)
	expectReason(t, "B write (read-only grant)", out, err, access.AccessDenied)

	// ---- Step 4: members grant + union visibility + revoke ----
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, membersGrant(access.OpRead))
	expectAccepted(t, "A set(members,{read})", out, err)

	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "C read (members late-binding)", out, err)
	expectBytes(t, "C read value", out, v2)

	// Revoke B's DIRECT entry — B stays readable via the members entry (union).
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, actorGrant(B))
	expectAccepted(t, "A set(actor:B,∅) revoke", out, err)

	out, err = hB.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "B read (still visible via members union)", out, err)
	expectBytes(t, "B read value via members", out, v2)

	// Revoke the members entry — now neither B nor C can read.
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, membersGrant())
	expectAccepted(t, "A set(members,∅) revoke", out, err)

	out, err = hB.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "B read (all grants revoked)", out, err, access.AccessDenied)
	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "C read (all grants revoked)", out, err, access.AccessDenied)

	// ---- Step 5: day-1 Ops narrowing + ingress malformed ----
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, actorGrant(B, access.OpRead, access.OpDelete))
	expectReason(t, "A set(actor:B,{read,delete}) day-1 narrowing", out, err, access.AccessDenied)

	_, err = hA.Invoke(ctx, access.OpSet, rid, nil, nil)
	expectMalformed(t, "A set with nil grant", err)

	// ---- Step 6: non-lossy delete + idempotency + fresh re-birth ----
	out, err = hA.Invoke(ctx, access.OpDelete, rid, nil, nil)
	expectAccepted(t, "A delete", out, err)

	out, err = hA.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "read after delete", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpWrite, rid, v1, nil)
	expectReason(t, "write after delete", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, actorGrant(B, access.OpRead))
	expectReason(t, "set after delete", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpDelete, rid, nil, nil)
	expectReason(t, "repeat delete (idempotent)", out, err, access.ResourceNotFound)

	// Fresh birth: create has no memory of the deleted id.
	out, err = hA.Create(ctx, rid, kvSpec, v1)
	expectAccepted(t, "A re-create after delete (fresh birth)", out, err)

	// ---- Step 7: dynamic membership (exit loses, join gains) ----
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, membersGrant(access.OpRead))
	expectAccepted(t, "A set(members,{read}) on fresh resource", out, err)

	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "C read before deregister", out, err)

	if err := endDeclaredTest(ctx, cs.ChannelStores, C, 100); err != nil {
		t.Fatalf("deregister C: %v", err)
	}
	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "C read after deregister (exit loses grant)", out, err, access.AccessDenied)

	E = seedMember(t, cs, E)
	hE := cs.Access.MintAdmitted(identityAdmission(E))
	out, err = hE.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "E read after joining (late join gains access)", out, err)
	expectBytes(t, "E read value", out, v1)
}

func TestChannelOwnerRecoversStrandedDaemonResource(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)
	record, err := cs.Actors.Insert(ctx, storespec.ActorDraft{
		Kind: actor.KindHuman, Principal: "channel-owner",
		Definition: storespec.ActorDefinition{Class: "human"},
		Placement:  storespec.NewServerPlacement(), CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Owner-ness is a door judgement over the genesis pointer, so the fixture
	// names the owner explicitly rather than reading a bit off the record.
	cs.markOwner(record.ID)
	owner := cs.Access.MintAdmitted(identityAdmission(record.ID))
	const rid resource.ResourceID = "file:stranded"
	if err := csResourcesCreateForTest(cs, rid, "retired-daemon", "orphan-coord"); err != nil {
		t.Fatal(err)
	}
	// Model a stranded row: the daemon has retired and its ordinary members
	// grant has been revoked, leaving no grant entry that can recover it.
	registry := rawResourceRegistryForTest(cs)
	if err := registry.SetGrant(ctx, rid, access.Grant{GranteeKind: access.GranteeMembers}); err != nil {
		t.Fatal(err)
	}
	page, err := owner.List(ctx, accessdoor.ListQuery{})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].ID != rid {
		t.Fatalf("owner List stranded = (%+v,%v)", page, err)
	}
	grant := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "peer", Ops: []access.Operation{access.OpRead, access.OpWrite}}
	out, err := owner.Invoke(ctx, access.OpSet, rid, nil, grant)
	expectAccepted(t, "owner grants read/write", out, err)
	grant.Ops = []access.Operation{access.OpRead, access.OpDelete}
	out, err = owner.Invoke(ctx, access.OpSet, rid, nil, grant)
	expectReason(t, "owner remains under day-1 grant ceiling", out, err, access.AccessDenied)
	// Restore the zero-grant stranded shape before manual recovery.
	if err := registry.SetGrant(ctx, rid, access.Grant{GranteeKind: access.GranteeActor, Grantee: "peer"}); err != nil {
		t.Fatal(err)
	}
	out, err = owner.Invoke(ctx, access.OpDelete, rid, nil, nil)
	expectAccepted(t, "owner deletes stranded resource", out, err)
	page, err = owner.List(ctx, accessdoor.ListQuery{})
	if err != nil || len(page.Entries) != 0 {
		t.Fatalf("owner List after recovery = (%+v,%v)", page, err)
	}
	tombstones, err := cs.Outbox.ListTombstonesByDaemon(ctx, "retired-daemon")
	if err != nil || len(tombstones) != 1 {
		t.Fatalf("tombstones = (%+v,%v)", tombstones, err)
	}
	// The inert tombstone is an outbox obligation, not a namespace lock.
	if err := csResourcesCreateForTest(cs, rid, "retired-daemon", "replacement-coord"); err != nil {
		t.Fatalf("inert tombstone blocked fresh birth: %v", err)
	}
}

func csResourcesCreateForTest(cs *testAccessChannel, id resource.ResourceID, daemonID, coord string) error {
	return rawResourceRegistryForTest(cs).Create(context.Background(), id, resourcespec.KindFile, actor.SystemActorID, daemonID, coord, nil,
		resourcespec.ResourceBirthPlan{})
}

func rawResourceRegistryForTest(cs *testAccessChannel) resourcespec.Registry {
	return cs.Assembly.Resources
}

// ---- helpers ----

type testAccessChannel struct {
	*ChannelStores
	Access accessdoor.AccessMinter
	Outbox resourcespec.ResourceOutbox
	owner  *actor.ActorID
}

func (c *testAccessChannel) markOwner(id actor.ActorID) { *c.owner = id }

type testResourceOutbox struct {
	resourcespec.ResourceOutbox
	completion accessdoor.ResourceCompletion
}

func (o testResourceOutbox) CommitReservation(
	ctx context.Context,
	id string,
) (resourcespec.LandedResource, bool, error) {
	return o.completion.CommitReservation(ctx, id)
}

func openAccessChannel(t *testing.T) *testAccessChannel {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenChannel(context.Background(), channel.ID("c-access"),
		filepath.Join(dir, "channel.sqlite"), OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	owner := new(actor.ActorID)
	authority := testAccessAuthority{declared: cs.Actors, owner: owner}
	access, completion, err := accessdoor.NewAssembly(accessdoor.Deps{
		Registry:  cs.Assembly.Resources,
		Drivers:   accessdoor.DriverTable{resourcespec.KindKV: cs.Assembly.KV},
		Authority: authority,
		State:     cs.Assembly.State,
		ChannelID: "c-access",
	})
	if err != nil {
		t.Fatalf("assemble access: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return &testAccessChannel{
		ChannelStores: cs,
		Access:        access,
		Outbox: testResourceOutbox{
			ResourceOutbox: cs.Assembly.Resources,
			completion:     completion,
		},
		owner: owner,
	}
}

type testAccessAuthority struct {
	declared storespec.ActorRegistryStore
	owner    *actor.ActorID
}

func (a testAccessAuthority) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorRecord, bool, error) {
	rec, ok, err := a.declared.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ActorRecord{}, false, err
	}
	return rec, true, nil
}

func (a testAccessAuthority) ListActive(context.Context) ([]storespec.ActorRecord, error) {
	return nil, nil
}

func (a testAccessAuthority) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	_, ok, err := a.LookupActive(ctx, id)
	return ok, err
}

func (a testAccessAuthority) AdmitIdentity(
	ctx context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	row, ok, err := a.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.IdentityAdmission{}, false, err
	}
	return storespec.IdentityAdmission{ID: row.ID, Kind: row.Kind}, true, nil
}

func (a testAccessAuthority) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	row, ok, err := a.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ResourceActorFacts{}, err
	}
	host := ""
	if row.Placement.Kind == storespec.PlacementDaemon {
		host = row.Placement.Host
	}
	return storespec.ResourceActorFacts{
		Active:               true,
		Owner:                a.owner != nil && *a.owner == row.ID,
		PreferredStorageHost: host,
	}, nil
}

func seedMember(t *testing.T, cs *testAccessChannel, id actor.ActorID) actor.ActorID {
	t.Helper()
	minted, err := admitDeclaredTest(context.Background(), cs.ChannelStores, actor.KindAgent, string(id), 1)
	if err != nil {
		t.Fatalf("seed member %s: %v", id, err)
	}
	return minted
}

func actorGrant(id actor.ActorID, ops ...access.Operation) *access.Grant {
	return &access.Grant{GranteeKind: access.GranteeActor, Grantee: id, Ops: ops}
}

func membersGrant(ops ...access.Operation) *access.Grant {
	return &access.Grant{GranteeKind: access.GranteeMembers, Ops: ops}
}

func expectAccepted(t *testing.T, label string, out accessdoor.Outcome, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected Go error: %v", label, err)
	}
	if !out.Accepted() {
		t.Fatalf("%s: rejected with %q, want accepted", label, out.RejectReason)
	}
}

func expectReason(t *testing.T, label string, out accessdoor.Outcome, err error, want access.FailureReason) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected Go error: %v", label, err)
	}
	if out.RejectReason != want {
		t.Fatalf("%s: reason = %q, want %q", label, out.RejectReason, want)
	}
}

func expectBytes(t *testing.T, label string, out accessdoor.Outcome, want []byte) {
	t.Helper()
	if !out.Found {
		t.Fatalf("%s: found = false, want the bytes present", label)
	}
	if !bytes.Equal(out.Value, want) {
		t.Fatalf("%s: value = %q, want %q", label, out.Value, want)
	}
}

func expectMalformed(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, accessdoor.ErrMalformed) {
		t.Fatalf("%s: err = %v, want ErrMalformed", label, err)
	}
}
