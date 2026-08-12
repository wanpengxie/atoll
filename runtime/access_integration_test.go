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
// OpenChannel over a real sqlite file — every branch (membrane-uniform
// read/write, PM-D3 creator-delete, non-lossy delete, fresh re-birth,
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

	hA := cs.Access.MintAuthority(identityAuthority{id: A})
	hB := cs.Access.MintAuthority(identityAuthority{id: B})
	hC := cs.Access.MintAuthority(identityAuthority{id: C})
	// Platform cannot obtain an admission for a non-member; model that failed
	// source-boundary admission with the zero value.
	hX := cs.Access.MintAuthority(nil)

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

	// ---- Step 2: membrane-uniform read/write (PM-D1) — membership IS the
	// right; no grant ceremony exists anywhere in this slice ----
	out, err = hA.Invoke(ctx, access.OpWrite, rid, v2)
	expectAccepted(t, "A write", out, err)

	out, err = hB.Invoke(ctx, access.OpRead, rid, nil)
	expectAccepted(t, "B read (membership is the right)", out, err)
	expectBytes(t, "B read value", out, v2)

	out, err = hC.Invoke(ctx, access.OpRead, rid, nil)
	expectAccepted(t, "C read (uniform)", out, err)

	out, err = hB.Invoke(ctx, access.OpWrite, rid, v1)
	expectAccepted(t, "B write (uniform)", out, err)

	// ---- Step 3: delete is the one creator-distinguished op (PM-D3) ----
	out, err = hB.Invoke(ctx, access.OpDelete, rid, nil)
	expectReason(t, "B delete (not creator, not owner)", out, err, access.AccessDenied)

	out, err = hA.Invoke(ctx, access.OpDelete, rid, nil)
	expectAccepted(t, "A delete (creator)", out, err)

	// ---- Step 4: non-lossy delete + idempotency + fresh re-birth ----
	out, err = hA.Invoke(ctx, access.OpRead, rid, nil)
	expectReason(t, "read after delete", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpWrite, rid, v1)
	expectReason(t, "write after delete", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpDelete, rid, nil)
	expectReason(t, "repeat delete (idempotent)", out, err, access.ResourceNotFound)

	// Fresh birth: create has no memory of the deleted id.
	out, err = hA.Create(ctx, rid, kvSpec, v1)
	expectAccepted(t, "A re-create after delete (fresh birth)", out, err)

	// ---- Step 5: dynamic membership (exit loses everything, join gains) ----
	out, err = hC.Invoke(ctx, access.OpRead, rid, nil)
	expectAccepted(t, "C read before deregister", out, err)

	if err := endDeclaredTest(ctx, cs.ChannelStores, C, 100); err != nil {
		t.Fatalf("deregister C: %v", err)
	}
	out, err = hC.Invoke(ctx, access.OpRead, rid, nil)
	expectReason(t, "C read after deregister (exit loses membership rights)", out, err, access.AccessDenied)

	E = seedMember(t, cs, E)
	hE := cs.Access.MintAuthority(identityAuthority{id: E})
	out, err = hE.Invoke(ctx, access.OpRead, rid, nil)
	expectAccepted(t, "E read after joining (late join gains access)", out, err)
	expectBytes(t, "E read value", out, v1)
}

// ---- helpers ----

type testAccessChannel struct {
	*ChannelStores
	Access accessdoor.AccessMinter
	owner  *actor.ActorID
}

func (c *testAccessChannel) markOwner(id actor.ActorID) { *c.owner = id }

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
	access, err := accessdoor.NewAssembly(accessdoor.Deps{
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
		owner:         owner,
	}
}

type testAccessAuthority struct {
	declared storespec.ActorRegistryStore
	owner    *actor.ActorID
}

// record is the fixture's own durable read; the Controller's public face is
// narrow question-shaped projections, and each of them is derived below.
func (a testAccessAuthority) record(ctx context.Context, id actor.ActorID) (storespec.ActorRecord, bool, error) {
	return a.declared.LookupActive(ctx, id)
}

func (a testAccessAuthority) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	_, ok, err := a.record(ctx, id)
	return ok, err
}

func (a testAccessAuthority) AdmitIdentity(
	ctx context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	row, ok, err := a.record(ctx, id)
	if err != nil || !ok {
		return storespec.IdentityAdmission{}, false, err
	}
	return storespec.IdentityAdmission{ID: row.ID, Kind: row.Kind}, true, nil
}

func (a testAccessAuthority) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	row, ok, err := a.record(ctx, id)
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
