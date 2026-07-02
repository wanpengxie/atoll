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
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// TestAccessDoorVerticalSlice drives the whole plane-2 door assembled by
// OpenChannel over a real sqlite file — every §4 branch (two loci, the A8
// union, day-1 Ops narrowing, non-lossy delete, fresh re-birth, and dynamic
// membership) against the actual resourceRegistry + kvDriver + membership
// adapter, not fakes. It is the integration counterpart to accessdoor's
// in-package unit tests: those exercise the tree with injected fakes; this
// proves the runtime wiring hands the door its real collaborators.
func TestAccessDoorVerticalSlice(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	// A, B, C are channel members; X is not (never registered); E joins late.
	const (
		A = actor.ActorID("A")
		B = actor.ActorID("B")
		C = actor.ActorID("C")
		E = actor.ActorID("E")
		X = actor.ActorID("X")
	)
	seedMember(t, cs, A)
	seedMember(t, cs, B)
	seedMember(t, cs, C)

	hA := cs.Access.Mint(A)
	hB := cs.Access.Mint(B)
	hC := cs.Access.Mint(C)
	hX := cs.Access.Mint(X)

	const rid = resource.ResourceID("kv:doc")
	const ridX = resource.ResourceID("kv:docX")
	v1 := []byte("hello")
	v2 := []byte("world")

	// ---- Step 1: create locus (channel membership) ----
	out, err := hA.Invoke(ctx, access.OpCreate, rid, v1, nil)
	expectAccepted(t, "A create", out, err)

	out, err = hX.Invoke(ctx, access.OpCreate, ridX, nil, nil)
	expectReason(t, "non-member X create", out, err, access.AccessDenied)

	out, err = hA.Invoke(ctx, access.OpCreate, rid, v1, nil)
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
	out, err = hA.Invoke(ctx, access.OpCreate, rid, v1, nil)
	expectAccepted(t, "A re-create after delete (fresh birth)", out, err)

	// ---- Step 7: dynamic membership (exit loses, join gains) ----
	out, err = hA.Invoke(ctx, access.OpSet, rid, nil, membersGrant(access.OpRead))
	expectAccepted(t, "A set(members,{read}) on fresh resource", out, err)

	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "C read before deregister", out, err)

	if err := cs.Membership.Deregister(ctx, C, 100); err != nil {
		t.Fatalf("deregister C: %v", err)
	}
	out, err = hC.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectReason(t, "C read after deregister (exit loses access)", out, err, access.AccessDenied)

	seedMember(t, cs, E)
	hE := cs.Access.Mint(E)
	out, err = hE.Invoke(ctx, access.OpRead, rid, nil, nil)
	expectAccepted(t, "E read after joining (late join gains access)", out, err)
	expectBytes(t, "E read value", out, v1)
}

// ---- helpers ----

func openAccessChannel(t *testing.T) *ChannelStores {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenChannel(context.Background(), channel.ID("c-access"),
		filepath.Join(dir, "channel.sqlite"), OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func seedMember(t *testing.T, cs *ChannelStores, id actor.ActorID) {
	t.Helper()
	if err := cs.Membership.Insert(context.Background(),
		storespec.Record{ID: id, Kind: actor.KindAgent, CreatedAt: 1}); err != nil {
		t.Fatalf("seed member %s: %v", id, err)
	}
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
