package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// kvSpec (the channel-scoped Create's CreateSpec) is declared once, in
// access_integration_test.go — same package, reused here.

// This file is the runtime-package-root INTEGRATION counterpart to accessdoor's
// in-package state unit tests (state_test.go): those drive the collapsed tree
// with injected fakes; these drive the ACTOR-SCOPED branch of the whole plane-2
// door assembled by OpenChannel over a real per-channel sqlite — MintState welds
// the owner, the real stateStore realizes bytes, and the real actor_registry
// dereg path cascades the clear.
//
// State handles are actor-scoped: the reachable set is structurally ≡ {owner}, so
// owners are NOT seeded as channel members here (membership is never consulted on
// this locus — that absence IS the scope law). Members are seeded only where a
// slice also touches the channel-scoped plane (slices 4 and 5).

// ---- slice 1: privacy by structure -----------------------------------------

// TestStateSlice1_PrivacyByStructure: two owners A and B each hold an actor-scoped
// handle; B reading A's id gets resource_not_found (the id is simply not in B's
// namespace), NOT access_denied. The negative assertion is load-bearing: the
// collapsed branch NEVER produces access_denied — there is no possible world in
// which the owner is denied its own namespace, so an access_denied here would be a
// lie the door structurally cannot tell.
func TestStateSlice1_PrivacyByStructure(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	const (
		A = actor.ActorID("A")
		B = actor.ActorID("B")
	)
	hA := cs.Access.MintState(A)
	hB := cs.Access.MintState(B)

	const id = resource.ResourceID("cursor")
	v := []byte("A's private bytes")

	// A drives the full CRUD over its own key — all pass under the welded owner.
	acc(t, "A create own")(hA.Invoke(ctx, access.OpCreate, id, v, nil))
	out, err := hA.Invoke(ctx, access.OpRead, id, nil, nil)
	expectAccepted(t, "A read own", out, err)
	expectBytes(t, "A read own value", out, v)
	acc(t, "A write own")(hA.Invoke(ctx, access.OpWrite, id, []byte("v2"), nil))
	acc(t, "A delete own")(hA.Invoke(ctx, access.OpDelete, id, nil, nil))
	acc(t, "A re-create own")(hA.Invoke(ctx, access.OpCreate, id, v, nil))

	// B reads A's id: the row is not in B's (owner=B, id) namespace → not_found,
	// and specifically NOT access_denied (no R was consulted).
	out, err = hB.Invoke(ctx, access.OpRead, id, nil, nil)
	expectStateNotFoundNotDenied(t, "B read A's id", out, err)
	// B writing/deleting A's id is equally invisible, never a denial.
	out, err = hB.Invoke(ctx, access.OpWrite, id, v, nil)
	expectStateNotFoundNotDenied(t, "B write A's id", out, err)
	out, err = hB.Invoke(ctx, access.OpDelete, id, nil, nil)
	expectStateNotFoundNotDenied(t, "B delete A's id", out, err)
}

// ---- slice 2: four-step ingress order + op distinctions --------------------

// TestStateSlice2_FourStepOrderAndOpDistinctions pins the four-step actor-scoped
// ingress order and the collapsed op verdicts through the assembled handle. The
// three signal classes stay distinct: ErrOpNotInScope (category error, ≠
// ErrMalformed), ErrMalformed (wire-shape fault), and the RESOLVE-stage verdicts
// (already_exists / resource_not_found).
func TestStateSlice2_FourStepOrderAndOpDistinctions(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	const A = actor.ActorID("A")
	hA := cs.Access.MintState(A)
	const id = resource.ResourceID("k")

	// set never exists on this locus, regardless of grant shape → ErrOpNotInScope
	// (and, load-bearing, NOT ErrMalformed — the two signals are distinct).
	_, err := hA.Invoke(ctx, access.OpSet, id, nil, nil)
	expectOpNotInScope(t, "set without grant", err)
	_, err = hA.Invoke(ctx, access.OpSet, id, nil, actorGrant(A, access.OpRead))
	expectOpNotInScope(t, "set with grant", err)

	// step ①: garbage op (out of the closed set) is a wire-shape fault → ErrMalformed.
	_, err = hA.Invoke(ctx, access.Operation("chmod"), id, nil, nil)
	expectMalformed(t, "garbage op", err)

	// step ②: empty ResourceID → ErrMalformed.
	_, err = hA.Invoke(ctx, access.OpCreate, resource.ResourceID(""), nil, nil)
	expectMalformed(t, "empty id create", err)

	// step ④: a legal op carrying a grant is malformed (no op on this locus carries
	// a Grant operand — set was the only grant-bearing op and it is gone).
	_, err = hA.Invoke(ctx, access.OpCreate, id, []byte("v"), actorGrant(A, access.OpRead))
	expectMalformed(t, "create with grant", err)

	// create-then-recreate: the second create collides atomically → already_exists.
	acc(t, "create")(hA.Invoke(ctx, access.OpCreate, id, []byte("v"), nil))
	out, err := hA.Invoke(ctx, access.OpCreate, id, []byte("v"), nil)
	expectReason(t, "re-create same id", out, err, access.AlreadyExists)

	// write / delete on an ABSENT id → resource_not_found (birth is create).
	const absent = resource.ResourceID("never-born")
	out, err = hA.Invoke(ctx, access.OpWrite, absent, []byte("v"), nil)
	expectReason(t, "write absent", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpDelete, absent, nil, nil)
	expectReason(t, "delete absent", out, err, access.ResourceNotFound)

	// repeated delete is idempotent: delete an existing id, then delete again →
	// honestly not-found (the second call is a safe retry).
	acc(t, "delete existing")(hA.Invoke(ctx, access.OpDelete, id, nil, nil))
	out, err = hA.Invoke(ctx, access.OpDelete, id, nil, nil)
	expectReason(t, "repeat delete idempotent", out, err, access.ResourceNotFound)
}

// ---- slice 2b: empty bytes vs resolved-but-empty ---------------------------

// TestStateSlice2b_EmptyBytes: the NULL-vs-empty distinction survives the round
// trip through the real sqlite BLOB column. create(nil) stores a row
// with a NULL bytes column = resolved-but-empty → read returns an accepted
// Outcome{Found:false, Value:nil}; create([]byte{}) stores an empty non-nil blob
// = a value → read returns Found:true with a non-nil zero-length Value.
//
// The store-level unit test (internal/store/state_test.go) already proved the
// sqlite driver distinguishes NULL from an empty blob; this asserts the door maps
// each to the correct Found bit end-to-end. No deviation from spec observed: the mattn
// sqlite driver stores a nil []byte arg as NULL and an []byte{} arg as an empty
// blob, and `bytes IS NULL` reads them back apart.
func TestStateSlice2b_EmptyBytes(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	const A = actor.ActorID("A")
	hA := cs.Access.MintState(A)

	// create(nil) → existing row, NULL bytes → read: accepted, Found:false, Value:nil.
	const nullID = resource.ResourceID("null-bytes")
	acc(t, "create(nil)")(hA.Invoke(ctx, access.OpCreate, nullID, nil, nil))
	out, err := hA.Invoke(ctx, access.OpRead, nullID, nil, nil)
	if err != nil {
		t.Fatalf("read(null): unexpected Go error: %v", err)
	}
	if !out.Accepted() {
		t.Fatalf("read(null): rejected with %q, want accepted", out.RejectReason)
	}
	if out.Found {
		t.Fatalf("read(null): Found = true, want false (existing row + NULL bytes = resolved-but-empty)")
	}
	if out.Value != nil {
		t.Fatalf("read(null): Value = %#v, want nil", out.Value)
	}

	// create([]byte{}) → existing row, empty non-nil blob → read: Found:true, Value len 0.
	const emptyID = resource.ResourceID("empty-value")
	acc(t, "create([]byte{})")(hA.Invoke(ctx, access.OpCreate, emptyID, []byte{}, nil))
	out, err = hA.Invoke(ctx, access.OpRead, emptyID, nil, nil)
	if err != nil {
		t.Fatalf("read(empty): unexpected Go error: %v", err)
	}
	if !out.Accepted() {
		t.Fatalf("read(empty): rejected with %q, want accepted", out.RejectReason)
	}
	if !out.Found {
		t.Fatalf("read(empty): Found = false, want true (empty non-nil bytes are a value)")
	}
	if out.Value == nil || len(out.Value) != 0 {
		t.Fatalf("read(empty): Value = %#v, want non-nil zero-length", out.Value)
	}
}

// ---- slice 3: WHICH-data = identity ----------------------------------------

// TestStateSlice3_WhichDataIsIdentity: state DATA is keyed by identity (ActorID),
// not by any per-handle/per-incarnation token. Two separate MintState calls for
// the SAME owner return handles that read the SAME bytes — the handle carries no
// incarnation state.
//
// NOTE (to avoid misreading): this asserts ONLY the WHICH-data axis. The
// orthogonal WHEN-valid axis (a handle held by a dead incarnation must be
// refused) is the liveAccess membrane's job, wired in a later integration
// phase — it does NOT exist in the current contract layer. A second MintState
// reading the same bytes here must NOT be read as "the WHEN half is built":
// there simply is no validity dimension on the current handle at all.
func TestStateSlice3_WhichDataIsIdentity(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	const A = actor.ActorID("A")
	const id = resource.ResourceID("checkpoint")
	v := []byte("continuity across incarnations")

	h1 := cs.Access.MintState(A)
	acc(t, "gen-1 create checkpoint")(h1.Invoke(ctx, access.OpCreate, id, v, nil))

	// A fresh MintState for the same owner (a later "incarnation"'s handle) reads
	// back the earlier handle's bytes: DATA is welded to identity.
	h2 := cs.Access.MintState(A)
	out, err := h2.Invoke(ctx, access.OpRead, id, nil, nil)
	expectAccepted(t, "gen-2 read checkpoint", out, err)
	expectBytes(t, "gen-2 checkpoint value", out, v)
}

// ---- slice 4: cascade clear vs non-lossy -----------------------------------

// TestStateSlice4_CascadeClearVsNonLossy: owner deregister cascades the
// actor-scoped locus (state dies with its owner) in the SAME tx that flips
// the registry, while the channel-scoped plane is NON-LOSSY — a resource the actor
// created (its row AND its R grant) survives the dereg. Both dereg entry points —
// the single-actor Deregister and the batch ApplyMemberTransitions(removes) — run
// the same clearActorScopedTx, so both are asserted.
func TestStateSlice4_CascadeClearVsNonLossy(t *testing.T) {
	ctx := context.Background()

	// The owner must be a channel MEMBER to create a channel-scoped resource (the
	// non-lossy witness); the actor-scoped create needs no membership.
	assertClears := func(t *testing.T, dereg func(t *testing.T, cs *ChannelStores, id actor.ActorID)) {
		cs := openAccessChannel(t)
		const A = actor.ActorID("A")
		seedMember(t, cs, A)

		hState := cs.Access.MintState(A)
		hChan := cs.Access.Mint(A)

		const stateID = resource.ResourceID("s")
		const kvID = resource.ResourceID("kv:doc")
		kvBytes := []byte("channel-scoped, survives dereg")

		acc(t, "A create actor-scoped state")(hState.Invoke(ctx, access.OpCreate, stateID, []byte("cursor"), nil))
		acc(t, "A create channel-scoped kv")(hChan.Create(ctx, kvID, kvSpec, kvBytes))

		dereg(t, cs, A)

		// Cascade: the actor_state row is gone (same tx as the registry flip).
		out, err := hState.Invoke(ctx, access.OpRead, stateID, nil, nil)
		expectReason(t, "state read after dereg (cascaded)", out, err, access.ResourceNotFound)

		// The channel-scoped resource survives, while the removed actor's direct
		// grant is cascaded on the grantee axis.
		out, err = hChan.Invoke(ctx, access.OpRead, kvID, nil, nil)
		expectReason(t, "channel-scoped kv read after dereg", out, err, access.AccessDenied)
	}

	t.Run("Deregister path", func(t *testing.T) {
		assertClears(t, func(t *testing.T, cs *ChannelStores, id actor.ActorID) {
			if err := cs.Membership.Deregister(ctx, id, 100); err != nil {
				t.Fatalf("Deregister: %v", err)
			}
		})
	})

	t.Run("ApplyMemberTransitions removes path", func(t *testing.T) {
		assertClears(t, func(t *testing.T, cs *ChannelStores, id actor.ActorID) {
			if err := cs.Membership.ApplyMemberTransitions(ctx, nil,
				[]storespec.MemberActorRemove{{ID: id, At: 100}}); err != nil {
				t.Fatalf("ApplyMemberTransitions: %v", err)
			}
		})
	})
}

// ---- slice 5: two loci mutually invisible ----------------------------------

// TestStateSlice5_TwoLociMutuallyInvisible: the actor-scoped locus (actor_state)
// and the channel-scoped locus (resources) are structurally separate homes — the
// scope law lives in WHICH table, not a column. A channel-scoped handle reading an
// id that only exists in state gets resource_not_found, and vice versa; the same id
// string may live independently in both loci as two distinct rows.
func TestStateSlice5_TwoLociMutuallyInvisible(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	const A = actor.ActorID("A")
	seedMember(t, cs, A) // member: needed to create a channel-scoped resource

	hChan := cs.Access.Mint(A)
	hState := cs.Access.MintState(A)

	const stateOnly = resource.ResourceID("state-only")
	const chanOnly = resource.ResourceID("chan-only")

	acc(t, "create state-only in actor_state")(hState.Invoke(ctx, access.OpCreate, stateOnly, []byte("s"), nil))
	acc(t, "create chan-only in resources")(hChan.Create(ctx, chanOnly, kvSpec, []byte("c")))

	// channel-scoped handle cannot see the state-only id (not in resources).
	out, err := hChan.Invoke(ctx, access.OpRead, stateOnly, nil, nil)
	expectReason(t, "channel handle reads state-only id", out, err, access.ResourceNotFound)

	// actor-scoped handle cannot see the chan-only id (not in actor_state) — and it
	// is not_found, never access_denied (the collapsed branch consults no R).
	out, err = hState.Invoke(ctx, access.OpRead, chanOnly, nil, nil)
	expectStateNotFoundNotDenied(t, "state handle reads chan-only id", out, err)

	// The same id string lives independently in each locus: two distinct rows,
	// each handle reads its own locus's value.
	const dup = resource.ResourceID("dup")
	acc(t, "state create dup")(hState.Invoke(ctx, access.OpCreate, dup, []byte("state-value"), nil))
	acc(t, "channel create dup")(hChan.Create(ctx, dup, kvSpec, []byte("chan-value")))
	out, err = hState.Invoke(ctx, access.OpRead, dup, nil, nil)
	expectAccepted(t, "state read dup", out, err)
	expectBytes(t, "state dup value", out, []byte("state-value"))
	out, err = hChan.Invoke(ctx, access.OpRead, dup, nil, nil)
	expectAccepted(t, "channel read dup", out, err)
	expectBytes(t, "channel dup value", out, []byte("chan-value"))
}

// (slice 6 — driver_error evidence — lives in accessdoor/state_test.go
// (TestInvokeActorScopedDriverError), where the fake family already exists: verdict
// mapping is a door branch behaviour, a unit concern, not an integration path.
// Keeping a fake-injected copy here would grow a third parallel fake family.)

// ---- state-slice assertion helpers -----------------------------------------

// acc curries expectAccepted so an Invoke's (Outcome, error) return spreads
// straight into the assertion, keeping the slice bodies one line per accepted op:
// acc(t, "label")(h.Invoke(...)).
func acc(t *testing.T, label string) func(accessdoor.Outcome, error) {
	t.Helper()
	return func(out accessdoor.Outcome, err error) {
		t.Helper()
		expectAccepted(t, label, out, err)
	}
}

// expectStateNotFoundNotDenied is the load-bearing negative assertion for the
// collapsed branch: a foreign/absent id resolves to resource_not_found and,
// crucially, NEVER access_denied (no R is consulted, so no authorization can fail).
func expectStateNotFoundNotDenied(t *testing.T, label string, out accessdoor.Outcome, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected Go error: %v", label, err)
	}
	if out.RejectReason == access.AccessDenied {
		t.Fatalf("%s: reason = access_denied — the collapsed branch must never deny (no R at this locus)", label)
	}
	if out.RejectReason != access.ResourceNotFound {
		t.Fatalf("%s: reason = %q, want resource_not_found", label, out.RejectReason)
	}
}

// expectOpNotInScope asserts the category-error signal AND its distinctness from
// ErrMalformed — the two protocol-error classes must not collapse into one word.
func expectOpNotInScope(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, accessdoor.ErrOpNotInScope) {
		t.Fatalf("%s: err = %v, want ErrOpNotInScope", label, err)
	}
	if errors.Is(err, accessdoor.ErrMalformed) {
		t.Fatalf("%s: err = ErrMalformed, want ErrOpNotInScope (category error ≠ wire-shape fault)", label)
	}
}
