package runtime

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// kvSpec (the channel-scoped Create's CreateSpec) is declared once, in
// access_integration_test.go — same package, reused here.

// This file is the runtime-package-root INTEGRATION counterpart to accessdoor's
// in-package state unit tests (state_test.go): those drive the collapsed tree
// with injected fakes; these drive the ACTOR-SCOPED branch of the whole plane-2
// door assembled by OpenChannel over a real per-channel sqlite — MintState welds
// the owner, the real stateStore realizes bytes, and the real actor_registry
// dereg path touches the registry row ALONE (a dead owner's state rows stay put
// as inert, unreachable data — §5.5).
//
// State handles are actor-scoped: the reachable set is structurally ≡ {owner}.
// Birth nevertheless requires that owner to be active, so every test that creates
// state seeds its owner first. After birth, actor-scoped read/write/delete still do
// not consult membership per operation; that absence remains the scope law.

// ---- privacy by structure ---------------------------------------------------

// TestStatePrivacyByStructure: two owners A and B each hold an actor-scoped
// handle; B reading A's id gets resource_not_found (the id is simply not in B's
// namespace), NOT access_denied. The negative assertion is load-bearing: the
// collapsed branch NEVER produces access_denied — there is no possible world in
// which the owner is denied its own namespace, so an access_denied here would be a
// lie the door structurally cannot tell.
func TestStatePrivacyByStructure(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	A := seedMember(t, cs, actor.ActorID("A"))
	B := seedMember(t, cs, actor.ActorID("B"))
	hA := cs.Access.MintStateAuthority(identityAuthority{id: A})
	hB := cs.Access.MintStateAuthority(identityAuthority{id: B})

	const id = resource.ResourceID("cursor")
	v := []byte("A's private bytes")

	// A drives the full CRUD over its own key — all pass under the welded owner.
	acc(t, "A create own")(hA.Invoke(ctx, access.OpCreate, id, v))
	out, err := hA.Invoke(ctx, access.OpRead, id, nil)
	expectAccepted(t, "A read own", out, err)
	expectBytes(t, "A read own value", out, v)
	acc(t, "A write own")(hA.Invoke(ctx, access.OpWrite, id, []byte("v2")))
	acc(t, "A delete own")(hA.Invoke(ctx, access.OpDelete, id, nil))
	acc(t, "A re-create own")(hA.Invoke(ctx, access.OpCreate, id, v))

	// B reads A's id: the row is not in B's (owner=B, id) namespace → not_found,
	// and specifically NOT access_denied (no R was consulted).
	out, err = hB.Invoke(ctx, access.OpRead, id, nil)
	expectStateNotFoundNotDenied(t, "B read A's id", out, err)
	// B writing/deleting A's id is equally invisible, never a denial.
	out, err = hB.Invoke(ctx, access.OpWrite, id, v)
	expectStateNotFoundNotDenied(t, "B write A's id", out, err)
	out, err = hB.Invoke(ctx, access.OpDelete, id, nil)
	expectStateNotFoundNotDenied(t, "B delete A's id", out, err)
}

// ---- ingress order + op distinctions ----------------------------------------

// TestStateIngressOrderAndOpDistinctions pins the actor-scoped
// ingress order and the collapsed op verdicts through the assembled handle. The
// two signal classes stay distinct: ErrMalformed (wire-shape fault) and the
// RESOLVE-stage verdicts (already_exists / resource_not_found).
func TestStateIngressOrderAndOpDistinctions(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	A := seedMember(t, cs, actor.ActorID("A"))
	hA := cs.Access.MintStateAuthority(identityAuthority{id: A})
	const id = resource.ResourceID("k")

	// step ①: garbage op (out of the closed set) is a wire-shape fault → ErrMalformed.
	_, err := hA.Invoke(ctx, access.Operation("chmod"), id, nil)
	expectMalformed(t, "garbage op", err)
	// the retired set verb is out of the closed set too — same signal.
	_, err = hA.Invoke(ctx, access.Operation("set"), id, nil)
	expectMalformed(t, "retired set verb", err)

	// step ②: empty ResourceID → ErrMalformed.
	_, err = hA.Invoke(ctx, access.OpCreate, resource.ResourceID(""), nil)
	expectMalformed(t, "empty id create", err)

	// create-then-recreate: the second create collides atomically → already_exists.
	acc(t, "create")(hA.Invoke(ctx, access.OpCreate, id, []byte("v")))
	out, err := hA.Invoke(ctx, access.OpCreate, id, []byte("v"))
	expectReason(t, "re-create same id", out, err, access.AlreadyExists)

	// write / delete on an ABSENT id → resource_not_found (birth is create).
	const absent = resource.ResourceID("never-born")
	out, err = hA.Invoke(ctx, access.OpWrite, absent, []byte("v"))
	expectReason(t, "write absent", out, err, access.ResourceNotFound)
	out, err = hA.Invoke(ctx, access.OpDelete, absent, nil)
	expectReason(t, "delete absent", out, err, access.ResourceNotFound)

	// repeated delete is idempotent: delete an existing id, then delete again →
	// honestly not-found (the second call is a safe retry).
	acc(t, "delete existing")(hA.Invoke(ctx, access.OpDelete, id, nil))
	out, err = hA.Invoke(ctx, access.OpDelete, id, nil)
	expectReason(t, "repeat delete idempotent", out, err, access.ResourceNotFound)
}

// ---- empty bytes vs resolved-but-empty --------------------------------------

// TestStateEmptyBytesVsAbsent: the NULL-vs-empty distinction survives the round
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
func TestStateEmptyBytesVsAbsent(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	A := seedMember(t, cs, actor.ActorID("A"))
	hA := cs.Access.MintStateAuthority(identityAuthority{id: A})

	// create(nil) → existing row, NULL bytes → read: accepted, Found:false, Value:nil.
	const nullID = resource.ResourceID("null-bytes")
	acc(t, "create(nil)")(hA.Invoke(ctx, access.OpCreate, nullID, nil))
	out, err := hA.Invoke(ctx, access.OpRead, nullID, nil)
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
	acc(t, "create([]byte{})")(hA.Invoke(ctx, access.OpCreate, emptyID, []byte{}))
	out, err = hA.Invoke(ctx, access.OpRead, emptyID, nil)
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

// ---- WHICH-data = identity ---------------------------------------------------

// TestStateDataKeyedByIdentity: state DATA is keyed by identity (ActorID),
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
func TestStateDataKeyedByIdentity(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	A := seedMember(t, cs, actor.ActorID("A"))
	const id = resource.ResourceID("checkpoint")
	v := []byte("continuity across incarnations")

	h1 := cs.Access.MintStateAuthority(identityAuthority{id: A})
	acc(t, "gen-1 create checkpoint")(h1.Invoke(ctx, access.OpCreate, id, v))

	// A fresh MintState for the same owner (a later "incarnation"'s handle) reads
	// back the earlier handle's bytes: DATA is welded to identity.
	h2 := cs.Access.MintStateAuthority(identityAuthority{id: A})
	out, err := h2.Invoke(ctx, access.OpRead, id, nil)
	expectAccepted(t, "gen-2 read checkpoint", out, err)
	expectBytes(t, "gen-2 checkpoint value", out, v)
}

// ---- deregister touches records only -----------------------------------------

// TestStateDeregisterTouchesRecordsOnly: deregistering an owner flips
// exactly one thing — its registry row. Its actor-scoped state rows stay as
// inert data (ActorIDs are never reused and every belonging is keyed by
// ActorID, so nobody can reach them), and correctness is carried by the
// admission gate, which now refuses the dead owner at the door. The
// channel-scoped plane is non-lossy for the opposite reason: its output is
// shared collaboration truth.
func TestStateDeregisterTouchesRecordsOnly(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)
	A := seedMember(t, cs, actor.ActorID("A"))

	hState := cs.Access.MintStateAuthority(identityAuthority{id: A})
	hChan := cs.Access.MintAuthority(identityAuthority{id: A})

	const stateID = resource.ResourceID("s")
	const kvID = resource.ResourceID("kv:doc")
	kvBytes := []byte("channel-scoped, survives dereg")

	acc(t, "A create actor-scoped state")(hState.Invoke(ctx, access.OpCreate, stateID, []byte("cursor")))
	acc(t, "A create channel-scoped kv")(hChan.Create(ctx, kvID, kvSpec, kvBytes))

	if err := endDeclaredTest(ctx, cs.ChannelStores, A, 100); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// The row survives untouched — a raw store read still finds it.
	value, exists, err := cs.Assembly.State.Read(ctx, A, stateID)
	if err != nil || !exists || string(value) != "cursor" {
		t.Fatalf("dead owner's state row must remain inert data: value=%q exists=%v err=%v",
			value, exists, err)
	}

	// The channel-scoped resource survives: it is collaboration output handed to
	// the channel, not a belonging of its creator. The dead actor's grant row is
	// likewise inert — nothing deletes it, and a live caller can never present
	// that identity again because the admission gate refuses it.
	if _, ok, err := cs.Assembly.Resources.Resolve(ctx, kvID); err != nil || !ok {
		t.Fatalf("channel-scoped resource must survive its creator: ok=%v err=%v", ok, err)
	}
	if _, ok, err := cs.Actors.LookupActive(ctx, A); err != nil || ok {
		t.Fatalf("owner must be inactive after deregister: ok=%v err=%v", ok, err)
	}
}

// ---- two loci mutually invisible ---------------------------------------------

// TestStateTwoLociMutuallyInvisible: the actor-scoped locus (actor_state)
// and the channel-scoped locus (resources) are structurally separate homes — the
// scope law lives in WHICH table, not a column. A channel-scoped handle reading an
// id that only exists in state gets resource_not_found, and vice versa; the same id
// string may live independently in both loci as two distinct rows.
func TestStateTwoLociMutuallyInvisible(t *testing.T) {
	ctx := context.Background()
	cs := openAccessChannel(t)

	A := seedMember(t, cs, actor.ActorID("A")) // member: needed to create a channel-scoped resource

	hChan := cs.Access.MintAuthority(identityAuthority{id: A})
	hState := cs.Access.MintStateAuthority(identityAuthority{id: A})

	const stateOnly = resource.ResourceID("state-only")
	const chanOnly = resource.ResourceID("chan-only")

	acc(t, "create state-only in actor_state")(hState.Invoke(ctx, access.OpCreate, stateOnly, []byte("s")))
	acc(t, "create chan-only in resources")(hChan.Create(ctx, chanOnly, kvSpec, []byte("c")))

	// channel-scoped handle cannot see the state-only id (not in resources).
	out, err := hChan.Invoke(ctx, access.OpRead, stateOnly, nil)
	expectReason(t, "channel handle reads state-only id", out, err, access.ResourceNotFound)

	// actor-scoped handle cannot see the chan-only id (not in actor_state) — and it
	// is not_found, never access_denied (the collapsed branch consults no R).
	out, err = hState.Invoke(ctx, access.OpRead, chanOnly, nil)
	expectStateNotFoundNotDenied(t, "state handle reads chan-only id", out, err)

	// The same id string lives independently in each locus: two distinct rows,
	// each handle reads its own locus's value.
	const dup = resource.ResourceID("dup")
	acc(t, "state create dup")(hState.Invoke(ctx, access.OpCreate, dup, []byte("state-value")))
	acc(t, "channel create dup")(hChan.Create(ctx, dup, kvSpec, []byte("chan-value")))
	out, err = hState.Invoke(ctx, access.OpRead, dup, nil)
	expectAccepted(t, "state read dup", out, err)
	expectBytes(t, "state dup value", out, []byte("state-value"))
	out, err = hChan.Invoke(ctx, access.OpRead, dup, nil)
	expectAccepted(t, "channel read dup", out, err)
	expectBytes(t, "channel dup value", out, []byte("chan-value"))
}

// (driver_error evidence lives in accessdoor/state_test.go
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

