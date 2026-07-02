package store

// White-box tests for the actor-scoped state locus: stateStore (byte realizer
// over actor_state) and the deregister cascade (clearActorScopedTx hung on both
// Deregister and applyMemberRemoveTx). Both stateStore and the registry are
// unexported and reachable only from inside the package — the same §4.5
// confinement the rest of the store relies on. They run over a real channel
// sqlite (ChannelLocalDDL), no fakes.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// stateTestChannelID is the channel every state fixture is bound to (this
// whitebox file is package `store`, so it cannot see helpers_test.go's
// testChannelID, which lives in the external store_test package).
const stateTestChannelID channel.ID = "C-test"

// stateFixture bundles the three plane collaborators over one shared channel
// sqlite so a test can exercise state CRUD, the deregister cascade, and the
// channel-scoped-resources non-cascade contrast against the same db.
type stateFixture struct {
	state *stateStore
	reg   *actorRegistry
	res   *resourceRegistry
}

func openStateFixture(t *testing.T) stateFixture {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "state.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return stateFixture{
		state: newStateStore(db),
		reg:   newActorRegistry(db, stateTestChannelID, nil),
		res:   newResourceRegistry(db),
	}
}

// --- CRUD lifecycle ----------------------------------------------------------

func TestState_CRUDLifecycle(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	val, present, err := f.state.Read(ctx, "actor:a", "cursor")
	if err != nil || !present {
		t.Fatalf("Read after create: present=%v err=%v", present, err)
	}
	if string(val) != "v1" {
		t.Errorf("value=%q want v1", val)
	}

	// Write overwrites an existing row (present=true).
	present, err = f.state.Write(ctx, "actor:a", "cursor", []byte("v2"))
	if err != nil || !present {
		t.Fatalf("Write existing: present=%v err=%v", present, err)
	}
	if val, _, _ := f.state.Read(ctx, "actor:a", "cursor"); string(val) != "v2" {
		t.Errorf("value after write=%q want v2", val)
	}

	// Delete removes the row (present=true), then it is gone.
	present, err = f.state.Delete(ctx, "actor:a", "cursor")
	if err != nil || !present {
		t.Fatalf("Delete existing: present=%v err=%v", present, err)
	}
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); present {
		t.Error("row must be gone after delete")
	}
}

// --- collision sentinel ------------------------------------------------------

func TestState_CreateCollisionSentinel(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := f.state.Create(ctx, "actor:a", "cursor", []byte("v2"))
	if !errors.Is(err, resourcespec.ErrAlreadyExists) {
		t.Fatalf("second Create err=%v want ErrAlreadyExists", err)
	}
	// The colliding create left the original bytes untouched (test-and-set, no
	// clobber).
	if val, _, _ := f.state.Read(ctx, "actor:a", "cursor"); string(val) != "v1" {
		t.Errorf("value after collision=%q want v1 (unchanged)", val)
	}
}

// (No empty-owner/id guard tests: the store validates nothing — owner is a
// coordinate the door welds at mint, and empty ids are rejected at the door's
// ingress. store-not-validate.)

// --- present semantics on absent rows ----------------------------------------

func TestState_PresentFalseOnAbsent(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if _, present, err := f.state.Read(ctx, "actor:ghost", "k"); err != nil || present {
		t.Errorf("Read absent: present=%v err=%v want false/nil", present, err)
	}
	// Write never creates: a missing row is present=false (door → not_found).
	if present, err := f.state.Write(ctx, "actor:ghost", "k", []byte("x")); err != nil || present {
		t.Errorf("Write absent: present=%v err=%v want false/nil", present, err)
	}
	// The failed write must NOT have created a row.
	if _, present, _ := f.state.Read(ctx, "actor:ghost", "k"); present {
		t.Error("Write on absent row must not create it")
	}
	// Delete on a missing row is honestly not-found (present=false), idempotent.
	if present, err := f.state.Delete(ctx, "actor:ghost", "k"); err != nil || present {
		t.Errorf("Delete absent: present=%v err=%v want false/nil", present, err)
	}
}

// --- NULL bytes vs empty-value distinction -----------------------------------

func TestState_NullBytesResolvedButEmpty(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	// create(nil initial) → a present row whose bytes are NULL = resolved-but-
	// empty: present=true, value=nil (door maps to Found=false).
	if err := f.state.Create(ctx, "actor:a", "empty", nil); err != nil {
		t.Fatalf("Create nil: %v", err)
	}
	val, present, err := f.state.Read(ctx, "actor:a", "empty")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !present {
		t.Error("NULL-bytes row must still be present (the row exists)")
	}
	if val != nil {
		t.Errorf("NULL-bytes value=%q want nil", val)
	}

	// An empty non-nil blob is a VALUE, not resolved-but-empty: present=true,
	// value=[]byte{} (non-nil, len 0) — must not collapse into the NULL case.
	if err := f.state.Create(ctx, "actor:a", "blank", []byte{}); err != nil {
		t.Fatalf("Create blank: %v", err)
	}
	val, present, err = f.state.Read(ctx, "actor:a", "blank")
	if err != nil {
		t.Fatalf("Read blank: %v", err)
	}
	if !present || val == nil || len(val) != 0 {
		t.Errorf("empty-blob row: present=%v val=%#v want present, non-nil empty value", present, val)
	}
}

// --- owner isolation: (owner, id) PK, same id under two owners is two rows ----

func TestState_OwnerScopedIsolation(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("A")); err != nil {
		t.Fatalf("Create a: %v", err)
	}
	// Same resource_id under a different owner is a distinct row (no collision).
	if err := f.state.Create(ctx, "actor:b", "cursor", []byte("B")); err != nil {
		t.Fatalf("Create b (same id, other owner) must not collide: %v", err)
	}
	if val, _, _ := f.state.Read(ctx, "actor:a", "cursor"); string(val) != "A" {
		t.Errorf("owner a value=%q want A", val)
	}
	if val, _, _ := f.state.Read(ctx, "actor:b", "cursor"); string(val) != "B" {
		t.Errorf("owner b value=%q want B", val)
	}
}

// --- deregister cascade: entry point #1 (Deregister) -------------------------

func TestState_CascadeClearedOnDeregister(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	// The owner must exist and be active for Deregister to transition (n==1).
	mustInsertActor(t, f.reg, "actor:a")
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := f.state.Create(ctx, "actor:a", "checkpoint", []byte("v2")); err != nil {
		t.Fatalf("Create 2: %v", err)
	}
	// A second owner's state is a control: the cascade must be scoped to owner:a.
	mustInsertActor(t, f.reg, "actor:b")
	if err := f.state.Create(ctx, "actor:b", "cursor", []byte("keep")); err != nil {
		t.Fatalf("Create b: %v", err)
	}

	if err := f.reg.Deregister(ctx, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// owner:a state is gone (both keys).
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); present {
		t.Error("owner:a cursor must be cascade-cleared on deregister")
	}
	if _, present, _ := f.state.Read(ctx, "actor:a", "checkpoint"); present {
		t.Error("owner:a checkpoint must be cascade-cleared on deregister")
	}
	// owner:b untouched.
	if _, present, _ := f.state.Read(ctx, "actor:b", "cursor"); !present {
		t.Error("owner:b state must NOT be cleared by owner:a deregister")
	}
}

// A no-op Deregister (missing / already deregistered) must NOT touch state — the
// cascade only fires when the row actually transitions.
func TestState_NoCascadeOnNoOpDeregister(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Deregister a DIFFERENT, non-existent actor — a no-op that must not clear a.
	if err := f.reg.Deregister(ctx, "actor:ghost", 1); err != nil {
		t.Fatalf("Deregister ghost must be no-op: %v", err)
	}
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); !present {
		t.Error("no-op deregister must not clear another actor's state")
	}

	// First real deregister clears a; the second (already-deregistered) is a
	// no-op and must not error.
	if err := f.reg.Deregister(ctx, "actor:a", 2); err != nil {
		t.Fatalf("Deregister a: %v", err)
	}
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); present {
		t.Error("owner:a state must be cleared by real deregister")
	}
	if err := f.reg.Deregister(ctx, "actor:a", 3); err != nil {
		t.Fatalf("repeat Deregister must be a no-op, got: %v", err)
	}
}

// --- deregister cascade: entry point #2 (applyMemberRemoveTx) ----------------

func TestState_CascadeClearedOnMemberRemove(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	// Add the member, give it state, then remove it via ApplyMemberTransitions.
	if err := f.reg.ApplyMemberTransitions(ctx,
		[]storespec.MemberActorAdd{{ID: "actor:a", Kind: actor.KindTool, At: 100}}, nil); err != nil {
		t.Fatalf("add member: %v", err)
	}
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := f.reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: "actor:a", At: 200}}); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); present {
		t.Error("member state must be cascade-cleared on member remove")
	}

	// A repeated remove (already-deregistered) is a no-op and must not error.
	if err := f.reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: "actor:a", At: 300}}); err != nil {
		t.Fatalf("repeat remove must be no-op: %v", err)
	}
}

// --- non-cascade contrast: channel-scoped resources survive deregister -------

// The channel-scoped resources table is NON-LOSSY: an object outlives its
// creator. Deregistering the creator clears its actor_state but MUST leave its
// resources rows resolvable (they die only on explicit delete / channel
// destroy). This is the structural difference between the two loci.
func TestState_ChannelScopedResourcesSurviveDeregister(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	// actor:a owns both an actor-scoped state row and a channel-scoped resource.
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("state")); err != nil {
		t.Fatalf("Create state: %v", err)
	}
	if err := f.res.Create(ctx, "kv:doc", "kv", "actor:a", []byte("resource")); err != nil {
		t.Fatalf("Create resource: %v", err)
	}

	if err := f.reg.Deregister(ctx, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// actor-scoped state gone...
	if _, present, _ := f.state.Read(ctx, "actor:a", "cursor"); present {
		t.Error("actor-scoped state must be cleared on deregister")
	}
	// ...but the channel-scoped resource survives (non-lossy).
	if _, ok, _ := f.res.Resolve(ctx, "kv:doc"); !ok {
		t.Error("channel-scoped resource must SURVIVE the creator's deregister (non-lossy)")
	}
}

// --- test helpers ------------------------------------------------------------

func mustInsertActor(t *testing.T, reg *actorRegistry, id actor.ActorID) {
	t.Helper()
	if err := reg.Insert(context.Background(), storespec.Record{ID: id, Kind: actor.KindTool, CreatedAt: 1}); err != nil {
		t.Fatalf("Insert %q: %v", id, err)
	}
}
