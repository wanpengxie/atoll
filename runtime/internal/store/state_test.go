package store

// White-box tests for the actor-scoped state locus: stateStore (byte realizer
// over actor_state) and the deregister cascade (clearActorScopedTx hung on both
// Deregister and applyMemberRemoveTx). Both stateStore and the registry are
// unexported and reachable only from inside the package — the same
// package-private confinement the rest of the store relies on. They run over
// a real channel sqlite (ChannelLocalDDL), no fakes.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
	mustInsertActor(t, f.reg, "actor:a")

	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	val, exists, err := f.state.Read(ctx, "actor:a", "cursor")
	if err != nil || !exists {
		t.Fatalf("Read after create: exists=%v err=%v", exists, err)
	}
	if string(val) != "v1" {
		t.Errorf("value=%q want v1", val)
	}

	// Write overwrites an existing row (exists=true).
	exists, err = f.state.Write(ctx, "actor:a", "cursor", []byte("v2"))
	if err != nil || !exists {
		t.Fatalf("Write existing: exists=%v err=%v", exists, err)
	}
	if val, _, _ := f.state.Read(ctx, "actor:a", "cursor"); string(val) != "v2" {
		t.Errorf("value after write=%q want v2", val)
	}

	// Delete removes the row (exists=true), then it is gone.
	exists, err = f.state.Delete(ctx, "actor:a", "cursor")
	if err != nil || !exists {
		t.Fatalf("Delete existing: exists=%v err=%v", exists, err)
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); exists {
		t.Error("row must be gone after delete")
	}
}

// --- collision sentinel ------------------------------------------------------

func TestState_CreateCollisionSentinel(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)
	mustInsertActor(t, f.reg, "actor:a")

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

// --- exists semantics on absent rows ----------------------------------------

func TestState_PresentFalseOnAbsent(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if _, exists, err := f.state.Read(ctx, "actor:ghost", "k"); err != nil || exists {
		t.Errorf("Read absent: exists=%v err=%v want false/nil", exists, err)
	}
	// Write never creates: a missing row is exists=false (door → not_found).
	if exists, err := f.state.Write(ctx, "actor:ghost", "k", []byte("x")); err != nil || exists {
		t.Errorf("Write absent: exists=%v err=%v want false/nil", exists, err)
	}
	// The failed write must NOT have created a row.
	if _, exists, _ := f.state.Read(ctx, "actor:ghost", "k"); exists {
		t.Error("Write on absent row must not create it")
	}
	// Delete on a missing row is honestly not-found (exists=false), idempotent.
	if exists, err := f.state.Delete(ctx, "actor:ghost", "k"); err != nil || exists {
		t.Errorf("Delete absent: exists=%v err=%v want false/nil", exists, err)
	}
}

// --- NULL bytes vs empty-value distinction -----------------------------------

func TestState_NullBytesResolvedButEmpty(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)
	mustInsertActor(t, f.reg, "actor:a")

	// create(nil initial) → an existing row whose bytes are NULL = resolved-but-
	// empty: exists=true, value=nil (door maps to Found=false).
	if err := f.state.Create(ctx, "actor:a", "empty", nil); err != nil {
		t.Fatalf("Create nil: %v", err)
	}
	val, exists, err := f.state.Read(ctx, "actor:a", "empty")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !exists {
		t.Error("NULL-bytes row must still exist")
	}
	if val != nil {
		t.Errorf("NULL-bytes value=%q want nil", val)
	}

	// An empty non-nil blob is a VALUE, not resolved-but-empty: exists=true,
	// value=[]byte{} (non-nil, len 0) — must not collapse into the NULL case.
	if err := f.state.Create(ctx, "actor:a", "blank", []byte{}); err != nil {
		t.Fatalf("Create blank: %v", err)
	}
	val, exists, err = f.state.Read(ctx, "actor:a", "blank")
	if err != nil {
		t.Fatalf("Read blank: %v", err)
	}
	if !exists || val == nil || len(val) != 0 {
		t.Errorf("empty-blob row: exists=%v val=%#v want existing row, non-nil empty value", exists, val)
	}
}

// --- owner isolation: (owner, id) PK, same id under two owners is two rows ----

func TestState_OwnerScopedIsolation(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)
	mustInsertActor(t, f.reg, "actor:a")
	mustInsertActor(t, f.reg, "actor:b")

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

func TestState_CreateRejectsInactiveOwnerWithoutResidue(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	// Missing and deregistered are the same inactive-owner fact. Neither order
	// may leave a state row behind.
	if err := f.state.Create(ctx, "actor:missing", "cursor", []byte("x")); !errors.Is(err, resourcespec.ErrOwnerInactive) {
		t.Fatalf("missing owner Create err=%v want ErrOwnerInactive", err)
	}
	if _, exists, err := f.state.Read(ctx, "actor:missing", "cursor"); err != nil || exists {
		t.Fatalf("missing owner residue: exists=%v err=%v", exists, err)
	}

	mustInsertActor(t, f.reg, "actor:gone")
	if err := endActorForTest(ctx, f.reg, "actor:gone", 2); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if err := f.state.Create(ctx, "actor:gone", "cursor", []byte("x")); !errors.Is(err, resourcespec.ErrOwnerInactive) {
		t.Fatalf("deregistered owner Create err=%v want ErrOwnerInactive", err)
	}
	if _, exists, err := f.state.Read(ctx, "actor:gone", "cursor"); err != nil || exists {
		t.Fatalf("deregistered owner residue: exists=%v err=%v", exists, err)
	}
}

func TestState_CreateInactiveTakesPriorityOverCollision(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)
	mustInsertActor(t, f.reg, "actor:a")
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("old")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Simulate the classification shape directly: an inactive registry row and
	// an old colliding state row. The public deregister path normally cascades
	// the row, but inactive must still win if recovery encounters this state.
	if _, err := f.state.db.ExecContext(ctx, `UPDATE actor_registry SET deregistered_at=2 WHERE actor_id='actor:a'`); err != nil {
		t.Fatalf("mark inactive: %v", err)
	}
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("new")); !errors.Is(err, resourcespec.ErrOwnerInactive) {
		t.Fatalf("Create err=%v want inactive to precede collision", err)
	}
	if got, _, _ := f.state.Read(ctx, "actor:a", "cursor"); string(got) != "old" {
		t.Fatalf("colliding bytes changed to %q", got)
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

	if err := endActorForTest(ctx, f.reg, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// owner:a state is gone (both keys).
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); exists {
		t.Error("owner:a cursor must be cascade-cleared on deregister")
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "checkpoint"); exists {
		t.Error("owner:a checkpoint must be cascade-cleared on deregister")
	}
	// owner:b untouched.
	if _, exists, _ := f.state.Read(ctx, "actor:b", "cursor"); !exists {
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
	if err := endActorForTest(ctx, f.reg, "actor:ghost", 1); err != nil {
		t.Fatalf("Deregister ghost must be no-op: %v", err)
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); !exists {
		t.Error("no-op deregister must not clear another actor's state")
	}

	// First real deregister clears a; the second (already-deregistered) is a
	// no-op and must not error.
	if err := endActorForTest(ctx, f.reg, "actor:a", 2); err != nil {
		t.Fatalf("Deregister a: %v", err)
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); exists {
		t.Error("owner:a state must be cleared by real deregister")
	}
	if err := endActorForTest(ctx, f.reg, "actor:a", 3); err != nil {
		t.Fatalf("repeat Deregister must be a no-op, got: %v", err)
	}
}

// --- deregister cascade: entry point #2 (applyMemberRemoveTx) ----------------

func TestState_CascadeClearedOnMemberRemove(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	// Add the identity, give it state, then end it through the cascade path.
	if err := f.reg.insertFixedID(ctx, storespec.Record{ID: "actor:a", Kind: actor.KindTool, CreatedAt: 100}); err != nil {
		t.Fatalf("add member: %v", err)
	}
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := endActorForTest(ctx, f.reg, "actor:a", 200); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); exists {
		t.Error("member state must be cascade-cleared on member remove")
	}

	// A repeated remove (already-deregistered) is a no-op and must not error.
	if err := endActorForTest(ctx, f.reg, "actor:a", 300); err != nil {
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
	if err := f.res.Create(ctx, "kv:doc", "kv", "actor:a", "", "", []byte("resource"), resourcespec.ResourceBirthPlan{}); err != nil {
		t.Fatalf("Create resource: %v", err)
	}

	if err := endActorForTest(ctx, f.reg, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// actor-scoped state gone...
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); exists {
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
	if err := reg.insertFixedID(context.Background(), storespec.Record{ID: id, Kind: actor.KindTool, CreatedAt: 1}); err != nil {
		t.Fatalf("Insert %q: %v", id, err)
	}
}
