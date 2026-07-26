package store

// White-box tests for the actor-scoped state locus: stateStore (byte realizer
// over actor_state). There is no deregister cascade to test — terminal touches
// actor_registry alone and leaves a dead owner's rows as inert, unreachable
// data. Both stateStore and the registry are
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
// sqlite so a test can exercise state CRUD, owner deregistration (which touches
// the registry row alone) and channel-scoped resources against the same db.
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

// The store is mechanical: it does not re-judge membership. An owner that the
// door never admitted simply gets its row — correctness for "may this actor
// write state" lives at the door's admission, and a second EXISTS check here
// would be a second authority (and would let a racing End veto an
// already-admitted call twice).
func TestState_CreateDoesNotReJudgeMembership(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	if err := f.state.Create(ctx, "actor:missing", "cursor", []byte("x")); err != nil {
		t.Fatalf("Create for an unadmitted owner must be mechanical, got %v", err)
	}
	if _, exists, err := f.state.Read(ctx, "actor:missing", "cursor"); err != nil || !exists {
		t.Fatalf("row must exist: exists=%v err=%v", exists, err)
	}
	// A collision is still a collision.
	if err := f.state.Create(ctx, "actor:missing", "cursor", []byte("y")); !errors.Is(err, resourcespec.ErrAlreadyExists) {
		t.Fatalf("Create collision err=%v want ErrAlreadyExists", err)
	}
	if got, _, _ := f.state.Read(ctx, "actor:missing", "cursor"); string(got) != "x" {
		t.Fatalf("colliding bytes changed to %q", got)
	}
}

// --- deregister touches records ONLY ----------------------------------------

// ActorIDs are never reused and every belonging is keyed by ActorID, so a dead
// owner's state rows are unreachable inert data. Deregister therefore clears
// nothing: reclaiming them is an explicit batch management action, never
// lifecycle logic.
func TestState_DeregisterLeavesOwnerStateInert(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	if err := f.state.Create(ctx, "actor:a", "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustInsertActor(t, f.reg, "actor:b")
	if err := f.state.Create(ctx, "actor:b", "cursor", []byte("keep")); err != nil {
		t.Fatalf("Create b: %v", err)
	}

	if err := endActorForTest(ctx, f.reg, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, exists, _ := f.state.Read(ctx, "actor:a", "cursor"); !exists {
		t.Error("deregister must not clear the dead owner's state (inert data)")
	}
	if _, exists, _ := f.state.Read(ctx, "actor:b", "cursor"); !exists {
		t.Error("owner:b state must be untouched")
	}
	// Repeating the latch is a no-op, never an error.
	if err := endActorForTest(ctx, f.reg, "actor:a", 1001); err != nil {
		t.Fatalf("repeat deregister must be a no-op, got: %v", err)
	}
	// Deregistering an id with no row is a no-op too.
	if err := endActorForTest(ctx, f.reg, "actor:ghost", 1); err != nil {
		t.Fatalf("deregister of a missing id must be a no-op: %v", err)
	}
}

// Channel-scoped resources are shared collaboration output: they outlive their
// creator unconditionally.
func TestState_ChannelScopedResourcesSurviveDeregister(t *testing.T) {
	ctx := context.Background()
	f := openStateFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	if err := f.res.Create(ctx, "kv:doc", "kv", "actor:a", "", "", []byte("resource"), resourcespec.ResourceBirthPlan{}); err != nil {
		t.Fatalf("Create resource: %v", err)
	}
	if err := endActorForTest(ctx, f.reg, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
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
