package store

// White-box tests for the identity-level pending-timer locus: timerStore (the
// timers table realizer, timerspec.TimerStore) and the deregister cascade
// (clearTimersTx hung on both Deregister and applyMemberRemoveTx, parallel to
// clearActorScopedTx). timerStore is unexported and reachable only from
// inside the package — the same confinement the rest of the store relies on
// (a raw TimerStore reachable downstream is a delayed forged-author write
// path around the pen). They run over a real channel sqlite
// (ChannelLocalDDL), no fakes.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// timersTestChannelID is the channel every timers fixture is bound to (this
// whitebox file is package `store`, so it cannot see helpers_test.go's
// testChannelID, which lives in the external store_test package).
const timersTestChannelID channel.ID = "C-test"

// timersFixture bundles the timer store with the actor registry (for the
// dereg-cascade tests) and the channel-scoped resource registry (the
// non-cascade contrast) over one shared channel sqlite.
type timersFixture struct {
	timers *timerStore
	reg    *actorRegistry
	res    *resourceRegistry
}

func openTimersFixture(t *testing.T) timersFixture {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "timers.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return timersFixture{
		timers: newTimerStore(db),
		reg:    newActorRegistry(db, timersTestChannelID, nil),
		res:    newResourceRegistry(db),
	}
}

func mustInsertTimer(t *testing.T, s *timerStore, row timerspec.TimerRow) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO actor_registry (actor_id, actor_kind, created_at) VALUES (?, 'agent', 1)`, string(row.AuthorID)); err != nil {
		t.Fatalf("register timer author: %v", err)
	}
	if err := s.Insert(context.Background(), row); err != nil {
		t.Fatalf("Insert %q: %v", row.ID, err)
	}
}

// --- Insert + Due --------------------------------------------------------

func TestTimer_InsertAndDue(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	mustInsertTimer(t, f.timers, timerspec.TimerRow{
		ID: "t1", AuthorID: "actor:a", FireAt: 3000, Type: "wake", Payload: []byte(`{"n":1}`),
		CorrelationID: "corr-1", CreatedAt: 100,
	})
	mustInsertTimer(t, f.timers, timerspec.TimerRow{
		ID: "t2", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 100,
	})
	mustInsertTimer(t, f.timers, timerspec.TimerRow{
		ID: "t3", AuthorID: "actor:b", FireAt: 2000, Type: "wake", CreatedAt: 100,
	})

	// now=2000 must return t2 (1000) and t3 (2000), ordered by fire_at, and
	// exclude t1 (3000, not yet due).
	due, err := f.timers.Due(ctx, 2000)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("Due(2000) len=%d want 2: %+v", len(due), due)
	}
	if due[0].ID != "t2" || due[1].ID != "t3" {
		t.Errorf("Due(2000) order=%v want [t2 t3]", []timerspec.TimerID{due[0].ID, due[1].ID})
	}

	// A round trip preserves the row's fields (fetch t1 by pushing now past it).
	due, err = f.timers.Due(ctx, 3000)
	if err != nil {
		t.Fatalf("Due(3000): %v", err)
	}
	var got timerspec.TimerRow
	for _, r := range due {
		if r.ID == "t1" {
			got = r
		}
	}
	if got.AuthorID != "actor:a" || got.Type != "wake" || string(got.Payload) != `{"n":1}` ||
		got.CorrelationID != "corr-1" || got.CreatedAt != 100 || got.FireAt != 3000 {
		t.Errorf("t1 round-trip=%+v", got)
	}
}

func TestTimer_DueIsFairAcrossAuthors(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)
	for i := 0; i < duePerAuthor+5; i++ {
		mustInsertTimer(t, f.timers, timerspec.TimerRow{
			ID: timerspec.TimerID(fmt.Sprintf("a-%03d", i)), AuthorID: "actor:a",
			FireAt: int64(i), Type: "wake", CreatedAt: 1,
		})
	}
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "b-only", AuthorID: "actor:b", FireAt: 999, Type: "wake", CreatedAt: 1})

	due, err := f.timers.Due(ctx, 1000)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != duePerAuthor+1 {
		t.Fatalf("Due len=%d want %d", len(due), duePerAuthor+1)
	}
	if due[len(due)-1].ID != "b-only" {
		t.Fatalf("other author's due timer was globally truncated: tail=%q", due[len(due)-1].ID)
	}
}

// --- Delete ----------------------------------------------------------------

func TestTimer_Delete(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 1})

	existed, err := f.timers.Delete(ctx, "t1")
	if err != nil || !existed {
		t.Fatalf("Delete existing: existed=%v err=%v", existed, err)
	}
	// Row is gone (confirmed via Due with a far-future now).
	due, err := f.timers.Due(ctx, 999999)
	if err != nil || len(due) != 0 {
		t.Fatalf("Due after delete=%+v err=%v want empty", due, err)
	}

	// Deleting an absent row is honestly existed=false, not an error (Cancel
	// after fire is a no-op).
	existed, err = f.timers.Delete(ctx, "t1")
	if err != nil || existed {
		t.Fatalf("Delete absent: existed=%v err=%v want false/nil", existed, err)
	}
}

// --- NextFireAt --------------------------------------------------------------

func TestTimer_NextFireAt(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	if _, ok, err := f.timers.NextFireAt(ctx); err != nil || ok {
		t.Fatalf("NextFireAt empty: ok=%v err=%v want false/nil", ok, err)
	}

	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 5000, Type: "wake", CreatedAt: 1})
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t2", AuthorID: "actor:a", FireAt: 2000, Type: "wake", CreatedAt: 1})
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t3", AuthorID: "actor:a", FireAt: 8000, Type: "wake", CreatedAt: 1})

	fireAt, ok, err := f.timers.NextFireAt(ctx)
	if err != nil || !ok || fireAt != 2000 {
		t.Fatalf("NextFireAt=%d ok=%v err=%v want 2000/true/nil", fireAt, ok, err)
	}

	// Removing the earliest advances the frontier.
	if _, err := f.timers.Delete(ctx, "t2"); err != nil {
		t.Fatalf("Delete t2: %v", err)
	}
	fireAt, ok, err = f.timers.NextFireAt(ctx)
	if err != nil || !ok || fireAt != 5000 {
		t.Fatalf("NextFireAt after delete=%d ok=%v err=%v want 5000/true/nil", fireAt, ok, err)
	}
}

// --- CancelOwned -------------------------------------------------------------

func TestTimer_CancelOwned(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 1})

	// A foreign author must not be able to cancel — existed=false, no leak, row
	// untouched.
	existed, err := f.timers.CancelOwned(ctx, "t1", "actor:eve")
	if err != nil || existed {
		t.Fatalf("CancelOwned foreign author: existed=%v err=%v want false/nil", existed, err)
	}
	due, _ := f.timers.Due(ctx, 999999)
	if len(due) != 1 {
		t.Fatalf("row must survive a foreign CancelOwned, got %+v", due)
	}

	// The true owner can cancel.
	existed, err = f.timers.CancelOwned(ctx, "t1", "actor:a")
	if err != nil || !existed {
		t.Fatalf("CancelOwned owner: existed=%v err=%v want true/nil", existed, err)
	}
	due, _ = f.timers.Due(ctx, 999999)
	if len(due) != 0 {
		t.Fatalf("row must be gone after owner CancelOwned, got %+v", due)
	}

	// Cancelling an absent id (already fired / never existed) is a silent no-op.
	existed, err = f.timers.CancelOwned(ctx, "t1", "actor:a")
	if err != nil || existed {
		t.Fatalf("CancelOwned absent: existed=%v err=%v want false/nil", existed, err)
	}
}

// --- deregister cascade: entry point #1 (Deregister) -------------------------

func TestTimer_CascadeClearedOnDeregister(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 1})
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t2", AuthorID: "actor:a", FireAt: 2000, Type: "wake", CreatedAt: 1})

	// A second owner's timer is a control: the cascade must be scoped to a.
	mustInsertActor(t, f.reg, "actor:b")
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t3", AuthorID: "actor:b", FireAt: 1000, Type: "wake", CreatedAt: 1})

	// A channel-scoped resource owned by a is a control for the OTHER locus:
	// resources are non-lossy and must survive the creator's deregister.
	if err := f.res.Create(ctx, "kv:doc", "kv", "actor:a", "", "", resourcespec.ProvenanceAxisAllocated, []byte("resource")); err != nil {
		t.Fatalf("Create resource: %v", err)
	}

	if err := f.reg.Deregister(ctx, "actor:a", 1000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	due, err := f.timers.Due(ctx, 999999)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 1 || due[0].ID != "t3" {
		t.Fatalf("after deregister(a) timers=%+v want only t3 (owner b)", due)
	}
	if _, ok, _ := f.res.Resolve(ctx, "kv:doc"); !ok {
		t.Error("channel-scoped resource must SURVIVE the creator's deregister (non-lossy, different locus)")
	}
}

// A no-op Deregister (missing / already deregistered) must NOT touch timers —
// the cascade only fires when the row actually transitions.
func TestTimer_NoCascadeOnNoOpDeregister(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	mustInsertActor(t, f.reg, "actor:a")
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 1})

	if err := f.reg.Deregister(ctx, "actor:ghost", 1); err != nil {
		t.Fatalf("Deregister ghost must be no-op: %v", err)
	}
	if due, _ := f.timers.Due(ctx, 999999); len(due) != 1 {
		t.Errorf("no-op deregister must not clear another actor's timers, got %+v", due)
	}

	if err := f.reg.Deregister(ctx, "actor:a", 2); err != nil {
		t.Fatalf("Deregister a: %v", err)
	}
	if due, _ := f.timers.Due(ctx, 999999); len(due) != 0 {
		t.Errorf("real deregister must clear timers, got %+v", due)
	}
	// A repeat is a no-op and must not error.
	if err := f.reg.Deregister(ctx, "actor:a", 3); err != nil {
		t.Fatalf("repeat Deregister must be a no-op, got: %v", err)
	}
}

// --- deregister cascade: entry point #2 (applyMemberRemoveTx) ----------------

func TestTimer_CascadeClearedOnMemberRemove(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	if err := f.reg.insertFixedID(ctx, storespec.Record{ID: "actor:a", Kind: actor.KindTool, CreatedAt: 100}); err != nil {
		t.Fatalf("add member: %v", err)
	}
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1000, Type: "wake", CreatedAt: 1})

	if err := f.reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: "actor:a", At: 200}}); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if due, _ := f.timers.Due(ctx, 999999); len(due) != 0 {
		t.Errorf("member state must be cascade-cleared on member remove, got %+v", due)
	}

	// A repeated remove (already-deregistered) is a no-op and must not error.
	if err := f.reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: "actor:a", At: 300}}); err != nil {
		t.Fatalf("repeat remove must be no-op: %v", err)
	}
}

// --- attach-reconcile host guard (期7 review P1a) -----------------------------

// TestMemberRemove_ExpectedHostGuard_MigrationWindowNoOp pins the host-flip
// TOCTOU closure on the attach-reconcile remove arm: daemon A snapshots its
// owned rows, the actor re-homes to daemon B (host-only UPDATE), and A's stale
// remove lands AFTER the flip. With ExpectedHost set the UPDATE carries
// `AND host=?`, so the flipped row is a 0-rows-affected no-op — B's active row
// AND its cascaded loci (state, identity timers) survive intact. The unguarded
// (product-level) remove semantics are untouched: a remove guarded on the
// CURRENT host — or carrying no guard at all — still deregisters and cascades.
func TestMemberRemove_ExpectedHostGuard_MigrationWindowNoOp(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "hostguard.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reg := newActorRegistry(db, timersTestChannelID, nil)
	timers := newTimerStore(db)
	state := newStateStore(db)

	const id = actor.ActorID("tool:migrant")
	// Registered on daemon A, with actor-scoped state and an identity timer.
	if err := reg.insertFixedID(ctx, storespec.Record{ID: id, Kind: actor.KindTool, Host: "daemon-a", CreatedAt: 100}); err != nil {
		t.Fatalf("add on daemon-a: %v", err)
	}
	if err := state.Create(ctx, id, "cursor", []byte("v1")); err != nil {
		t.Fatalf("Create state: %v", err)
	}
	mustInsertTimer(t, timers, timerspec.TimerRow{ID: "t1", AuthorID: id, FireAt: 1000, Type: "wake", CreatedAt: 1})

	// Migration: the row re-homes to daemon B before A's reconcile applies.
	if err := reg.ApplyMemberTransitions(ctx,
		[]storespec.MemberActorAdd{{ID: id, Kind: actor.KindTool, Host: "daemon-b", At: 200}}, nil); err != nil {
		t.Fatalf("re-home to daemon-b: %v", err)
	}

	// Daemon A's stale reconcile remove, guarded on its own host: MUST no-op.
	if err := reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: id, ExpectedHost: "daemon-a", At: 300}}); err != nil {
		t.Fatalf("stale guarded remove must not error: %v", err)
	}
	rec, ok, err := reg.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup after stale remove ok=%v err=%v", ok, err)
	}
	if !rec.IsActive() || rec.Host != "daemon-b" {
		t.Fatalf("B's row damaged by A's stale remove: active=%v host=%q, want active on daemon-b", rec.IsActive(), rec.Host)
	}
	if _, exists, _ := state.Read(ctx, id, "cursor"); !exists {
		t.Error("actor state was cascade-cleared by a no-op guarded remove")
	}
	if due, _ := timers.Due(ctx, 999999); len(due) != 1 || due[0].ID != "t1" {
		t.Errorf("identity timers were cascade-cleared by a no-op guarded remove: %+v", due)
	}

	// A remove guarded on the row's CURRENT host still deregisters + cascades
	// (B's own later reconcile) — the guard narrows, it never disables.
	if err := reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: id, ExpectedHost: "daemon-b", At: 400}}); err != nil {
		t.Fatalf("matching guarded remove: %v", err)
	}
	rec, ok, err = reg.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup after matching remove ok=%v err=%v", ok, err)
	}
	if rec.IsActive() {
		t.Fatal("matching guarded remove did not deregister the row")
	}
	if _, exists, _ := state.Read(ctx, id, "cursor"); exists {
		t.Error("matching guarded remove did not cascade-clear state")
	}
	if due, _ := timers.Due(ctx, 999999); len(due) != 0 {
		t.Errorf("matching guarded remove did not cascade-clear timers: %+v", due)
	}
}
