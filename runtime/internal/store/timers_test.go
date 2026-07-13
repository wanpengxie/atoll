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
	"errors"
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

// --- timer_dead: faithful column-for-column relocation -----------------------

// TestTimer_MoveToDead_RelocatesEveryColumnFaithfully pins the dead-letter
// contract: MoveToDead lifts the LIVE row into timer_dead preserving every
// carried column (timer_id/author_id/fire_at/type/payload/correlation_id/
// created_at) byte-for-byte, stamps the four death columns
// (death_class/reason/detail/died_at) exactly as supplied, and deletes the
// source. A drifting column (e.g. payload dropped, correlation_id lost) would
// silently corrupt the forensic record a poison-row disposal exists to keep.
func TestTimer_MoveToDead_RelocatesEveryColumnFaithfully(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)

	src := timerspec.TimerRow{
		ID: "t-dead", AuthorID: "actor:a", FireAt: 4242, Type: "wake",
		Payload: []byte(`{"k":"v"}`), CorrelationID: "corr-xyz", CreatedAt: 777,
	}
	mustInsertTimer(t, f.timers, src)

	const (
		reason = "harness_reserved_type_unauthorized_sender"
		detail = "the gory detail"
		diedAt = int64(999888)
	)
	moved, evicted, err := f.timers.MoveToDead(ctx, src.ID, timerspec.DeathFireRejected, reason, detail, diedAt)
	if err != nil || !moved || evicted != 0 {
		t.Fatalf("MoveToDead: moved=%v evicted=%d err=%v want true/0/nil", moved, evicted, err)
	}

	// Source row is gone (moved, not copied).
	if due, _ := f.timers.Due(ctx, 999999); len(due) != 0 {
		t.Fatalf("source timers row survived MoveToDead: %+v", due)
	}

	// Read the dead row directly and compare every column to the source.
	var (
		gotID, gotAuthor, gotType, gotCorr, gotClass, gotReason, gotDetail string
		gotFireAt, gotCreated, gotDied                                     int64
		gotPayload                                                         []byte
	)
	row := f.timers.db.QueryRowContext(ctx, `SELECT timer_id,author_id,fire_at,type,payload,correlation_id,created_at,death_class,reason,detail,died_at FROM timer_dead WHERE timer_id=?`, string(src.ID))
	if err := row.Scan(&gotID, &gotAuthor, &gotFireAt, &gotType, &gotPayload, &gotCorr, &gotCreated, &gotClass, &gotReason, &gotDetail, &gotDied); err != nil {
		t.Fatalf("scan timer_dead: %v", err)
	}
	if gotID != string(src.ID) || gotAuthor != string(src.AuthorID) || gotFireAt != src.FireAt ||
		gotType != src.Type || string(gotPayload) != string(src.Payload) || gotCorr != src.CorrelationID ||
		gotCreated != src.CreatedAt {
		t.Errorf("carried columns drifted: id=%q author=%q fire=%d type=%q payload=%q corr=%q created=%d\n want id=%q author=%q fire=%d type=%q payload=%q corr=%q created=%d",
			gotID, gotAuthor, gotFireAt, gotType, gotPayload, gotCorr, gotCreated,
			src.ID, src.AuthorID, src.FireAt, src.Type, src.Payload, src.CorrelationID, src.CreatedAt)
	}
	if gotClass != string(timerspec.DeathFireRejected) || gotReason != reason || gotDetail != detail || gotDied != diedAt {
		t.Errorf("death columns wrong: class=%q reason=%q detail=%q died=%d want %q/%q/%q/%d",
			gotClass, gotReason, gotDetail, gotDied, timerspec.DeathFireRejected, reason, detail, diedAt)
	}
}

// TestTimer_MoveToDead_RejectsZeroDeathClass: the death_class column is a typed
// closed set {fire_rejected, revive_rejected}; a zero/unknown class is refused
// at the store boundary (never written as a blank forensic record).
func TestTimer_MoveToDead_RejectsZeroDeathClass(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)
	mustInsertTimer(t, f.timers, timerspec.TimerRow{ID: "t1", AuthorID: "actor:a", FireAt: 1, Type: "wake", CreatedAt: 1})
	if _, _, err := f.timers.MoveToDead(ctx, "t1", timerspec.DeathClass(""), "r", "d", 1); err == nil {
		t.Fatal("MoveToDead with zero death class: got nil error, want reject")
	}
	// Missing source row is an honest moved=false, not an error (idempotent re-death).
	moved, _, err := f.timers.MoveToDead(ctx, "ghost", timerspec.DeathFireRejected, "r", "d", 1)
	if err != nil || moved {
		t.Fatalf("MoveToDead absent row: moved=%v err=%v want false/nil", moved, err)
	}
}

// TestTimer_MoveToDead_RingEviction pins the bounded dead-letter ring: once the
// table is full (maxDeadTimers), each further death evicts the OLDEST dead row
// (lowest dead_seq), the eviction count surfaces to the caller, and the row
// count is capped — the forensic buffer never grows without bound.
func TestTimer_MoveToDead_RingEviction(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)
	// Shrink the ring: the semantics under test (evict-oldest, count surfaced,
	// row count capped) are size-independent, and at the production 4096 this
	// test burned 34s in ~8200 fsync'd transactions. The production value is
	// pinned by TestTimerCapsProductionValues.
	prev := maxDeadTimers
	maxDeadTimers = 8
	t.Cleanup(func() { maxDeadTimers = prev })

	// Register the shared author once; each iteration inserts one live row then
	// immediately moves it dead (so pending never exceeds the per-author quota).
	if _, err := f.timers.db.Exec(`INSERT OR IGNORE INTO actor_registry (actor_id, actor_kind, created_at) VALUES ('actor:a', 'agent', 1)`); err != nil {
		t.Fatalf("register author: %v", err)
	}

	overflow := maxDeadTimers + 1
	totalEvicted := 0
	firstEvictedByCall := -1
	for i := 0; i < overflow; i++ {
		id := timerspec.TimerID(fmt.Sprintf("t-%06d", i))
		if err := f.timers.Insert(ctx, timerspec.TimerRow{ID: id, AuthorID: "actor:a", FireAt: int64(i), Type: "wake", CreatedAt: int64(i)}); err != nil {
			t.Fatalf("Insert %q: %v", id, err)
		}
		_, evicted, err := f.timers.MoveToDead(ctx, id, timerspec.DeathFireRejected, "r", "d", int64(i))
		if err != nil {
			t.Fatalf("MoveToDead %q: %v", id, err)
		}
		if evicted > 0 && firstEvictedByCall < 0 {
			firstEvictedByCall = i
		}
		totalEvicted += evicted
	}

	// Exactly one eviction total (we overflowed by one), and it happened on the
	// call that pushed the table one past the cap.
	if totalEvicted != 1 {
		t.Fatalf("total evicted = %d over %d deaths, want exactly 1", totalEvicted, overflow)
	}
	if firstEvictedByCall != maxDeadTimers {
		t.Fatalf("eviction first surfaced on death #%d, want #%d (the cap+1'th)", firstEvictedByCall, maxDeadTimers)
	}

	// Table is capped at maxDeadTimers.
	var count int
	if err := f.timers.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timer_dead`).Scan(&count); err != nil {
		t.Fatalf("count timer_dead: %v", err)
	}
	if count != maxDeadTimers {
		t.Fatalf("timer_dead row count = %d, want capped at %d", count, maxDeadTimers)
	}

	// The oldest death (t-000000) was the evicted one; the newest survives.
	var present int
	_ = f.timers.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timer_dead WHERE timer_id='t-000000'`).Scan(&present)
	if present != 0 {
		t.Fatal("oldest dead row was not evicted (ring must drop lowest dead_seq)")
	}
	_ = f.timers.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timer_dead WHERE timer_id=?`, fmt.Sprintf("t-%06d", overflow-1)).Scan(&present)
	if present != 1 {
		t.Fatal("newest dead row missing after eviction (ring must keep newest dead_seq)")
	}
}

// TestTimer_Insert_DurableQuota pins the durable-side admission ceiling: the
// (maxPendingTimersPerAuthor+1)'th pending row for one author is refused with
// ErrScheduleQuota, while a DIFFERENT author is unaffected (the quota is
// per-author, not global).
func TestTimer_Insert_DurableQuota(t *testing.T) {
	ctx := context.Background()
	f := openTimersFixture(t)
	// Shrink the quota: per-author admission semantics are size-independent
	// (see the ring-eviction test's note); production value pinned by
	// TestTimerCapsProductionValues.
	prev := maxPendingTimersPerAuthor
	maxPendingTimersPerAuthor = 8
	t.Cleanup(func() { maxPendingTimersPerAuthor = prev })

	if _, err := f.timers.db.Exec(`INSERT OR IGNORE INTO actor_registry (actor_id, actor_kind, created_at) VALUES ('actor:a', 'agent', 1)`); err != nil {
		t.Fatalf("register author a: %v", err)
	}
	if _, err := f.timers.db.Exec(`INSERT OR IGNORE INTO actor_registry (actor_id, actor_kind, created_at) VALUES ('actor:b', 'agent', 1)`); err != nil {
		t.Fatalf("register author b: %v", err)
	}

	for i := 0; i < maxPendingTimersPerAuthor; i++ {
		id := timerspec.TimerID(fmt.Sprintf("a-%05d", i))
		if err := f.timers.Insert(ctx, timerspec.TimerRow{ID: id, AuthorID: "actor:a", FireAt: int64(i), Type: "wake", CreatedAt: 1}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	// The next row for actor:a is over quota.
	err := f.timers.Insert(ctx, timerspec.TimerRow{ID: "a-over", AuthorID: "actor:a", FireAt: 1, Type: "wake", CreatedAt: 1})
	if !errors.Is(err, timerspec.ErrScheduleQuota) {
		t.Fatalf("Insert over quota: err=%v want ErrScheduleQuota", err)
	}
	// A different author at zero pending is unaffected (quota is per-author).
	if err := f.timers.Insert(ctx, timerspec.TimerRow{ID: "b-1", AuthorID: "actor:b", FireAt: 1, Type: "wake", CreatedAt: 1}); err != nil {
		t.Fatalf("Insert for unrelated author must succeed, got %v", err)
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

// TestTimerCapsProductionValues pins the production sizes of the two timer
// caps that tests above temporarily shrink — the values themselves are拍定
// constants (W3 S2: quota 千行级 / dead-letter ring 4096), and this pin is
// what lets the semantic tests run at toy size without silently losing the
// chosen production numbers.
func TestTimerCapsProductionValues(t *testing.T) {
	if maxPendingTimersPerAuthor != 1024 {
		t.Fatalf("maxPendingTimersPerAuthor = %d, want the拍定 1024", maxPendingTimersPerAuthor)
	}
	if maxDeadTimers != 4096 {
		t.Fatalf("maxDeadTimers = %d, want the拍定 4096", maxDeadTimers)
	}
}
