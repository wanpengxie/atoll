package store

// Single-transaction-bed atomicity for the actor record store.
//
// actorRegistry.Insert and actorRegistry.UpdateDefinition both DECLARE in their
// doc comments that the whole verb rides ONE transaction: "a crash at any point
// either leaves nothing (retry starts over) or leaves everything", and "every
// fallible step sits before the commit". These tests make that declaration
// falsifiable by injecting real SQLite faults into a real channel sqlite — no
// fake driver, no injected error value.
//
// Two fault shapes, deliberately both:
//
//   - STATEMENT fault (BEFORE-trigger RAISE(ABORT)): the verb's write never
//     lands. Proves the earlier in-transaction work (semantic-key lookup, id
//     mint / row read) leaves no residue of its own.
//   - COMMIT fault (AFTER-trigger insert violating a DEFERRABLE INITIALLY
//     DEFERRED foreign key, which SQLite only reports at COMMIT): the verb's
//     write DID succeed inside the transaction and is then discarded. This is
//     the half a statement fault cannot reach, and the only one that can tell a
//     transaction bed apart from an autocommit write.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorRegRig is a real channel sqlite plus the registry over it, keeping the
// raw *sql.DB so a test can inspect rows the registry's own reads hide
// (deregistered rows) and install fault triggers on the same single pooled
// connection the registry writes through.
type actorRegRig struct {
	t       *testing.T
	db      *sql.DB
	reg     *actorRegistry
	commits int
}

func newActorRegRig(t *testing.T) *actorRegRig {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "registry.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rig := &actorRegRig{t: t, db: db}
	// onCommit fires synchronously on the calling goroutine, so a plain counter
	// is race-free and is itself evidence: a rolled-back verb must not signal.
	rig.reg = newActorRegistry(db, "C-registry-test", func() { rig.commits++ })
	return rig
}

func (r *actorRegRig) exec(sqlText string) {
	r.t.Helper()
	if _, err := r.db.ExecContext(context.Background(), sqlText); err != nil {
		r.t.Fatalf("rig exec %q: %v", sqlText, err)
	}
}

// injectStatementFault aborts every verb ("INSERT" / "UPDATE") aimed at
// actor_registry at statement time. Returns the remover.
func (r *actorRegRig) injectStatementFault(verb string) func() {
	r.t.Helper()
	r.exec(fmt.Sprintf(`CREATE TRIGGER zz_stmt_fault BEFORE %s ON actor_registry
		BEGIN SELECT RAISE(ABORT, 'injected statement fault'); END`, verb))
	return func() { r.exec(`DROP TRIGGER zz_stmt_fault`) }
}

// injectCommitFault lets the verb's write land inside the transaction and makes
// the COMMIT itself fail: the AFTER trigger writes into a sink whose foreign key
// is unsatisfiable AND deferred, so SQLite defers the verdict to commit time.
func (r *actorRegRig) injectCommitFault(verb string) func() {
	r.t.Helper()
	r.exec(`CREATE TABLE zz_fault_parent (k TEXT PRIMARY KEY)`)
	r.exec(`CREATE TABLE zz_fault_sink (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		ref TEXT REFERENCES zz_fault_parent(k) DEFERRABLE INITIALLY DEFERRED)`)
	r.exec(fmt.Sprintf(`CREATE TRIGGER zz_commit_fault AFTER %s ON actor_registry
		BEGIN INSERT INTO zz_fault_sink(ref) VALUES ('no-such-parent'); END`, verb))
	return func() {
		r.exec(`DROP TRIGGER zz_commit_fault`)
		r.exec(`DROP TABLE zz_fault_sink`)
		r.exec(`DROP TABLE zz_fault_parent`)
	}
}

// rawRowCount counts EVERY actor_registry row, tombstoned ones included — the
// registry's own reads filter those out and would hide a leaked half-write.
func (r *actorRegRig) rawRowCount() int {
	r.t.Helper()
	var n int
	if err := r.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM actor_registry`).Scan(&n); err != nil {
		r.t.Fatalf("count actor_registry: %v", err)
	}
	return n
}

// rawRow returns (class, deregistered_at) for one row regardless of tombstone.
func (r *actorRegRig) rawRow(id actor.ActorID) (string, sql.NullInt64, bool) {
	r.t.Helper()
	var class string
	var dereg sql.NullInt64
	err := r.db.QueryRowContext(context.Background(),
		`SELECT class, deregistered_at FROM actor_registry WHERE actor_id=?`,
		string(id)).Scan(&class, &dereg)
	if errors.Is(err, sql.ErrNoRows) {
		return "", sql.NullInt64{}, false
	}
	if err != nil {
		r.t.Fatalf("raw row %q: %v", id, err)
	}
	return class, dereg, true
}

func agentDraft(declID, class string, createdAt int64) storespec.ActorDraft {
	return storespec.ActorDraft{
		Kind:         actor.KindAgent,
		SourceDeclID: declID,
		CreatedAt:    createdAt,
		Definition:   storespec.ActorDefinition{Class: class, Config: json.RawMessage(`{"v":1}`)},
		Placement:    storespec.NewServerPlacement(),
	}
}

func (r *actorRegRig) mustInsert(draft storespec.ActorDraft) storespec.ActorRecord {
	r.t.Helper()
	rec, err := r.reg.Insert(context.Background(), draft)
	if err != nil {
		r.t.Fatalf("Insert %+v: %v", draft, err)
	}
	return rec
}

// --- T51: Insert -------------------------------------------------------------

// A fault at the row write leaves the whole birth undone: no row, no minted id
// burned, no commit signal — and the identical retry then succeeds.
func TestActorRegistryInsert_StatementFaultLeavesNothing(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	remove := rig.injectStatementFault("INSERT")
	if _, err := rig.reg.Insert(ctx, agentDraft("decl-atomic", "worker", 1000)); err == nil {
		t.Fatal("Insert must fail while the row write aborts")
	}
	if n := rig.rawRowCount(); n != 0 {
		t.Fatalf("actor_registry rows after failed insert = %d, want 0 (nothing may land)", n)
	}
	if rig.commits != 0 {
		t.Fatalf("commit signal fired %d times for a rolled-back insert, want 0", rig.commits)
	}
	remove()

	rec := rig.mustInsert(agentDraft("decl-atomic", "worker", 1000))
	if rec.ID == "" {
		t.Fatal("retry after the fault must mint an id")
	}
	if n := rig.rawRowCount(); n != 1 {
		t.Fatalf("actor_registry rows after successful retry = %d, want exactly 1", n)
	}
	if rig.commits != 1 {
		t.Fatalf("commit signal count = %d, want 1", rig.commits)
	}
	got, found, err := rig.reg.LookupActive(ctx, rec.ID)
	if err != nil || !found {
		t.Fatalf("LookupActive(%q) = %v, found=%v, err=%v", rec.ID, got, found, err)
	}
}

// The row write succeeds inside the transaction and the COMMIT fails: the whole
// birth is still discarded. This is the half only a transaction bed can pass.
func TestActorRegistryInsert_CommitFaultDiscardsTheRow(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	remove := rig.injectCommitFault("INSERT")
	_, err := rig.reg.Insert(ctx, agentDraft("decl-atomic", "worker", 1000))
	if err == nil {
		t.Fatal("Insert must fail when its commit fails")
	}
	if !strings.Contains(err.Error(), "commit") {
		t.Fatalf("fault must land at commit (proving the row write already ran in-tx); err = %v", err)
	}
	if n := rig.rawRowCount(); n != 0 {
		t.Fatalf("actor_registry rows after failed commit = %d, want 0", n)
	}
	if rig.commits != 0 {
		t.Fatalf("commit signal fired %d times for a failed commit, want 0", rig.commits)
	}
	remove()

	rec := rig.mustInsert(agentDraft("decl-atomic", "worker", 1000))
	if n := rig.rawRowCount(); n != 1 {
		t.Fatalf("rows after retry = %d, want exactly 1", n)
	}
	if rec.Definition.Class != "worker" {
		t.Fatalf("retry record class = %q, want %q", rec.Definition.Class, "worker")
	}
}

func TestActorRegistryInsert_NonSingletonBirthsAreIndependent(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	first := rig.mustInsert(agentDraft("decl-replay", "worker", 1000))
	update := agentDraft("decl-replay", "other-class", 5000)
	update.Principal = "steward"
	update.Definition.Config = json.RawMessage(`{"v":2}`)
	update.Placement, _ = storespec.NewDaemonPlacement("device:one")
	replay, err := rig.reg.Insert(ctx, update)
	if err != nil {
		t.Fatalf("replay Insert: %v", err)
	}
	if replay.ID == first.ID {
		t.Fatalf("second birth reused %q", first.ID)
	}
	if replay.Principal != "steward" || replay.Definition.Class != "other-class" ||
		string(replay.Definition.Config) != `{"v":2}` || replay.Placement != update.Placement {
		t.Fatalf("replay did not return updated mutable fields: %+v", replay)
	}
	if replay.Kind != first.Kind || replay.SourceDeclID != first.SourceDeclID || replay.CreatedAt != update.CreatedAt {
		t.Fatalf("second birth fields: first=%+v replay=%+v", first, replay)
	}
	if n := rig.rawRowCount(); n != 2 {
		t.Fatalf("rows after second birth = %d, want 2", n)
	}
	stored, found, err := rig.reg.LookupActive(ctx, first.ID)
	if err != nil || !found || stored.Principal == "steward" || stored.Definition.Class != "worker" {
		t.Fatalf("first birth changed=%+v found=%v err=%v", stored, found, err)
	}
}

func TestActorRegistryInsert_NonHumanPrincipalNeverMergesBirths(t *testing.T) {
	rig := newActorRegRig(t)
	firstDraft := agentDraft("decl-principal-a", "worker", 1000)
	firstDraft.Principal = "shared-operator"
	secondDraft := agentDraft("decl-principal-b", "worker", 2000)
	secondDraft.Principal = "shared-operator"
	first := rig.mustInsert(firstDraft)
	second := rig.mustInsert(secondDraft)
	if second.ID == first.ID || rig.rawRowCount() != 2 {
		t.Fatalf("non-human births merged: first=%+v second=%+v rows=%d", first, second, rig.rawRowCount())
	}
	if first.SourceDeclID == second.SourceDeclID {
		t.Fatalf("fixture did not cover distinct declarations: first=%+v second=%+v", first, second)
	}
	stored, found, err := rig.reg.LookupActive(context.Background(), first.ID)
	if err != nil || !found || stored.CreatedAt != first.CreatedAt || stored.Principal != "shared-operator" {
		t.Fatalf("first birth changed: stored=%+v found=%v err=%v", stored, found, err)
	}
}

func TestActorRegistryInsert_EqualReplayPreservesShape(t *testing.T) {
	rig := newActorRegRig(t)
	draft := agentDraft("decl-equal", "worker", 1000)
	first := rig.mustInsert(draft)
	replay := rig.mustInsert(draft)
	if replay.ID == first.ID || replay.Kind != first.Kind || replay.Principal != first.Principal ||
		replay.SourceDeclID != first.SourceDeclID || !replay.Definition.Equal(first.Definition) || replay.Placement != first.Placement {
		t.Fatalf("equal draft did not mint an independent peer: first=%+v replay=%+v", first, replay)
	}
}

func TestActorRegistryInsert_NonSingletonSourceMayMintDifferentKinds(t *testing.T) {
	rig := newActorRegRig(t)
	first := rig.mustInsert(agentDraft("decl-kind", "worker", 1000))
	conflict := agentDraft("decl-kind", "worker", 2000)
	conflict.Kind = actor.KindTool
	second, err := rig.reg.Insert(context.Background(), conflict)
	if err != nil || second.Kind != actor.KindTool || second.ID == first.ID {
		t.Fatalf("second kind birth=%+v err=%v", second, err)
	}
	stored, found, err := rig.reg.LookupActive(context.Background(), first.ID)
	if err != nil || !found || stored.Kind != actor.KindAgent {
		t.Fatalf("kind conflict changed stored row: %+v found=%v err=%v", stored, found, err)
	}
}

func TestActorRegistryInsert_ThreeSegmentKindsAndReservedSingletons(t *testing.T) {
	rig := newActorRegRig(t)
	ctx := context.Background()

	systemDraft := agentDraft("registrar", "registrar", 1000)
	systemDraft.Kind = actor.KindSystem
	system := rig.mustInsert(systemDraft)
	if system.ID != "system:registrar:1000" {
		t.Fatalf("system id=%q", system.ID)
	}
	if _, err := rig.reg.Insert(ctx, systemDraft); !errors.Is(err, storespec.ErrConflictExists) {
		t.Fatalf("second system identity err=%v", err)
	}

	peerDraft := agentDraft("remote", "peeractor", 2000)
	peerDraft.Kind = actor.KindPeer
	peer := rig.mustInsert(peerDraft)
	if peer.ID != "peer:remote:2000" {
		t.Fatalf("peer id=%q", peer.ID)
	}
	if _, err := rig.reg.Insert(ctx, peerDraft); !errors.Is(err, storespec.ErrConflictExists) {
		t.Fatalf("second peer identity err=%v", err)
	}

	badSeed := agentDraft("bad:seed", "worker", 3000)
	if _, err := rig.reg.Insert(ctx, badSeed); err == nil || !strings.Contains(err.Error(), "seed") {
		t.Fatalf("colon seed err=%v", err)
	}
}

// --- T52: UpdateDefinition ---------------------------------------------------

func TestActorRegistryUpdateDefinition_StatementFaultLeavesDefinitionIntact(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)
	rec := rig.mustInsert(agentDraft("decl-update", "v1", 1000))
	before := rig.commits

	remove := rig.injectStatementFault("UPDATE")
	_, err := rig.reg.UpdateDefinition(ctx, rec.ID,
		storespec.ActorDefinition{Class: "v2", Config: json.RawMessage(`{"v":2}`)})
	if err == nil {
		t.Fatal("UpdateDefinition must fail while the row write aborts")
	}
	remove()

	got, found, err := rig.reg.LookupActive(ctx, rec.ID)
	if err != nil || !found {
		t.Fatalf("LookupActive after failed update: found=%v err=%v", found, err)
	}
	if got.Definition.Class != "v1" || string(got.Definition.Config) != `{"v":1}` {
		t.Fatalf("definition after failed update = %+v, want the untouched v1", got.Definition)
	}
	if rig.commits != before {
		t.Fatalf("commit signal fired for a rolled-back update (%d extra)", rig.commits-before)
	}
}

func TestActorRegistryUpdateDefinition_CommitFaultDiscardsTheRewrite(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)
	rec := rig.mustInsert(agentDraft("decl-update", "v1", 1000))
	before := rig.commits

	remove := rig.injectCommitFault("UPDATE")
	_, err := rig.reg.UpdateDefinition(ctx, rec.ID,
		storespec.ActorDefinition{Class: "v2", Config: json.RawMessage(`{"v":2}`)})
	if err == nil {
		t.Fatal("UpdateDefinition must fail when its commit fails")
	}
	if !strings.Contains(err.Error(), "commit") {
		t.Fatalf("fault must land at commit (proving the rewrite already ran in-tx); err = %v", err)
	}
	remove()

	class, _, ok := rig.rawRow(rec.ID)
	if !ok {
		t.Fatalf("row %q vanished after a failed update commit", rec.ID)
	}
	if class != "v1" {
		t.Fatalf("class after failed commit = %q, want the untouched %q", class, "v1")
	}
	if rig.commits != before {
		t.Fatalf("commit signal fired for a failed commit (%d extra)", rig.commits-before)
	}

	// And the verb still works once the fault is gone.
	updated, err := rig.reg.UpdateDefinition(ctx, rec.ID,
		storespec.ActorDefinition{Class: "v2", Config: json.RawMessage(`{"v":2}`)})
	if err != nil {
		t.Fatalf("UpdateDefinition after fault removal: %v", err)
	}
	if updated.Definition.Class != "v2" {
		t.Fatalf("updated class = %q, want v2", updated.Definition.Class)
	}
	if rig.commits != before+1 {
		t.Fatalf("commit signal count = %d, want %d", rig.commits, before+1)
	}
}

// The returned record is composed from the SAME transaction's read (never a
// read-back after commit): every non-definition field must be the committed
// row's, and the stored row must agree with what was returned.
func TestActorRegistryUpdateDefinition_ReturnsInTxRecord(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)
	seed := agentDraft("decl-compose", "v1", 1234)
	rec := rig.mustInsert(seed)

	updated, err := rig.reg.UpdateDefinition(ctx, rec.ID,
		storespec.ActorDefinition{Class: "v2", Config: json.RawMessage(`{"v":2}`)})
	if err != nil {
		t.Fatalf("UpdateDefinition: %v", err)
	}
	if updated.ID != rec.ID || updated.Kind != rec.Kind ||
		updated.SourceDeclID != rec.SourceDeclID || updated.CreatedAt != rec.CreatedAt ||
		updated.Placement != rec.Placement || updated.Principal != rec.Principal {
		t.Fatalf("non-definition fields changed: got %+v, want the row of %+v", updated, rec)
	}
	if updated.Definition.Class != "v2" || string(updated.Definition.Config) != `{"v":2}` {
		t.Fatalf("returned definition = %+v, want v2", updated.Definition)
	}
	stored, found, err := rig.reg.LookupActive(ctx, rec.ID)
	if err != nil || !found {
		t.Fatalf("LookupActive: found=%v err=%v", found, err)
	}
	if !stored.Definition.Equal(updated.Definition) {
		t.Fatalf("stored definition %+v disagrees with returned %+v",
			stored.Definition, updated.Definition)
	}
}
