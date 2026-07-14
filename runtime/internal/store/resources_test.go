package store

// White-box tests for the access-plane store: resourceRegistry (R + existence
// + create/delete outbox) and kvDriver (inline bytes). Both are unexported and
// reachable only from inside the package — the same confinement the
// message-log store relies on. They run over a real channel sqlite
// (ChannelLocalDDL), no fakes.

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// openResourceReg opens a fresh temp-dir channel sqlite with the full DDL
// (resources + resource_grants + resource_reservations + resource_tombstones
// present, foreign_keys=ON) and returns a registry over it, registering
// cleanup.
func openResourceReg(t *testing.T) *resourceRegistry {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "res.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newResourceRegistry(db)
}

func kvOf(r *resourceRegistry) *kvDriver { return newKVDriver(r.db) }

// createKV is the day-1 kv shape: no external placement (empty daemon/coord).
func createKV(t *testing.T, reg *resourceRegistry, id resource.ResourceID, creator actor.ActorID, initial []byte) {
	t.Helper()
	if err := reg.Create(context.Background(), id, resourcespec.KindKV, creator, "", "", initial); err != nil {
		t.Fatalf("Create %q: %v", id, err)
	}
}

// --- Create: atomicity (row + full grant + bytes) + collision sentinel -------

func TestResource_CreateWritesRowGrantBytes(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "kv:doc", resourcespec.KindKV, "actor:a", "", "", []byte("hello")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	meta, ok, err := reg.Resolve(ctx, "kv:doc")
	if err != nil || !ok {
		t.Fatalf("Resolve ok=%v err=%v", ok, err)
	}
	if meta.Kind != "kv" {
		t.Errorf("kind=%q want kv", meta.Kind)
	}
	if meta.CreatedAt <= 0 {
		t.Errorf("created_at=%d must be stamped positive", meta.CreatedAt)
	}

	// Creator holds the full object-rights grant (read/write/set/delete).
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete} {
		if !allowActor(t, reg, "actor:a", "kv:doc", op) {
			t.Errorf("creator must hold op=%s in its full grant", op)
		}
	}

	// Initial bytes are readable.
	val, found, err := kvOf(reg).Read(ctx, "kv:doc")
	if err != nil || !found {
		t.Fatalf("Read found=%v err=%v", found, err)
	}
	if string(val) != "hello" {
		t.Errorf("bytes=%q want hello", val)
	}
}

// KV rows carry no external route, while retaining their creator audit fact.
func TestResource_CreateKVRoutingAndAudit(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", []byte("v"))

	meta, ok, err := reg.Resolve(ctx, "kv:doc")
	if err != nil || !ok {
		t.Fatalf("Resolve ok=%v err=%v", ok, err)
	}
	if meta.PlacementDaemonID != "" {
		t.Errorf("kv PlacementDaemonID=%q want empty", meta.PlacementDaemonID)
	}
	if meta.PlacementCoord != "" {
		t.Errorf("kv PlacementCoord=%q want empty", meta.PlacementCoord)
	}
	if meta.CreatedBy != "actor:a" {
		t.Errorf("CreatedBy=%q want actor:a", meta.CreatedBy)
	}
}

// A file-kind row created via the direct immediate path persists the real
// placement daemon and coord supplied by the door.
func TestResource_CreateFilePersistsRoute(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-xyz", nil); err != nil {
		t.Fatalf("Create file: %v", err)
	}

	meta, ok, err := reg.Resolve(ctx, "file:doc")
	if err != nil || !ok {
		t.Fatalf("Resolve ok=%v err=%v", ok, err)
	}
	if meta.PlacementDaemonID != "daemon-1" {
		t.Errorf("PlacementDaemonID=%q want daemon-1", meta.PlacementDaemonID)
	}
	if meta.PlacementCoord != "coord-xyz" {
		t.Errorf("PlacementCoord=%q want coord-xyz", meta.PlacementCoord)
	}
	// file bytes never ride the inline `bytes` column — resolved-but-empty via
	// the kv driver's shape is NOT how file bytes are read (a file driver is a
	// later addition, §4), but the row's own bytes column must stay NULL.
	if val, found, err := kvOf(reg).Read(ctx, "file:doc"); err != nil || found || val != nil {
		t.Errorf("file row bytes column: found=%v val=%q err=%v, want found=false nil (bytes live at placement_coord, never inline)", found, val, err)
	}
}

func TestResource_CreateCollisionSentinel(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	createKV(t, reg, "kv:doc", "actor:a", []byte("v1"))
	err := reg.Create(ctx, "kv:doc", resourcespec.KindKV, "actor:b", "", "", []byte("v2"))
	if err == nil {
		t.Fatal("second Create on same id must collide")
	}
	if err.Error() != "resourcespec: resource already exists" {
		t.Errorf("collision err=%v want ErrAlreadyExists", err)
	}
	// The colliding create left the original untouched (no controller grab, no
	// byte overwrite).
	if val, _, _ := kvOf(reg).Read(ctx, "kv:doc"); string(val) != "v1" {
		t.Errorf("bytes after collision=%q want v1 (unchanged)", val)
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("colliding creator b must NOT have grabbed any grant")
	}
}

func TestResource_CreateGuardsEmptyInputs(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if err := reg.Create(ctx, "", resourcespec.KindKV, "actor:a", "", "", nil); err == nil {
		t.Error("Create with empty id must error")
	}
	if err := reg.Create(ctx, "kv:x", resourcespec.KindKV, "", "", "", nil); err == nil {
		t.Error("Create with empty creator must error")
	}
}

// --- ReserveCreate / CommitReservation: create-outbox (§1.7) -----------------

func TestResource_ReserveCreateAndCommitLandsRow(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	rid, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-1", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}
	if rid == "" {
		t.Fatal("ReserveCreate must return a non-empty reservation id")
	}

	// Not yet visible: the row lands only on Commit.
	if _, ok, _ := reg.Resolve(ctx, "file:doc"); ok {
		t.Fatal("resource must not be visible before CommitReservation")
	}

	found, err := reg.CommitReservation(ctx, rid)
	if err != nil {
		t.Fatalf("CommitReservation: %v", err)
	}
	if !found {
		t.Fatal("CommitReservation must report found=true for a live reservation")
	}

	meta, ok, err := reg.Resolve(ctx, "file:doc")
	if err != nil || !ok {
		t.Fatalf("Resolve after commit ok=%v err=%v", ok, err)
	}
	if meta.CreatedBy != "actor:a" {
		t.Errorf("CreatedBy=%q want actor:a (door-authenticated creator from the reservation, never daemon-reported)", meta.CreatedBy)
	}
	if meta.PlacementDaemonID != "daemon-1" || meta.PlacementCoord != "coord-1" {
		t.Errorf("placement = (%q,%q) want (daemon-1,coord-1)", meta.PlacementDaemonID, meta.PlacementCoord)
	}
	// Creator holds the full object-rights grant, same as the direct path.
	if !allowActor(t, reg, "actor:a", "file:doc", access.OpDelete) {
		t.Error("committed creator must hold the full-rights grant")
	}
}

// TestResource_ReserveCreateDirSurvivesOutbox pins 期11 丁12's byte-shape bit
// crossing the create-outbox: a content-less dir=true reservation must land a
// resources row whose Resolve reports Dir=true (the door's later Open reads
// this to route to an os.Root subtree lease, never a single-file staging
// handle). Daemon reports no truth (§1.3), so the shape has to ride the
// server-side reservation write-ahead, not be re-derived from disk.
func TestResource_ReserveCreateDirSurvivesOutbox(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	// Regular file: Dir stays false end-to-end.
	ridFile, err := reg.ReserveCreate(ctx, "file:blob", resourcespec.KindFile, "actor:a", "daemon-1", "coord-blob", false)
	if err != nil {
		t.Fatalf("ReserveCreate(file): %v", err)
	}
	if _, err := reg.CommitReservation(ctx, ridFile); err != nil {
		t.Fatalf("CommitReservation(file): %v", err)
	}
	if meta, ok, _ := reg.Resolve(ctx, "file:blob"); !ok || meta.Dir {
		t.Fatalf("regular file Resolve.Dir=%v want false (ok=%v)", meta.Dir, ok)
	}

	// Directory-shaped workspace: Dir must survive to the landed row.
	ridDir, err := reg.ReserveCreate(ctx, "file:ws", resourcespec.KindFile, "actor:a", "daemon-1", "coord-ws", true)
	if err != nil {
		t.Fatalf("ReserveCreate(dir): %v", err)
	}
	if _, err := reg.CommitReservation(ctx, ridDir); err != nil {
		t.Fatalf("CommitReservation(dir): %v", err)
	}
	meta, ok, err := reg.Resolve(ctx, "file:ws")
	if err != nil || !ok {
		t.Fatalf("Resolve(dir) ok=%v err=%v", ok, err)
	}
	if !meta.Dir {
		t.Fatal("dir workspace Resolve.Dir=false want true — byte-shape bit lost across the create-outbox")
	}
}

// A repeat Committed (replay) after the reservation already landed must be a
// clean no-op — level-triggered idempotency, §1.7.
func TestResource_CommitReservation_ReplayAfterLandingIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	rid, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-1", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}
	if _, err := reg.CommitReservation(ctx, rid); err != nil {
		t.Fatalf("first CommitReservation: %v", err)
	}

	found, err := reg.CommitReservation(ctx, rid)
	if err != nil {
		t.Fatalf("replay CommitReservation must not error: %v", err)
	}
	if found {
		t.Error("replay CommitReservation must report found=false (already consumed)")
	}
}

// A reservation_id that never existed is also a clean no-op.
func TestResource_CommitReservation_UnknownIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	found, err := reg.CommitReservation(ctx, "no-such-reservation")
	if err != nil {
		t.Fatalf("unknown reservation must not error: %v", err)
	}
	if found {
		t.Error("unknown reservation must report found=false")
	}
}

// Two reservations racing to land the SAME resource_id: the first Commit
// wins, the second gets ErrReservationLost and its own reservation row is
// still deleted (never left dangling), per §1.7's "并发败者".
func TestResource_CommitReservation_LosingRaceDeletesLoserReservation(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	rid1, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-1", false)
	if err != nil {
		t.Fatalf("ReserveCreate 1: %v", err)
	}
	rid2, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:b", "daemon-2", "coord-2", false)
	if err != nil {
		t.Fatalf("ReserveCreate 2: %v", err)
	}

	if found, err := reg.CommitReservation(ctx, rid1); err != nil || !found {
		t.Fatalf("winner CommitReservation found=%v err=%v", found, err)
	}

	found, err := reg.CommitReservation(ctx, rid2)
	if !found {
		t.Errorf("loser CommitReservation found=%v want true (the reservation DID exist)", found)
	}
	if err == nil {
		t.Fatal("loser CommitReservation must return ErrReservationLost")
	}
	if err != resourcespec.ErrReservationLost {
		t.Errorf("loser err=%v want ErrReservationLost", err)
	}

	// The winner's attributes must stand — the loser never overwrote them.
	meta, ok, _ := reg.Resolve(ctx, "file:doc")
	if !ok || meta.CreatedBy != "actor:a" || meta.PlacementDaemonID != "daemon-1" {
		t.Errorf("landed row = %+v, want the WINNER (actor:a/daemon-1) unperturbed by the loser", meta)
	}

	// The loser's reservation is gone (a second commit attempt on rid2 is a
	// clean no-op, not a repeat "lost" — proves it was actually deleted).
	found2, err2 := reg.CommitReservation(ctx, rid2)
	if err2 != nil {
		t.Fatalf("repeat commit of the already-deleted loser reservation must not error: %v", err2)
	}
	if found2 {
		t.Error("loser reservation must have been deleted on its first (losing) commit")
	}

	// Replaying the dead loser reservation ID must not resurrect or mutate
	// the already-landed resource — the winner's row must be byte-identical
	// to what it was before this no-op replay.
	metaAfter, ok2, _ := reg.Resolve(ctx, "file:doc")
	if !ok2 || metaAfter != meta {
		t.Errorf("landed row mutated by replaying dead loser reservation: before=%+v after=%+v", meta, metaAfter)
	}
}

// --- ReservationDaemon / ListReservationsByDaemon (§4.7 daemon control-RPC) --

func TestResource_ReservationDaemon(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	rid, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-1", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}

	daemonID, found, err := reg.ReservationDaemon(ctx, rid)
	if err != nil {
		t.Fatalf("ReservationDaemon: %v", err)
	}
	if !found || daemonID != "daemon-1" {
		t.Errorf("ReservationDaemon = (%q,%v), want (daemon-1,true)", daemonID, found)
	}

	// After commit the reservation is gone — ReservationDaemon reports a
	// clean not-found, never an error (the same replay-safety contract
	// CommitReservation itself draws).
	if _, err := reg.CommitReservation(ctx, rid); err != nil {
		t.Fatalf("CommitReservation: %v", err)
	}
	if _, found, err := reg.ReservationDaemon(ctx, rid); err != nil || found {
		t.Errorf("ReservationDaemon after commit: found=%v err=%v, want (false,nil)", found, err)
	}
}

func TestResource_ReservationDaemon_UnknownIsNotFound(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if _, found, err := reg.ReservationDaemon(ctx, "no-such-reservation"); err != nil || found {
		t.Errorf("unknown reservation: found=%v err=%v, want (false,nil)", found, err)
	}
}

// ListReservationsByDaemon must return ONLY the calling daemon's own rows
// (§4.7's read-side sender-auth discipline: "不泄他 daemon 的 coord 清单").
func TestResource_ListReservationsByDaemonFiltersPerDaemon(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if _, err := reg.ReserveCreate(ctx, "file:a", resourcespec.KindFile, "actor:a", "daemon-1", "coord-a", false); err != nil {
		t.Fatalf("ReserveCreate a: %v", err)
	}
	if _, err := reg.ReserveCreate(ctx, "file:b", resourcespec.KindFile, "actor:a", "daemon-1", "coord-b", false); err != nil {
		t.Fatalf("ReserveCreate b: %v", err)
	}
	if _, err := reg.ReserveCreate(ctx, "file:c", resourcespec.KindFile, "actor:a", "daemon-2", "coord-c", false); err != nil {
		t.Fatalf("ReserveCreate c: %v", err)
	}

	rows, err := reg.ListReservationsByDaemon(ctx, "daemon-1")
	if err != nil {
		t.Fatalf("ListReservationsByDaemon: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ListReservationsByDaemon(daemon-1) rows=%d want 2", len(rows))
	}
	for _, row := range rows {
		if row.PlacementDaemonID != "daemon-1" {
			t.Errorf("row %+v leaked into daemon-1's list", row)
		}
	}

	rows2, err := reg.ListReservationsByDaemon(ctx, "daemon-3-with-nothing-pending")
	if err != nil {
		t.Fatalf("ListReservationsByDaemon (empty): %v", err)
	}
	if len(rows2) != 0 {
		t.Errorf("ListReservationsByDaemon for an unrelated daemon = %d rows, want 0", len(rows2))
	}
}

// TestResource_SweepExpiredReservations (§1.7's third reservation-deletion
// trigger, S6's account): a reservation older than the cutoff is swept
// (deleted + returned); one younger is left untouched; a same-daemon-but-
// young row and an other-daemon row are both structurally confirmed
// unaffected (per-daemon confinement + age gate compose correctly).
func TestResource_SweepExpiredReservations(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	reg.nowMs = func() int64 { return 1000 }
	oldID, err := reg.ReserveCreate(ctx, "file:old", resourcespec.KindFile, "actor:a", "daemon-1", "coord-old", false)
	if err != nil {
		t.Fatalf("ReserveCreate old: %v", err)
	}
	oldOtherDaemonID, err := reg.ReserveCreate(ctx, "file:old-other", resourcespec.KindFile, "actor:a", "daemon-2", "coord-old-other", false)
	if err != nil {
		t.Fatalf("ReserveCreate old-other-daemon: %v", err)
	}

	reg.nowMs = func() int64 { return 5000 }
	freshID, err := reg.ReserveCreate(ctx, "file:fresh", resourcespec.KindFile, "actor:a", "daemon-1", "coord-fresh", false)
	if err != nil {
		t.Fatalf("ReserveCreate fresh: %v", err)
	}

	swept, err := reg.SweepExpiredReservations(ctx, "daemon-1", 3000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations: %v", err)
	}
	if len(swept) != 1 || swept[0].ReservationID != oldID {
		t.Fatalf("swept = %+v, want exactly [%s]", swept, oldID)
	}

	// old (daemon-1, before cutoff) is gone.
	if _, found, err := reg.ReservationDaemon(ctx, oldID); err != nil || found {
		t.Fatalf("old reservation still present: found=%v err=%v", found, err)
	}
	// fresh (daemon-1, after cutoff) survives.
	if _, found, err := reg.ReservationDaemon(ctx, freshID); err != nil || !found {
		t.Fatalf("fresh reservation swept: found=%v err=%v", found, err)
	}
	// old-other-daemon (before cutoff, but a DIFFERENT daemon) survives — the
	// sweep is per-daemon confined exactly like every other List*ByDaemon call.
	if _, found, err := reg.ReservationDaemon(ctx, oldOtherDaemonID); err != nil || !found {
		t.Fatalf("other-daemon reservation swept: found=%v err=%v", found, err)
	}

	// Re-sweeping is a clean no-op (idempotent, level-triggered like every
	// other outbox-completion method).
	swept2, err := reg.SweepExpiredReservations(ctx, "daemon-1", 3000)
	if err != nil {
		t.Fatalf("re-sweep: %v", err)
	}
	if len(swept2) != 0 {
		t.Fatalf("re-sweep swept = %+v, want none", swept2)
	}
}

// TestResource_TouchReservationsByCoordsSurvivesShortSweepCutoff (期11 S1's
// "本地慢写(注入短cutoff)不被sweep断" DoD, narrowed by 期11 review to coords
// rather than "any row this daemon owns"): a reservation born long before an
// injected short cutoff would normally be swept on reserved_at alone — but a
// TouchReservationsByCoords call NAMING its own coord (the daemon's own
// per-ReconcilePull activeCoords liveness bump, platform/storagehost.go's
// ReconcilePull handler) refreshes last_progress_at, and
// SweepExpiredReservations judges THAT column, not birth time. A same-daemon,
// same-age row whose coord is NOT in the touched set still sweeps on
// schedule — this is the exact regression this narrowing fixes: an abandoned
// reservation must not be kept alive merely because ITS DAEMON also owns some
// other, genuinely active, coord.
func TestResource_TouchReservationsByCoordsSurvivesShortSweepCutoff(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	reg.nowMs = func() int64 { return 1000 }
	activeID, err := reg.ReserveCreate(ctx, "file:active", resourcespec.KindFile, "actor:a", "daemon-1", "coord-active", false)
	if err != nil {
		t.Fatalf("ReserveCreate active: %v", err)
	}
	abandonedID, err := reg.ReserveCreate(ctx, "file:abandoned", resourcespec.KindFile, "actor:a", "daemon-1", "coord-abandoned", false)
	if err != nil {
		t.Fatalf("ReserveCreate abandoned: %v", err)
	}

	// The daemon polls (ReconcilePull) at t=9000 — long after birth (t=1000)
	// but well within a short cutoff window measured from NOW — naming ONLY
	// coord-active as its currently-open WriteHandle. coord-abandoned's
	// AllocRequest never got a write opened for it (or it already closed and
	// was never reopened), so it is deliberately absent from activeCoords.
	if err := reg.TouchReservationsByCoords(ctx, "daemon-1", []string{"coord-active"}, 9000); err != nil {
		t.Fatalf("TouchReservationsByCoords: %v", err)
	}

	// A short cutoff (10000-8000=2000) would sweep BOTH rows on reserved_at
	// alone (born at 1000, well before 2000) — S1's whole point is that it no
	// longer does for the touched one, and 期11 review's whole point is that
	// it STILL does for the same-daemon row whose coord was never named.
	swept, err := reg.SweepExpiredReservations(ctx, "daemon-1", 2000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations daemon-1: %v", err)
	}
	if len(swept) != 1 || swept[0].ReservationID != abandonedID {
		t.Fatalf("swept = %+v, want exactly [%s] (coord-active must survive, coord-abandoned must age out)", swept, abandonedID)
	}
	if _, found, err := reg.ReservationDaemon(ctx, activeID); err != nil || !found {
		t.Fatalf("active reservation swept despite being touched: found=%v err=%v", found, err)
	}
	if _, found, err := reg.ReservationDaemon(ctx, abandonedID); err != nil || found {
		t.Fatalf("abandoned reservation survived the sweep: found=%v err=%v, want gone", found, err)
	}
}

// TestResource_TouchReservationsByCoordsIsNoopForUnrelatedDaemon confirms the
// UPDATE is confined to daemonID's own rows even when another daemon's row
// happens to share a coord string — the per-daemon confinement every other
// List*ByDaemon/Sweep* method already carries, orthogonal to the new
// per-coord narrowing.
func TestResource_TouchReservationsByCoordsIsNoopForUnrelatedDaemon(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	reg.nowMs = func() int64 { return 1000 }
	otherID, err := reg.ReserveCreate(ctx, "file:other", resourcespec.KindFile, "actor:a", "daemon-2", "coord-other", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}

	if err := reg.TouchReservationsByCoords(ctx, "daemon-1", []string{"coord-other"}, 9000); err != nil {
		t.Fatalf("TouchReservationsByCoords: %v", err)
	}

	// daemon-2's row was never touched by a daemon-1-scoped call (even though
	// the coord string matches) — it still ages out on its birth time.
	swept, err := reg.SweepExpiredReservations(ctx, "daemon-2", 2000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations: %v", err)
	}
	if len(swept) != 1 || swept[0].ReservationID != otherID {
		t.Fatalf("swept = %+v, want exactly [%s]", swept, otherID)
	}
}

// TestResource_TouchReservationsByCoordsEmptyCoordsTouchesNothing is the
// abandoned-reservation regression this whole narrowing exists for (期11
// review): an empty coords list — a daemon that is online/polling but has
// NOTHING currently open — must touch ZERO rows, never fall back to "every
// reservation this daemon owns" (the pre-review bug: an always-online daemon
// suppressing age-sweep forever for a reservation whose write was never
// opened, or was opened and already closed).
func TestResource_TouchReservationsByCoordsEmptyCoordsTouchesNothing(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	reg.nowMs = func() int64 { return 1000 }
	abandonedID, err := reg.ReserveCreate(ctx, "file:abandoned", resourcespec.KindFile, "actor:a", "daemon-1", "coord-abandoned", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}

	// Repeated ReconcilePull cycles (sweep+touch), each with an empty
	// activeCoords — the daemon has nothing open at all.
	for i := 0; i < 3; i++ {
		if err := reg.TouchReservationsByCoords(ctx, "daemon-1", nil, 9000); err != nil {
			t.Fatalf("TouchReservationsByCoords: %v", err)
		}
	}

	swept, err := reg.SweepExpiredReservations(ctx, "daemon-1", 2000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations: %v", err)
	}
	if len(swept) != 1 || swept[0].ReservationID != abandonedID {
		t.Fatalf("swept = %+v, want exactly [%s] — an untouched reservation must age out even though its daemon keeps polling", swept, abandonedID)
	}
}

// --- TombstoneDaemon / ListTombstonesByDaemon (§4.7 daemon control-RPC) ------

func TestResource_TombstoneDaemon(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if err := reg.Create(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-xyz", nil); err != nil {
		t.Fatalf("Create file: %v", err)
	}
	if err := reg.Delete(ctx, "file:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	tombstoneID := onlyTombstoneID(t, reg.db, "file:doc")

	daemonID, found, err := reg.TombstoneDaemon(ctx, tombstoneID)
	if err != nil {
		t.Fatalf("TombstoneDaemon: %v", err)
	}
	if !found || daemonID != "daemon-1" {
		t.Errorf("TombstoneDaemon = (%q,%v), want (daemon-1,true)", daemonID, found)
	}

	if _, err := reg.ClearTombstone(ctx, tombstoneID); err != nil {
		t.Fatalf("ClearTombstone: %v", err)
	}
	if _, found, err := reg.TombstoneDaemon(ctx, tombstoneID); err != nil || found {
		t.Errorf("TombstoneDaemon after clear: found=%v err=%v, want (false,nil)", found, err)
	}
}

func TestResource_ListTombstonesByDaemonFiltersPerDaemon(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	for _, tc := range []struct{ id, daemon, coord string }{
		{"file:a", "daemon-1", "coord-a"},
		{"file:b", "daemon-1", "coord-b"},
		{"file:c", "daemon-2", "coord-c"},
	} {
		if err := reg.Create(ctx, resource.ResourceID(tc.id), resourcespec.KindFile, "actor:a", tc.daemon, tc.coord, nil); err != nil {
			t.Fatalf("Create %s: %v", tc.id, err)
		}
		if err := reg.Delete(ctx, resource.ResourceID(tc.id)); err != nil {
			t.Fatalf("Delete %s: %v", tc.id, err)
		}
	}

	rows, err := reg.ListTombstonesByDaemon(ctx, "daemon-1")
	if err != nil {
		t.Fatalf("ListTombstonesByDaemon: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ListTombstonesByDaemon(daemon-1) rows=%d want 2", len(rows))
	}
	for _, row := range rows {
		if row.DaemonID != "daemon-1" {
			t.Errorf("row %+v leaked into daemon-1's list", row)
		}
	}
}

// --- ListByPlacementDaemon (§4.7 ReconcilePull "应有资源清单") ----------------

func TestResource_ListByPlacementDaemonFiltersPerDaemonAndExcludesKV(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "file:a", resourcespec.KindFile, "actor:a", "daemon-1", "coord-a", nil); err != nil {
		t.Fatalf("Create file a: %v", err)
	}
	if err := reg.Create(ctx, "file:b", resourcespec.KindFile, "actor:a", "daemon-2", "coord-b", nil); err != nil {
		t.Fatalf("Create file b: %v", err)
	}
	createKV(t, reg, "kv:c", "actor:a", []byte("v"))

	rows, err := reg.ListByPlacementDaemon(ctx, "daemon-1")
	if err != nil {
		t.Fatalf("ListByPlacementDaemon: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "file:a" {
		t.Fatalf("ListByPlacementDaemon(daemon-1) = %+v, want exactly [file:a]", rows)
	}
	if len(rows[0].Grants) != 1 {
		t.Errorf("ListByPlacementDaemon row grants = %d, want the creator's full-rights grant", len(rows[0].Grants))
	}
}

// --- SetGrant: replace + revoke, and the two query halves --------------------

func TestResource_SetGrantReplaceAndRevoke(t *testing.T) {
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", nil)

	// Grant B read only.
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))
	if !allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B must have read after set")
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpWrite) {
		t.Error("B must NOT have write (grant was read-only)")
	}

	// Replace (not merge) with read+write.
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead, access.OpWrite))
	if !allowActor(t, reg, "actor:b", "kv:doc", access.OpWrite) {
		t.Error("B must have write after replacing grant")
	}

	// Revoke: empty ops deletes the entry.
	set(t, reg, "kv:doc", grantActor("actor:b"))
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B read must be gone after revoke (∅ ops)")
	}
}

func TestResource_MembersEntryQuerySplit(t *testing.T) {
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", nil)

	// A members entry (grantee empty) grants read.
	set(t, reg, "kv:doc", grantMembers(access.OpRead))

	// MembersAllow sees it; it does NOT look at any caller.
	if !allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("MembersAllow(read) must be true")
	}
	if allowMembers(t, reg, "kv:doc", access.OpWrite) {
		t.Error("MembersAllow(write) must be false (only read granted)")
	}

	// The actor-entry half is independent: B has no direct entry.
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("ActorAllows must NOT see the members entry (two separate halves)")
	}

	// Revoke the members entry.
	set(t, reg, "kv:doc", grantMembers())
	if allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("members entry must be gone after ∅ revoke")
	}
}

// --- Delete: cascades row + all grants; file additionally tombstones --------

func TestResource_DeleteCascadesRowAndGrants(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", nil)
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))
	set(t, reg, "kv:doc", grantMembers(access.OpRead))

	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, _ := reg.Resolve(ctx, "kv:doc"); ok {
		t.Error("resource must not resolve after delete")
	}
	// All grants gone (creator, B, members).
	if allowActor(t, reg, "actor:a", "kv:doc", access.OpRead) {
		t.Error("creator grant must be cascaded away by Delete")
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B grant must be cascaded away by Delete")
	}
	if allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("members grant must be cascaded away by Delete")
	}

	// Delete is idempotent: deleting an absent id is a clean no-op.
	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Errorf("repeat Delete must be a no-op, got %v", err)
	}
}

// kv delete leaves no tombstone (bytes live inline, "no tombstone" per §1.8).
func TestResource_DeleteKVWritesNoTombstone(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", []byte("v"))
	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	n := countRows(t, reg.db, "resource_tombstones")
	if n != 0 {
		t.Errorf("resource_tombstones rows=%d want 0 after a kv delete", n)
	}
}

// file delete is row-first-bytes-last + tombstone (§1.8): the row/grants are
// gone but a tombstone row carries daemon_id/coord/kind for the
// (later) Reclaimer, and ClearTombstone removes it once collection is
// confirmed.
func TestResource_DeleteFileWritesTombstoneThenClearTombstone(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if err := reg.Create(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-xyz", nil); err != nil {
		t.Fatalf("Create file: %v", err)
	}

	if err := reg.Delete(ctx, "file:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Row + grants gone (same non-lossy cascade as kv).
	if _, ok, _ := reg.Resolve(ctx, "file:doc"); ok {
		t.Error("file resource must not resolve after delete")
	}

	tombstoneID := onlyTombstoneID(t, reg.db, "file:doc")
	daemonID, coord, kind := readTombstone(t, reg.db, tombstoneID)
	if daemonID != "daemon-1" || coord != "coord-xyz" {
		t.Errorf("tombstone placement = (%q,%q) want (daemon-1,coord-xyz)", daemonID, coord)
	}
	if kind != string(resourcespec.KindFile) {
		t.Errorf("tombstone kind=%q want file", kind)
	}

	found, err := reg.ClearTombstone(ctx, tombstoneID)
	if err != nil {
		t.Fatalf("ClearTombstone: %v", err)
	}
	if !found {
		t.Error("ClearTombstone must report found=true for a live tombstone")
	}
	if n := countRows(t, reg.db, "resource_tombstones"); n != 0 {
		t.Errorf("resource_tombstones rows=%d want 0 after ClearTombstone", n)
	}
}

// ClearTombstone on an unknown id is a clean no-op (replay-safe).
func TestResource_ClearTombstone_UnknownIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	found, err := reg.ClearTombstone(ctx, "no-such-tombstone")
	if err != nil {
		t.Fatalf("unknown tombstone must not error: %v", err)
	}
	if found {
		t.Error("unknown tombstone must report found=false")
	}
}

// Same-name delete/recreate must not collide on the tombstone table's primary
// key — resource_id carries a NON-unique index precisely so multiple
// tombstones for the same id can co-exist (§1.3).
func TestResource_DeleteRecreateDeleteLeavesTwoTombstones(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-1", nil); err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	if err := reg.Delete(ctx, "file:doc"); err != nil {
		t.Fatalf("Delete 1: %v", err)
	}
	if err := reg.Create(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-2", nil); err != nil {
		t.Fatalf("Create 2 (recreate): %v", err)
	}
	if err := reg.Delete(ctx, "file:doc"); err != nil {
		t.Fatalf("Delete 2: %v", err)
	}

	if n := countRows(t, reg.db, "resource_tombstones"); n != 2 {
		t.Errorf("resource_tombstones rows=%d want 2 (delete/recreate/delete must not collide)", n)
	}
}

// --- List: raw range scan, prefix filter, pagination, grant projection ------

func TestResource_ListOrdersByCreatedAtThenID(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	// Pin the clock so all three land on the SAME created_at: order must fall
	// back to resource_id (a wall-clock nowMs() would only collide by luck —
	// and does NOT under -race's slower scheduling — so the fixed-tick clock
	// is load-bearing for this assertion, not incidental).
	reg.nowMs = func() int64 { return 1000 }
	createKV(t, reg, "kv:b", "actor:a", nil)
	createKV(t, reg, "kv:a", "actor:a", nil)
	createKV(t, reg, "kv:c", "actor:a", nil)

	rows, next, err := reg.List(ctx, "", 50, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if next != "" {
		t.Errorf("next cursor=%q want empty (fewer rows than limit)", next)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3", len(rows))
	}
	for i, want := range []resource.ResourceID{"kv:a", "kv:b", "kv:c"} {
		if rows[i].ID != want {
			t.Errorf("rows[%d].ID=%q want %q", i, rows[i].ID, want)
		}
	}
}

func TestResource_ListPrefixFilters(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc:1", "actor:a", nil)
	createKV(t, reg, "kv:doc:2", "actor:a", nil)
	createKV(t, reg, "kv:other", "actor:a", nil)

	rows, _, err := reg.List(ctx, "kv:doc:", 50, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2, got %+v", len(rows), rows)
	}
	for _, row := range rows {
		if row.ID != "kv:doc:1" && row.ID != "kv:doc:2" {
			t.Errorf("unexpected row id %q under prefix kv:doc:", row.ID)
		}
	}
}

// A raw '%' in an id must not act as a glob — likePrefix escapes it.
func TestResource_ListPrefixEscapesWildcards(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:100%off", "actor:a", nil)
	createKV(t, reg, "kv:100Xoff", "actor:a", nil)

	rows, _, err := reg.List(ctx, "kv:100%", 50, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "kv:100%off" {
		t.Fatalf("rows=%+v want exactly [kv:100%%off] (literal %% must not glob)", rows)
	}
}

// limit bounds the SCAN; a full page yields a non-empty cursor that resumes
// exactly where the previous page left off, covering the whole set exactly
// once with '>' strict comparison (no repeats, no gaps).
func TestResource_ListPaginatesByCursor(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	ids := []resource.ResourceID{"kv:a", "kv:b", "kv:c", "kv:d", "kv:e"}
	for _, id := range ids {
		createKV(t, reg, id, "actor:a", nil)
	}

	var seen []resource.ResourceID
	cursor := ""
	for i := 0; i < 10; i++ { // bounded loop guard against an infinite-pagination bug
		rows, next, err := reg.List(ctx, "", 2, cursor)
		if err != nil {
			t.Fatalf("List page %d: %v", i, err)
		}
		for _, row := range rows {
			seen = append(seen, row.ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}

	if len(seen) != len(ids) {
		t.Fatalf("seen=%v want all of %v", seen, ids)
	}
	for i, want := range ids {
		if seen[i] != want {
			t.Errorf("seen[%d]=%q want %q (order/no-repeat/no-gap)", i, seen[i], want)
		}
	}
}

func TestResource_ListMalformedCursorIsGoError(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if _, _, err := reg.List(ctx, "", 50, "not-a-valid-cursor!!"); err == nil {
		t.Fatal("malformed cursor must be a Go error")
	}
}

func TestResource_ListNonPositiveLimitErrors(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if _, _, err := reg.List(ctx, "", 0, ""); err == nil {
		t.Error("limit=0 must error")
	}
	if _, _, err := reg.List(ctx, "", -1, ""); err == nil {
		t.Error("negative limit must error")
	}
}

// List does NOT grant-filter — it returns the FULL grant projection per row,
// including entries the door would later filter away for a given caller.
func TestResource_ListReturnsFullGrantProjection(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", nil)
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))
	set(t, reg, "kv:doc", grantMembers(access.OpRead))

	rows, _, err := reg.List(ctx, "", 50, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if len(rows[0].Grants) != 3 { // creator full grant + actor:b read + members read
		t.Fatalf("grants=%+v want 3 entries (creator, actor:b, members)", rows[0].Grants)
	}
}

// --- kvDriver: found semantics (empty bytes vs no row), write, no-op delete ---

func TestKVDriver_ReadFoundSemantics(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)

	// Row exists with NULL bytes (nil initial) → resolved-but-empty: found=false.
	createKV(t, reg, "kv:empty", "actor:a", nil)
	if _, ok, _ := reg.Resolve(ctx, "kv:empty"); !ok {
		t.Fatal("empty-bytes resource must still RESOLVE (the row exists)")
	}
	val, found, err := kv.Read(ctx, "kv:empty")
	if err != nil {
		t.Fatalf("Read empty: %v", err)
	}
	if found || val != nil {
		t.Errorf("NULL-bytes row: found=%v val=%q want found=false empty", found, val)
	}

	// No row at all → also found=false, but Resolve reports it as not-existing
	// (that is where the door draws not_found vs resolved-but-empty).
	if _, ok, _ := reg.Resolve(ctx, "kv:ghost"); ok {
		t.Error("absent id must not resolve")
	}
	if _, found, _ := kv.Read(ctx, "kv:ghost"); found {
		t.Error("absent id Read must be found=false")
	}

	// Row with content → found=true.
	createKV(t, reg, "kv:full", "actor:a", []byte("payload"))
	val, found, _ = kv.Read(ctx, "kv:full")
	if !found || string(val) != "payload" {
		t.Errorf("content row: found=%v val=%q want true/payload", found, val)
	}

	// Present-but-empty ([]byte{}, proto: legal and distinct from nil) must read
	// back found=true with empty value — NOT collapse into the NULL case.
	createKV(t, reg, "kv:blank", "actor:a", []byte{})
	val, found, err = kv.Read(ctx, "kv:blank")
	if err != nil {
		t.Fatalf("Read blank: %v", err)
	}
	if !found || len(val) != 0 {
		t.Errorf("empty-blob row: found=%v val=%q want found=true empty value", found, val)
	}
}

// Write against a row that vanished in the resolve→execute window must surface
// an error (→ driver_error verdict at the door), never a silent zero-row success.
func TestKVDriver_WriteVanishedRow(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	if err := kv.Write(ctx, "kv:gone", []byte("x")); err == nil {
		t.Fatal("Write on absent row must error, not silently succeed")
	}
}

func TestKVDriver_WriteOverwrites(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	createKV(t, reg, "kv:doc", "actor:a", nil) // created with nil bytes

	if err := kv.Write(ctx, "kv:doc", []byte("v1")); err != nil {
		t.Fatalf("Write v1: %v", err)
	}
	if val, found, _ := kv.Read(ctx, "kv:doc"); !found || string(val) != "v1" {
		t.Errorf("after write: found=%v val=%q want true/v1", found, val)
	}
	// PUT overwrites (idempotent-on-content).
	if err := kv.Write(ctx, "kv:doc", []byte("v2")); err != nil {
		t.Fatalf("Write v2: %v", err)
	}
	if val, _, _ := kv.Read(ctx, "kv:doc"); string(val) != "v2" {
		t.Errorf("after overwrite: val=%q want v2", val)
	}
}

// kvDriver.Delete is a no-op: the bytes live inline, so the driver leaves the
// row and its bytes intact — it is Registry.Delete that removes them.
func TestKVDriver_DeleteIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	if err := reg.Create(ctx, "kv:doc", resourcespec.KindKV, "actor:a", "", "", []byte("keep")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := kv.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("driver Delete: %v", err)
	}
	// Row + bytes untouched by the driver-level delete.
	if _, ok, _ := reg.Resolve(ctx, "kv:doc"); !ok {
		t.Error("kvDriver.Delete must NOT remove the row (no-op)")
	}
	if val, found, _ := kv.Read(ctx, "kv:doc"); !found || string(val) != "keep" {
		t.Errorf("kvDriver.Delete must leave bytes intact, got found=%v val=%q", found, val)
	}
}

// --- test helpers ------------------------------------------------------------

func mustCreate(t *testing.T, reg *resourceRegistry, id resource.ResourceID, creator actor.ActorID) {
	t.Helper()
	createKV(t, reg, id, creator, nil)
}

func set(t *testing.T, reg *resourceRegistry, id resource.ResourceID, g access.Grant) {
	t.Helper()
	if err := reg.SetGrant(context.Background(), id, g); err != nil {
		t.Fatalf("SetGrant %q %+v: %v", id, g, err)
	}
}

func grantActor(who actor.ActorID, ops ...access.Operation) access.Grant {
	return access.Grant{GranteeKind: access.GranteeActor, Grantee: who, Ops: ops}
}

func grantMembers(ops ...access.Operation) access.Grant {
	return access.Grant{GranteeKind: access.GranteeMembers, Ops: ops}
}

func allowActor(t *testing.T, reg *resourceRegistry, caller actor.ActorID, id resource.ResourceID, op access.Operation) bool {
	t.Helper()
	ok, err := reg.ActorAllows(context.Background(), caller, id, op)
	if err != nil {
		t.Fatalf("ActorAllows(%q,%q,%s): %v", caller, id, op, err)
	}
	return ok
}

func allowMembers(t *testing.T, reg *resourceRegistry, id resource.ResourceID, op access.Operation) bool {
	t.Helper()
	ok, err := reg.MembersAllow(context.Background(), id, op)
	if err != nil {
		t.Fatalf("MembersAllow(%q,%s): %v", id, op, err)
	}
	return ok
}

// countRows is a raw row-count probe over the given table — used only to
// assert outbox table population/depopulation (resource_reservations /
// resource_tombstones), which resourcespec.Registry deliberately exposes no
// enumeration method for (they are internal accounting, not caller-facing).
func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// onlyTombstoneID asserts exactly one tombstone row exists for resourceID and
// returns its tombstone_id.
func onlyTombstoneID(t *testing.T, db *sql.DB, resourceID resource.ResourceID) string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT tombstone_id FROM resource_tombstones WHERE resource_id=?`, string(resourceID))
	if err != nil {
		t.Fatalf("query tombstones for %q: %v", resourceID, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan tombstone id: %v", err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 {
		t.Fatalf("tombstones for %q = %v, want exactly 1", resourceID, ids)
	}
	return ids[0]
}

// readTombstone reads back one tombstone row's daemon_id/placement_coord/kind.
func readTombstone(t *testing.T, db *sql.DB, tombstoneID string) (daemonID, coord, kind string) {
	t.Helper()
	err := db.QueryRowContext(context.Background(),
		`SELECT daemon_id, placement_coord, kind FROM resource_tombstones WHERE tombstone_id=?`,
		tombstoneID,
	).Scan(&daemonID, &coord, &kind)
	if err != nil {
		t.Fatalf("read tombstone %q: %v", tombstoneID, err)
	}
	return daemonID, coord, kind
}

// Every stale reservation is sweepable; there is no landed phase immunity.
func TestResource_SweepHasNoLandedPhaseImmunity(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	reg.nowMs = func() int64 { return 1000 }
	_, err := reg.ReserveCreate(ctx, "file:landed", resourcespec.KindFile, "actor:a", "daemon-1", "coord-landed", false)
	if err != nil {
		t.Fatalf("ReserveCreate landed: %v", err)
	}
	reservedID, err := reg.ReserveCreate(ctx, "file:reserved", resourcespec.KindFile, "actor:a", "daemon-1", "coord-reserved", false)
	if err != nil {
		t.Fatalf("ReserveCreate reserved: %v", err)
	}

	// A cutoff of 3000 is far past both rows' birth (t=1000).
	swept, err := reg.SweepExpiredReservations(ctx, "daemon-1", 3000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations: %v", err)
	}
	if len(swept) != 2 {
		t.Fatalf("swept = %+v, want both stale reservations", swept)
	}
	if _, found, err := reg.ReservationDaemon(ctx, reservedID); err != nil || found {
		t.Fatalf("reserved reservation survived the sweep: found=%v err=%v", found, err)
	}
}

func TestResource_StaleReservationSweep(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	reg.nowMs = func() int64 { return 1000 }
	id, err := reg.ReserveCreate(ctx, "file:x", resourcespec.KindFile, "actor:a", "daemon-1", "coord-x", false)
	if err != nil {
		t.Fatalf("ReserveCreate: %v", err)
	}
	swept, err := reg.SweepExpiredReservations(ctx, "daemon-1", 3000)
	if err != nil {
		t.Fatalf("SweepExpiredReservations: %v", err)
	}
	if len(swept) != 1 || swept[0].ReservationID != id {
		t.Fatalf("swept = %+v, want [%s] — an empty mark-landed must not have flipped anything", swept, id)
	}
}

// TestResource_DeleteSupersedesPendingReservations is 期11 review §2.5 #C: a
// Delete on a resource_id kills any still-pending reservation for the SAME id
// (a held-open straggler), writes a reclaim tombstone for its coord, and
// thereby prevents the straggler's later Committed from resurrecting the id
// with no fresh authorization — CommitReservation then finds no row and
// no-ops.
func TestResource_DeleteSupersedesPendingReservations(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	// The winner reserves + lands "file:doc" (coord-win).
	winID, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:a", "daemon-1", "coord-win", false)
	if err != nil {
		t.Fatalf("ReserveCreate win: %v", err)
	}
	if _, err := reg.CommitReservation(ctx, winID); err != nil {
		t.Fatalf("CommitReservation win: %v", err)
	}
	// A straggler had ALSO reserved the same id (before the winner landed) and
	// is still holding its write open — coord-strag.
	stragID, err := reg.ReserveCreate(ctx, "file:doc", resourcespec.KindFile, "actor:b", "daemon-1", "coord-strag", false)
	if err != nil {
		t.Fatalf("ReserveCreate straggler: %v", err)
	}

	// Delete the resource. This must supersede the straggler reservation.
	if err := reg.Delete(ctx, "file:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The straggler reservation is gone.
	if _, found, err := reg.ReservationDaemon(ctx, stragID); err != nil || found {
		t.Fatalf("straggler reservation survived Delete (would resurrect the id): found=%v err=%v", found, err)
	}
	// Two tombstones exist on daemon-1: the resource's own (coord-win) AND the
	// superseded straggler's (coord-strag) — both routed to the ordinary
	// ReconcilePull Reclaimer.
	tombs, err := reg.ListTombstonesByDaemon(ctx, "daemon-1")
	if err != nil {
		t.Fatalf("ListTombstonesByDaemon: %v", err)
	}
	coords := map[string]bool{}
	for _, ts := range tombs {
		coords[ts.PlacementCoord] = true
	}
	if !coords["coord-win"] || !coords["coord-strag"] {
		t.Fatalf("tombstone coords = %v, want both coord-win and coord-strag", coords)
	}
	// The straggler's later Committed lands NOTHING — no resurrection.
	found, err := reg.CommitReservation(ctx, stragID)
	if err != nil {
		t.Fatalf("CommitReservation straggler after supersede: %v", err)
	}
	if found {
		t.Fatal("straggler CommitReservation returned found=true — the id was resurrected (#C bug)")
	}
	if _, exists, err := reg.Resolve(ctx, "file:doc"); err != nil || exists {
		t.Fatalf("file:doc exists again after straggler commit: exists=%v err=%v", exists, err)
	}
}
