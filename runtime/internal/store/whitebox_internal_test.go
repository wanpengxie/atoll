package store

// In-package (white-box) tests for the branches that the segregated public
// interfaces cannot reach: the unexported appendTx guards, the membership
// transition tx helpers under injected DB faults, the classifyAppendErr
// mapping, and the request-lookup wrapper success path. All use either the
// relaxed-DDL poison DB (corruption / forward-version surface) or a directly
// constructed tx — both are reachable only from inside the package, which is
// exactly the structural confinement the store relies on.

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// --- appendTx guards (unexported; Append validates before reaching here, so
// these defenses are only reachable by calling appendTx directly) -------------

func TestAppendTx_NilTx(t *testing.T) {
	if _, err := appendTx(context.Background(), nil,
		&message.Envelope{ID: "x", Payload: []byte("{}")}, false); err == nil {
		t.Error("appendTx with nil tx must error")
	}
}

func TestAppendTx_GuardsEnvelope(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	beginTx := func() *sql.Tx {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		return tx
	}

	tx := beginTx()
	if _, err := appendTx(ctx, tx, nil, false); err == nil {
		t.Error("appendTx with nil envelope must error")
	}
	_ = tx.Rollback()

	tx = beginTx()
	if _, err := appendTx(ctx, tx, &message.Envelope{ID: "", Payload: []byte("{}")}, false); err == nil {
		t.Error("appendTx with empty id must error")
	}
	_ = tx.Rollback()

	tx = beginTx()
	if _, err := appendTx(ctx, tx, &message.Envelope{ID: "x", Payload: nil}, false); err == nil {
		t.Error("appendTx with nil payload must error")
	}
	_ = tx.Rollback()
}

// appendTx INSERT failure that is NOT a UNIQUE violation flows through
// classifyAppendErr's default arm (a generic wrapped error, not an
// *AppendError). We trigger it by inserting into a relaxed schema whose
// messages table is missing a column the INSERT references.
func TestAppendTx_GenericInsertError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "narrow.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// A messages table missing the `audience` (and most) columns: the INSERT
	// will fail with a non-UNIQUE "no column named ..." error.
	if _, err := db.ExecContext(ctx, `CREATE TABLE messages (
	   seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("narrow DDL: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = appendTx(ctx, tx, &message.Envelope{
		ID:      "x",
		Payload: []byte("{}"),
		Sender:  message.Sender{Kind: actor.KindHuman, ID: "a"},
		Kind:    message.KindEvent,
	}, false)
	if err == nil {
		t.Fatal("appendTx into a narrow table must error")
	}
	var ae *storespec.AppendError
	if errors.As(err, &ae) {
		t.Errorf("generic insert error must NOT be a typed *AppendError, got %v", ae)
	}
}

// --- classifyAppendErr direct mapping ----------------------------------------

func TestClassifyAppendErr_NilIsNil(t *testing.T) {
	if got := classifyAppendErr(nil, "x"); got != nil {
		t.Errorf("classifyAppendErr(nil) = %v want nil", got)
	}
}

func TestClassifyAppendErr_TerminalDuplicate(t *testing.T) {
	err := classifyAppendErr(errors.New("UNIQUE constraint failed: index 'ux_terminal_response_per_request'"), "m1")
	var ae *storespec.AppendError
	if !errors.As(err, &ae) {
		t.Fatalf("want *AppendError, got %T", err)
	}
	if ae.Reason != "harness_terminal_duplicate" {
		t.Errorf("reason=%q want harness_terminal_duplicate", ae.Reason)
	}
	if ae.PartialMessageID != "m1" {
		t.Errorf("partial id=%q want m1", ae.PartialMessageID)
	}
}

func TestClassifyAppendErr_Generic(t *testing.T) {
	err := classifyAppendErr(errors.New("disk full"), "m1")
	var ae *storespec.AppendError
	if errors.As(err, &ae) {
		t.Errorf("non-constraint error must not be *AppendError")
	}
}

// --- scanEnvelopeFrom: audience JSON corruption ------------------------------

// A row whose audience column holds non-JSON surfaces as a scan error, never a
// silent empty audience. Reachable only via the relaxed DB (the write path
// always marshals valid JSON).
func TestScanEnvelope_CorruptAudience(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id, kind, type, payload, visibility, audience, is_terminal)
		 VALUES ('m1', 1, 1, 'C', 'human', 's', 'event', 't', '{}', 'public', 'not-json', 0)`); err != nil {
		t.Fatalf("inject corrupt audience: %v", err)
	}
	m := newMessages(db, nil)
	if _, _, err := m.FindByID(ctx, "m1"); err == nil {
		t.Error("FindByID must error on corrupt audience JSON")
	}
}

// --- ListActive: scan / poison-kind on the multi-row path --------------------

// ListActive over a poison actor_kind surfaces the closed-set guard error on
// the rows path (distinct from Lookup's single-row guard).
func TestListActive_PoisonKindOnRowsPath(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('x', 'wizard', NULL, 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, err := reg.ListActive(ctx); err == nil {
		t.Error("ListActive must error on out-of-closed-set kind (rows path)")
	}
}

// Binding read path is symmetric with kind: a non-empty out-of-closed-set
// actor_binding column is a poisoned row and must fail loudly (ParseBinding),
// not silently raw-cast into the Record. Both read paths (ListActive rows /
// Lookup single-row) enforce it.
func TestListActive_PoisonBindingOnRowsPath(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('x', 'agent', 'teleport', 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, err := reg.ListActive(ctx); err == nil {
		t.Error("ListActive must error on out-of-closed-set binding (rows path)")
	}
}

func TestLookup_PoisonBinding(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('x', 'agent', 'teleport', 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, _, err := reg.Lookup(ctx, "x"); err == nil {
		t.Error("Lookup must error on out-of-closed-set binding (single-row path)")
	}
}

// Empty binding is a legitimate state (a presence-less member — e.g. a human —
// carries no binding), so a NULL/empty actor_binding must read back cleanly as
// "" — the validation rejects only NON-empty out-of-set values.
func TestLookup_EmptyBindingAccepted(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('h', 'human', NULL, 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	rec, ok, err := reg.Lookup(ctx, "h")
	if err != nil || !ok {
		t.Fatalf("Lookup empty-binding member: ok=%v err=%v", ok, err)
	}
	if rec.Binding != "" {
		t.Errorf("empty binding must read back as \"\", got %q", rec.Binding)
	}
}

// ListActive raw rows.Scan error: an active row whose created_at column holds
// non-integer text cannot scan into the int64 Record.CreatedAt, hitting the
// scan-error arm (distinct from the closed-set kind guard). Reachable only via
// a relaxed DB whose created_at is typeless TEXT.
func TestListActive_RawScanError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "txtcreated.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// created_at typed TEXT so a non-numeric value survives insert, then fails
	// the int64 scan in ListActive.
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, actor_binding TEXT,
	   created_at TEXT, deregistered_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('a', 'agent', NULL, 'not-a-number', NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, err := reg.ListActive(ctx); err == nil {
		t.Error("ListActive must surface the raw rows.Scan error on a non-integer created_at")
	}
}

// ReadAfterSeq over a poison message row surfaces the per-row scan-error wrap
// (scanEnvelopeRows fails the closed-set guard) — the rows-loop error arm.
func TestReadAfterSeq_PoisonRowScanError(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	insertRawMessage(t, db, "m1", "wizard", "event", "public") // poison sender kind
	m := newMessages(db, nil)
	if _, err := m.ReadAfterSeq(ctx, 0, 100); err == nil {
		t.Error("ReadAfterSeq must surface the per-row scan error on a poison row")
	}
}

// OpenRequestsForActor over a poison request row likewise surfaces the per-row
// scan-error wrap.
func TestOpenRequestsForActor_PoisonRowScanError(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	// A request row addressed to actor "a" (first audience member) with a poison
	// sender kind: the query selects it, the scan then fails the closed-set guard.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id, kind, type, payload, visibility, audience, is_terminal)
		 VALUES ('r1', 1, 1, 'C', 'wizard', 's', 'request', 't', '{}', 'public', '["a"]', 0)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	m := newMessages(db, nil)
	if _, err := m.OpenRequestsForActor(ctx, "a"); err == nil {
		t.Error("OpenRequestsForActor must surface the per-row scan error on a poison row")
	}
}

// ReadAfterSeq with limit<=0 falls back to the default page size (256). Pinning
// the guard: a zero limit still returns rows (not an empty page).
func TestReadAfterSeq_NonPositiveLimitDefaults(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	insertRawMessage(t, db, "m1", "human", "event", "public")
	m := newMessages(db, nil)
	rows, err := m.ReadAfterSeq(ctx, 0, 0) // limit<=0 → default 256
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("limit<=0 default page must still return the row, got %d", len(rows))
	}
}

// --- applyMemberAddTx / applyMemberRemoveTx under a poisoned actor_kind -------

// applyMemberAddTx reactivates a deregistered row. If the row carries a poison
// kind we still reactivate (the add carries the authoritative new kind), so the
// reactivate UPDATE arm runs. We separately drive its error arm and the
// member-lookup default arm.

// applyMemberAddTx's lookup default arm: a Scan failure that is neither nil nor
// ErrNoRows. We force it by giving actor_registry a deregistered_at column that
// holds a non-integer the NullInt64 scan rejects.
func TestApplyMemberAddTx_LookupScanError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "poison.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, actor_binding TEXT,
	   created_at INTEGER, deregistered_at TEXT)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	// deregistered_at = a string that cannot scan into sql.NullInt64.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry VALUES ('a','agent',NULL,1,'not-an-int')`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := reg.applyMemberAddTx(ctx, tx, storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, At: 9}); err == nil {
		t.Error("applyMemberAddTx must surface a non-ErrNoRows scan error from the lookup")
	}
}

// applyMemberRemoveTx error arm: the UPDATE itself fails. We use a tx on a DB
// whose actor_registry lacks deregistered_at so the SET clause errors.
func TestApplyMemberRemoveTx_ExecError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "noderg.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, created_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := reg.applyMemberRemoveTx(ctx, tx, storespec.MemberActorRemove{ID: "a", At: 9}); err == nil {
		t.Error("applyMemberRemoveTx must surface the UPDATE error (missing deregistered_at column)")
	}
}

// applyMemberAddTx insert arm error: actor_registry missing a column the INSERT
// references (no actor_binding) → the no-rows INSERT branch errors.
func TestApplyMemberAddTx_InsertError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "noins.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, created_at INTEGER, deregistered_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := reg.applyMemberAddTx(ctx, tx, storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, At: 9}); err == nil {
		t.Error("applyMemberAddTx INSERT must error (table missing actor_binding column)")
	}
}

// ApplyMemberTransitions must abort (return the helper error) when the add
// helper's INSERT fails mid-transition — driven over a registry whose
// actor_registry lacks the actor_binding column.
func TestApplyMemberTransitions_AddHelperError(t *testing.T) {
	ctx := context.Background()
	db := brokenRegistryDB(t)
	reg := newActorRegistry(db, "C", nil)
	err := reg.ApplyMemberTransitions(ctx,
		[]storespec.MemberActorAdd{{ID: "a", Kind: actor.KindAgent, At: 9}}, nil)
	if err == nil {
		t.Error("ApplyMemberTransitions must propagate the add-helper INSERT error")
	}
}

// ApplyMemberTransitions must abort when the remove helper's UPDATE fails
// mid-transition — driven over a registry missing deregistered_at.
func TestApplyMemberTransitions_RemoveHelperError(t *testing.T) {
	ctx := context.Background()
	db := brokenRegistryDB(t)
	reg := newActorRegistry(db, "C", nil)
	err := reg.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: "a", At: 9}})
	if err == nil {
		t.Error("ApplyMemberTransitions must propagate the remove-helper UPDATE error")
	}
}

// brokenRegistryDB returns a DB with an actor_registry that lacks both
// actor_binding and deregistered_at, so every add INSERT / reactivate UPDATE /
// remove UPDATE referencing those columns errors.
func brokenRegistryDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "broken.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, created_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	return db
}

// applyMemberAddTx reactivate-arm error: a row exists and is deregistered, but
// the UPDATE references a column (actor_binding) the table lacks.
func TestApplyMemberAddTx_ReactivateError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "noreact.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, created_at INTEGER, deregistered_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	// Seed a deregistered row so the lookup finds it and takes the reactivate arm.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, created_at, deregistered_at)
		 VALUES ('a', 'agent', 1, 5)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := reg.applyMemberAddTx(ctx, tx, storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, At: 9}); err == nil {
		t.Error("applyMemberAddTx reactivate UPDATE must error (table missing actor_binding column)")
	}
}
