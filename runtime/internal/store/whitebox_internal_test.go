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

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
	db, err := openSqlite(ctx, filepath.Join(dir, "narrow.sqlite"), OpenOptions{}, "")
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
		`INSERT INTO actor_registry (actor_id, actor_kind, created_at, deregistered_at)
		 VALUES ('x', 'wizard', 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, err := reg.ListActive(ctx); err == nil {
		t.Error("ListActive must error on out-of-closed-set kind (rows path)")
	}
}

// A poison placement column is the second closed-set on the record read path
// and must fail loudly rather than raw-cast into the ActorRecord.
func TestLookupActive_PoisonPlacement(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, class, placement, created_at, deregistered_at)
		 VALUES ('x', 'agent', 'agent', 'teleport', 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, _, err := reg.LookupActive(ctx, "x"); err == nil {
		t.Error("LookupActive must error on out-of-closed-set placement")
	}
}

// ListActive raw rows.Scan error: an active row whose created_at column holds
// non-integer text cannot scan into the int64 Record.CreatedAt, hitting the
// scan-error arm (distinct from the closed-set kind guard). Reachable only via
// a relaxed DB whose created_at is typeless TEXT.
func TestListActive_RawScanError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSqlite(ctx, filepath.Join(dir, "txtcreated.sqlite"), OpenOptions{}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// created_at typed TEXT so a non-numeric value survives insert, then fails
	// the int64 scan in ListActive.
	if _, err := db.ExecContext(ctx, `CREATE TABLE actor_registry (
	   actor_id TEXT PRIMARY KEY, actor_kind TEXT, principal TEXT DEFAULT '',
	   source_decl_id TEXT DEFAULT '', class TEXT DEFAULT '', config_json TEXT,
	   placement TEXT DEFAULT 'server', desired_host TEXT DEFAULT '',
	   created_at TEXT, deregistered_at INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, created_at, deregistered_at)
		 VALUES ('a', 'agent', 'not-a-number', NULL)`); err != nil {
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

// --- LookupActivePrincipal: poison-row discipline (third machine over the
// same table — purity v3 A2: it silently discarded ParseKind/ParseBinding
// errors while Lookup/ListActive fail loudly; the three read machines must
// share one discipline, and THIS one feeds the admission path
// (census.ResolvePrincipal)). ---------------------------------------------

// Kind poison is unreachable through parsed callers (the WHERE clause echoes
// the caller's kind back), but the guard is kept symmetric with the other two
// machines — this whitebox probe passes the poison kind straight through.
func TestLookupActivePrincipal_PoisonKind(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, principal, created_at, deregistered_at)
		 VALUES ('x', 'wizard', 'p', 1, NULL)`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, _, err := reg.LookupActivePrincipal(ctx, actor.Kind("wizard"), "p"); err == nil {
		t.Error("LookupActivePrincipal must error on out-of-closed-set kind")
	}
}
