package store

// In-package (white-box) tests for the closed-set READ BOUNDARY.
//
// The channel DDL's CHECK constraints block out-of-set values on the WRITE
// path, so to exercise the read-side ParseKind/ParseVisibility guards we must
// build a relaxed sqlite (no CHECKs) and inject poison rows directly. This is
// only reachable from inside the package — exactly the structural confinement
// the store relies on. The contract under test: a corrupted / forward-version
// DB value MUST surface as an error on read, never be silently cast into the
// closed-set ADT (kernel closed-set invariant).

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// relaxedDDL mirrors the channel tables but drops the CHECK constraints so we
// can inject out-of-closed-set poison values that a real DB could only acquire
// via corruption or a forward-incompatible writer.
const relaxedDDL = `
CREATE TABLE messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE, ts INTEGER NOT NULL, ts_received INTEGER NOT NULL,
  channel_id TEXT NOT NULL, sender_kind TEXT NOT NULL, sender_id TEXT NOT NULL,
  kind TEXT NOT NULL, type TEXT NOT NULL, payload TEXT NOT NULL,
  parent_id TEXT, correlation_id TEXT, visibility TEXT NOT NULL,
  audience TEXT NOT NULL, expires_at INTEGER, is_terminal INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE actor_registry (
  actor_id TEXT PRIMARY KEY, actor_kind TEXT NOT NULL, actor_binding TEXT,
  created_at INTEGER NOT NULL, deregistered_at INTEGER
);
`

func openRelaxed(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "relaxed.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, relaxedDDL); err != nil {
		t.Fatalf("relaxed DDL: %v", err)
	}
	return db
}

func TestRegistry_ReadRejectsPoisonKind(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('x', 'wizard', NULL, 1, NULL)`); err != nil {
		t.Fatalf("inject poison: %v", err)
	}
	reg := newActorRegistry(db, "C", nil)
	if _, _, err := reg.Lookup(ctx, "x"); err == nil {
		t.Error("Lookup must error on out-of-closed-set actor_kind, not silently cast into ADT")
	}
	if _, err := reg.ListActive(ctx); err == nil {
		t.Error("ListActive must error on out-of-closed-set actor_kind")
	}
}

func TestMessages_ReadRejectsPoisonSenderKind(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	insertRawMessage(t, db, "m1", "wizard", "request", "public")
	m := newMessages(db, nil)
	if _, _, err := m.FindByID(ctx, "m1"); err == nil {
		t.Error("FindByID must error on out-of-closed-set sender_kind")
	}
}

func TestMessages_ReadRejectsPoisonKind(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	insertRawMessage(t, db, "m1", "human", "telepathy", "public")
	m := newMessages(db, nil)
	if _, _, err := m.FindByID(ctx, "m1"); err == nil {
		t.Error("FindByID must error on out-of-closed-set message kind")
	}
}

func TestMessages_ReadRejectsPoisonVisibility(t *testing.T) {
	ctx := context.Background()
	db := openRelaxed(t)
	insertRawMessage(t, db, "m1", "human", "event", "cosmic")
	m := newMessages(db, nil)
	if _, _, err := m.FindByID(ctx, "m1"); err == nil {
		t.Error("FindByID must error on out-of-closed-set visibility")
	}
}

func insertRawMessage(t *testing.T, db *sql.DB, id, senderKind, kind, vis string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id, kind, type, payload, visibility, audience, is_terminal)
		 VALUES (?, 1, 1, 'C', ?, 's', ?, 't', '{}', ?, '["a"]', 0)`,
		id, senderKind, kind, vis); err != nil {
		t.Fatalf("inject raw message: %v", err)
	}
}
