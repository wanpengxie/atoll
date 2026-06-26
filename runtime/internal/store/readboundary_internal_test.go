package store

// In-package (white-box) tests for the closed-set READ BOUNDARY.
//
// Closed-set vocabularies are enforced in Go, not by a DB CHECK (schema.go
// carries no value-set CHECK on sender_kind / kind / visibility / actor_kind /
// actor_binding). The write path is guarded by the harness; the read path is
// guarded by the store scan (ParseKind / ParseVisibility / ParseBinding). To
// exercise those read-side guards we inject poison rows that a real writer
// could never produce — a corrupted file or a forward-incompatible writer that
// committed an out-of-set value. The relaxedDDL below is a plain mirror of the
// channel tables used purely as the injection vehicle. This is only reachable
// from inside the package — exactly the structural confinement the store relies
// on. The contract under test: a corrupted / forward-version DB value MUST
// surface as an error on read, never be silently cast into the closed-set ADT
// (kernel closed-set invariant).

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// relaxedDDL is a stripped mirror of the channel tables (no indexes, no
// is_terminal bool CHECK) used only to inject out-of-closed-set poison values
// that a real DB could acquire only via corruption or a forward-incompatible
// writer. The closed-set vocabularies were never DB-enforced, so no value-set
// CHECK is dropped here — the poison would insert against schema.go too.
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
