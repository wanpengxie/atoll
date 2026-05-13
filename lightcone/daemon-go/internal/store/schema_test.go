package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenChannel_BuildsSpecTables verifies that opening a fresh channel
// sqlite produces exactly the 6 tables L2 §1.4 mandates.
func TestOpenChannel_BuildsSpecTables(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	for _, name := range ChannelLocalTables {
		if !objectExists(t, ctx, db, "table", name) {
			t.Errorf("expected table %q to exist after OpenChannel", name)
		}
	}
}

// TestOpenChannel_BuildsSpecIndexes verifies every required index ships,
// including the partial UNIQUE INDEX that enforces The One Law.
func TestOpenChannel_BuildsSpecIndexes(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	for _, name := range ChannelLocalIndexes {
		if !objectExists(t, ctx, db, "index", name) {
			t.Errorf("expected index %q to exist after OpenChannel", name)
		}
	}
}

// TestOpenChannel_PartialUniqueIndexWHERE verifies that the partial
// UNIQUE INDEX uses the exact WHERE clause demanded by L2 §1.4.1.
// Drift here breaks The One Law uniqueness invariant.
func TestOpenChannel_PartialUniqueIndexWHERE(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	var ddl string
	err := db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='index' AND name=?`,
		"ux_terminal_response_per_request",
	).Scan(&ddl)
	if err != nil {
		t.Fatalf("query partial index sql: %v", err)
	}

	want := []string{
		`ux_terminal_response_per_request`,
		`messages`,
		`parent_id`,
		`kind = 'response'`,
		`is_terminal = 1`,
	}
	for _, frag := range want {
		if !strings.Contains(ddl, frag) {
			t.Errorf("partial index DDL missing %q in: %s", frag, ddl)
		}
	}
}

// TestOpenChannel_ExpiresIndexIsPartial verifies the request-only partial
// index for expires_at (L2 §1.4.1).
func TestOpenChannel_ExpiresIndexIsPartial(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	var ddl string
	if err := db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='index' AND name=?`,
		"ix_messages_expires",
	).Scan(&ddl); err != nil {
		t.Fatalf("query ix_messages_expires sql: %v", err)
	}

	for _, frag := range []string{"expires_at", "kind='request'"} {
		if !strings.Contains(ddl, frag) {
			t.Errorf("expected ix_messages_expires DDL to contain %q, got %s", frag, ddl)
		}
	}
}

// TestOpenChannel_Idempotent verifies that opening the same path twice
// is a no-op — CREATE TABLE IF NOT EXISTS protects us from a second
// `OpenChannel` call (e.g. daemon restart) wiping data.
func TestOpenChannel_Idempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.sqlite")

	db1, err := OpenChannel(ctx, path, OpenOptions{})
	if err != nil {
		t.Fatalf("first OpenChannel: %v", err)
	}
	// Write a sentinel row to verify the second open does not wipe.
	if _, err := db1.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at)
		 VALUES ('system', 'system', NULL, 1)`,
	); err != nil {
		t.Fatalf("insert sentinel: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := OpenChannel(ctx, path, OpenOptions{})
	if err != nil {
		t.Fatalf("second OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	var n int
	if err := db2.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id='system'`,
	).Scan(&n); err != nil {
		t.Fatalf("count sentinel: %v", err)
	}
	if n != 1 {
		t.Errorf("sentinel row count = %d, want 1 (second open should not wipe data)", n)
	}
}

// TestOpenChannel_PragmasApplied verifies that WAL + busy_timeout +
// foreign_keys are enforced on the connection. Drift here breaks
// concurrent-write semantics that the partial UNIQUE INDEX and
// worker_locks CAS rely on.
func TestOpenChannel_PragmasApplied(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}

	var fk int
	if err := db.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&fk); err != nil {
		t.Fatalf("query foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, want 1", fk)
	}

	var busy int
	if err := db.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busy); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busy < 1000 {
		t.Errorf("busy_timeout = %d ms, want >= 1000", busy)
	}
}

// TestOpenChannel_MessagesCheckConstraints verifies the CHECK constraints
// guard against off-enum values. Without these, harness step bugs would
// poison the channel sqlite irrecoverably.
func TestOpenChannel_MessagesCheckConstraints(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	// kind enum
	_, err := db.ExecContext(ctx,
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		                       kind, type, payload, visibility, audience)
		 VALUES ('m1', 1, 1, 'c', 'human', 'u', 'gossip', 't', '{}', 'public', '["*"]')`,
	)
	if err == nil {
		t.Errorf("expected kind CHECK to reject 'gossip'")
	}

	// visibility enum
	_, err = db.ExecContext(ctx,
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		                       kind, type, payload, visibility, audience)
		 VALUES ('m2', 1, 1, 'c', 'human', 'u', 'event', 't', '{}', 'invisible', '["*"]')`,
	)
	if err == nil {
		t.Errorf("expected visibility CHECK to reject 'invisible'")
	}

	// is_terminal range
	_, err = db.ExecContext(ctx,
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		                       kind, type, payload, visibility, audience, is_terminal)
		 VALUES ('m3', 1, 1, 'c', 'human', 'u', 'event', 't', '{}', 'public', '["*"]', 2)`,
	)
	if err == nil {
		t.Errorf("expected is_terminal CHECK to reject value 2")
	}
}

// TestOpenDaemon_BuildsBootstrapRegistry verifies the daemon-level sqlite
// gets bootstrap_registry + its status index.
func TestOpenDaemon_BuildsBootstrapRegistry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "daemon.sqlite")
	db, err := OpenDaemon(ctx, path, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenDaemon: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if !objectExists(t, ctx, db, "table", "bootstrap_registry") {
		t.Errorf("bootstrap_registry table missing")
	}
	if !objectExists(t, ctx, db, "index", "ix_bootstrap_status") {
		t.Errorf("ix_bootstrap_status index missing")
	}

	// status CHECK constraint
	_, err = db.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('r1', 'c1', 'frobbed', '/tmp', 1)`,
	)
	if err == nil {
		t.Errorf("expected status CHECK to reject 'frobbed'")
	}
}

// TestOpenChannel_SkipDDL verifies SkipDDL leaves the file empty so the
// migrate tool can use it for foreign Node sqlite files.
func TestOpenChannel_SkipDDL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "blank.sqlite")
	db, err := OpenChannel(ctx, path, OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("OpenChannel SkipDDL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if objectExists(t, ctx, db, "table", "messages") {
		t.Errorf("messages table should NOT exist when SkipDDL=true")
	}
}

// openTempChannel is a test helper that creates a fresh channel sqlite
// in t.TempDir() and registers cleanup.
func openTempChannel(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	db, err := OpenChannel(ctx, path, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// objectExists returns true if `sqlite_master` has a row of the given
// type + name. Used to assert table/index presence post-DDL.
func objectExists(t *testing.T, ctx context.Context, db *sql.DB, objType, name string) bool {
	t.Helper()
	var got string
	err := db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type=? AND name=?`,
		objType, name,
	).Scan(&got)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master for %s %q: %v", objType, name, err)
	}
	return got == name
}
