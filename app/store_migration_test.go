package app

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// store_migration_test.go proves the agents→actor_decls table rename (S0) against
// every DB vintage it must serve. The migration form is load-bearing (spec §2):
// the rename runs BEFORE the canonical CREATE TABLE IF NOT EXISTS — running it
// after would either recreate an empty zombie `agents` table (CREATE kept the old
// name) or strand the old data invisible behind a fresh empty `actor_decls`
// (CREATE used the new name, rename fails on existing target). Five paths + an
// idempotency re-open, each asserting row survival, guard against both.

// rawExec opens the sqlite file WITHOUT migrating (fixture setup for old-vintage
// DBs) and runs the statements.
func rawExec(t *testing.T, path string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("raw exec %q: %v", s, err)
		}
	}
}

// rawCount counts rows via a throwaway un-migrated connection.
func rawCount(t *testing.T, path, query string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("raw count %q: %v", query, err)
	}
	return n
}

// assertDeclVisible opens via OpenDB (runs the migration) and asserts the seeded
// declaration row is readable through the NEW table + column names.
func assertDeclVisible(t *testing.T, path, wantID, wantClass string) {
	t.Helper()
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	var class string
	if err := db.QueryRow(
		`SELECT default_class FROM actor_decls WHERE id = ?`, wantID).Scan(&class); err != nil {
		t.Fatalf("read migrated decl row: %v", err)
	}
	if class != wantClass {
		t.Fatalf("default_class = %q, want %q", class, wantClass)
	}
	// The old table name must be gone — a lingering `agents` table means the
	// canonical DDL recreated a zombie.
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='agents'`).Scan(&n); err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	if n != 0 {
		t.Fatalf("zombie `agents` table exists after migration")
	}
}

// Path 1: fresh DB — canonical DDL creates actor_decls directly.
func TestMigration_FreshOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO users (id, email, password, created_at) VALUES ('u1','a@b','x',0)`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO actor_decls (id, name, owner, default_class, created_at, updated_at) VALUES ('d1','D','u1','echo',0,0)`); err != nil {
		t.Fatalf("insert decl: %v", err)
	}
	db.Close()
	assertDeclVisible(t, path, "d1", "echo")
}

// Path 2: idempotency — the SAME db opened twice must not grow a zombie `agents`
// table (the CREATE block no-ops on actor_decls, the rename no-ops on absence).
func TestMigration_DoubleOpenIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	rawExec(t, path,
		`CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
			default_looper TEXT NOT NULL, config_json TEXT, deleted_at INTEGER,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			visibility TEXT NOT NULL DEFAULT 'private')`,
		`INSERT INTO agents (id, name, owner, default_looper, created_at, updated_at) VALUES ('d2','D','u1','claude',0,0)`,
	)
	assertDeclVisible(t, path, "d2", "claude") // first open migrates
	assertDeclVisible(t, path, "d2", "claude") // second open must be a no-op
	if n := rawCount(t, path, `SELECT COUNT(*) FROM actor_decls`); n != 1 {
		t.Fatalf("row count after double open = %d, want 1", n)
	}
}

// Path 3: HEAD-vintage old DB (agents + default_looper) — table rename + one
// column rename.
func TestMigration_HeadVintageAgentsTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	rawExec(t, path,
		`CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
			default_looper TEXT NOT NULL, config_json TEXT, deleted_at INTEGER,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			visibility TEXT NOT NULL DEFAULT 'private')`,
		`INSERT INTO agents (id, name, owner, default_looper, created_at, updated_at) VALUES ('d3','D','u1','go-kimi',0,0)`,
	)
	assertDeclVisible(t, path, "d3", "go-kimi")
}

// Path 4: pre-class-split vintage (agents + looper, no visibility) — table rename
// + full column chain (looper→default_looper→default_class) + visibility ADD.
func TestMigration_LooperVintageAgentsTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	rawExec(t, path,
		`CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
			looper TEXT NOT NULL, config_json TEXT, deleted_at INTEGER,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`INSERT INTO agents (id, name, owner, looper, created_at, updated_at) VALUES ('d4','D','u1','claude',0,0)`,
	)
	assertDeclVisible(t, path, "d4", "claude")
	// The visibility backfill must have landed on the renamed table too.
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	var vis string
	if err := db.QueryRow(`SELECT visibility FROM actor_decls WHERE id='d4'`).Scan(&vis); err != nil {
		t.Fatalf("visibility column missing after migration: %v", err)
	}
	if vis != "private" {
		t.Fatalf("visibility = %q, want private", vis)
	}
}

// Path 5 (negative): BOTH tables present — a half-migrated / hand-touched DB.
// OpenDB must fail loud and touch neither table's rows (the surrounding migration
// is riddled with best-effort swallowed ALTERs; this asserts the rename step did
// NOT inherit that posture).
func TestMigration_BothTablesFailLoud(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	rawExec(t, path,
		`CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
			default_looper TEXT NOT NULL, config_json TEXT, deleted_at INTEGER,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			visibility TEXT NOT NULL DEFAULT 'private')`,
		`INSERT INTO agents (id, name, owner, default_looper, created_at, updated_at) VALUES ('old','O','u1','claude',0,0)`,
		`CREATE TABLE actor_decls (id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
			default_class TEXT NOT NULL, config_json TEXT, deleted_at INTEGER,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			visibility TEXT NOT NULL DEFAULT 'private')`,
		`INSERT INTO actor_decls (id, name, owner, default_class, created_at, updated_at) VALUES ('new','N','u1','echo',0,0)`,
	)
	if _, err := OpenDB(path); err == nil {
		t.Fatalf("OpenDB succeeded with both agents and actor_decls present — must fail loud")
	}
	if n := rawCount(t, path, `SELECT COUNT(*) FROM agents`); n != 1 {
		t.Fatalf("agents rows = %d after failed open, want 1 (untouched)", n)
	}
	if n := rawCount(t, path, `SELECT COUNT(*) FROM actor_decls`); n != 1 {
		t.Fatalf("actor_decls rows = %d after failed open, want 1 (untouched)", n)
	}
}
