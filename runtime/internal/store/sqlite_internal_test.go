package store

// In-package tests for the unexported sqlite open / schema-verify primitives.
// openSqlite, verifySchema and tableColumns are confined to this package (the
// raw *sql.DB must never cross the store boundary), so their defensive arms
// can only be driven from inside the package.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// openSqlite("") is the empty-dbPath guard.
func TestOpenSqlite_EmptyPath(t *testing.T) {
	if _, err := openSqlite(context.Background(), "", OpenOptions{}, ""); err == nil {
		t.Error("openSqlite with empty dbPath must error")
	}
}

// A write open whose parent path is an existing *file* (not a dir) makes
// MkdirAll fail — the mkdir error arm.
func TestOpenSqlite_MkdirAllFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Create a regular file, then ask to open a DB "under" it: MkdirAll must
	// fail because a path component is a file.
	blocker := filepath.Join(dir, "iam-a-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	dbPath := filepath.Join(blocker, "sub", "ch.sqlite")
	if _, err := openSqlite(ctx, dbPath, OpenOptions{}, ChannelLocalDDL); err == nil {
		t.Error("openSqlite must error when MkdirAll cannot create the parent (component is a file)")
	}
}

// A non-empty, syntactically invalid DDL string makes the DDL exec arm fail.
func TestOpenSqlite_BadDDLErrors(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	if _, err := openSqlite(ctx, dbPath, OpenOptions{}, "THIS IS NOT VALID SQL;"); err == nil {
		t.Error("openSqlite must error on invalid DDL")
	}
}

// verifySchema must propagate a tableColumns error (the err!=nil arm) and the
// missing-column arm (table present, required column absent).
func TestVerifySchema_TableColumnsErrorPropagates(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// A bad table identifier makes PRAGMA table_info(...) a syntax error, which
	// tableColumns wraps and verifySchema must surface.
	shape := map[string][]string{"bad(name": {"x"}}
	if err := verifySchema(ctx, db, "channel", shape); err == nil {
		t.Error("verifySchema must propagate the tableColumns query error")
	}
}

func TestVerifySchema_MissingColumn(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE present (a INTEGER)`); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	// Table exists but lacks the required column "missing" → missing-column arm.
	shape := map[string][]string{"present": {"a", "missing"}}
	if err := verifySchema(ctx, db, "channel", shape); err == nil {
		t.Error("verifySchema must error on a present table missing a required column")
	}
}

// tableColumns over a bad identifier surfaces the QueryContext error arm.
func TestTableColumns_QueryError(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{SkipDDL: true}, "")
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := tableColumns(ctx, db, "bad(name"); err == nil {
		t.Error("tableColumns must error on a syntactically invalid table identifier")
	}
}

// tableColumns over a real table returns the column set (the happy rows loop) —
// pins the success path the verify guard depends on.
func TestTableColumns_ReturnsColumnSet(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cols, err := tableColumns(ctx, db, "actor_registry")
	if err != nil {
		t.Fatalf("tableColumns: %v", err)
	}
	for _, want := range []string{"actor_id", "actor_kind", "deregistered_at"} {
		if _, ok := cols[want]; !ok {
			t.Errorf("column %q missing from actor_registry set %v", want, cols)
		}
	}
}
