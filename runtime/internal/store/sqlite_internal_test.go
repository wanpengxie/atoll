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

// ddlTableColumns executes ChannelLocalDDL into a throwaway sqlite and reads
// back, per created table, the REAL column set (the package's own tableColumns,
// i.e. PRAGMA table_info). The database engine is the parser — no hand-rolled
// DDL text scanning, so there is zero room for a false green on DDL syntax a
// scanner did not anticipate (e.g. two columns declared on one line).
func ddlTableColumns(t *testing.T) map[string]map[string]struct{} {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ddl.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite with ChannelLocalDDL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}

	tables := map[string]map[string]struct{}{}
	for _, name := range names {
		cols, err := tableColumns(ctx, db, name)
		if err != nil {
			t.Fatalf("tableColumns(%q): %v", name, err)
		}
		tables[name] = cols
	}
	return tables
}

// TestChannelLocalSchemaShapeMatchesDDL machine-reconciles channelLocalSchemaShape
// against the schema.go DDL so the two can never silently drift by手抄 again
// (the failure mode that had actor_registry / resource_reservations lose columns
// that were added to the DDL). Every table's column set must be bidirectionally
// equal to the DDL's — EXCEPT messages, whose shape entry is a deliberate
// representative probe (documented at channelLocalSchemaShape) and is only
// required to be a subset of the DDL's columns.
func TestChannelLocalSchemaShapeMatchesDDL(t *testing.T) {
	ddl := ddlTableColumns(t)

	// Every shaped table must exist in the DDL, and every DDL table must be
	// shaped — the table-name sets are bidirectionally equal.
	for table := range channelLocalSchemaShape {
		if _, ok := ddl[table]; !ok {
			t.Errorf("shape lists table %q that the DDL does not declare", table)
		}
	}
	for table := range ddl {
		if _, ok := channelLocalSchemaShape[table]; !ok {
			t.Errorf("DDL declares table %q that the shape does not list", table)
		}
	}

	for table, cols := range channelLocalSchemaShape {
		ddlCols, ok := ddl[table]
		if !ok {
			continue // already reported above
		}
		shapeCols := map[string]struct{}{}
		for _, c := range cols {
			shapeCols[c] = struct{}{}
		}
		// Every shaped column must exist in the DDL (both messages-subset and
		// full-mirror tables demand this direction).
		for c := range shapeCols {
			if _, ok := ddlCols[c]; !ok {
				t.Errorf("table %q: shape column %q is not in the DDL", table, c)
			}
		}
		if table == "messages" {
			continue // intentional subset — direction above is the only assertion
		}
		// Full-mirror tables: the DDL must carry no column the shape omits.
		for c := range ddlCols {
			if _, ok := shapeCols[c]; !ok {
				t.Errorf("table %q: DDL column %q is missing from the shape (drift)", table, c)
			}
		}
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
