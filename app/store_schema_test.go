package app

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenDB_InstallsFreshSchemaAndReopensWithoutRetiredObjects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB fresh: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO users (id, email, password, created_at) VALUES ('u1','a@b','x',0)`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO actor_decls (id, name, owner, default_class, created_at, updated_at) VALUES ('d1','D','u1','echo',0,0)`); err != nil {
		t.Fatalf("insert declaration: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fresh DB: %v", err)
	}

	db, err = OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB reopen: %v", err)
	}
	defer db.Close()
	var class string
	if err := db.QueryRow(`SELECT default_class FROM actor_decls WHERE id='d1'`).Scan(&class); err != nil {
		t.Fatalf("read declaration after reopen: %v", err)
	}
	if class != "echo" {
		t.Fatalf("default_class=%q want echo", class)
	}

	for _, retired := range []string{"agents", "channel_actors"} {
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, retired).Scan(&n); err != nil {
			t.Fatalf("inspect retired table %q: %v", retired, err)
		}
		if n != 0 {
			t.Errorf("retired table %q exists in fresh schema", retired)
		}
	}
	assertColumnAbsent(t, db, "channels", "default_agent")
	assertColumnAbsent(t, db, "actor_decls", "looper")
	assertColumnAbsent(t, db, "actor_decls", "default_looper")
}

func assertColumnAbsent(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table_info(%s): %v", table, err)
		}
		if name == column {
			t.Errorf("retired column %s.%s exists in fresh schema", table, column)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info(%s): %v", table, err)
	}
}
