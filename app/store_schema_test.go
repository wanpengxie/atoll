package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenDB_InstallsFreshSchemaAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	p, err := OpenProcessDB(path, true)
	if err != nil {
		t.Fatalf("OpenDB fresh: %v", err)
	}
	db := p.DB
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

	if err := p.Close(); err != nil {
		t.Fatalf("close fresh process DB: %v", err)
	}
	p, err = OpenProcessDB(path, false)
	if err != nil {
		t.Fatalf("OpenDB reopen: %v", err)
	}
	defer p.Close()
	db = p.DB
	var class string
	if err := db.QueryRow(`SELECT default_class FROM actor_decls WHERE id='d1'`).Scan(&class); err != nil {
		t.Fatalf("read declaration after reopen: %v", err)
	}
	if class != "echo" {
		t.Fatalf("default_class=%q want echo", class)
	}

	assertColumnAbsent(t, db, "channels", "default_agent")
	assertColumnAbsent(t, db, "actor_decls", "looper")
	assertColumnAbsent(t, db, "actor_decls", "default_looper")
}

func TestAdmissionOwnerSchemaEnforcesTaggedIdentityAndScopedIdempotency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	process, err := OpenProcessDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Close() })
	db := process.DB
	insert := func(id, ch, principal, actorID, key string) error {
		var p, a, k any
		if principal != "<null>" {
			p = principal
		}
		if actorID != "<null>" {
			a = actorID
		}
		if key != "<null>" {
			k = key
		}
		_, err := db.Exec(`INSERT INTO channel_admission_operations
			(operation_id,idempotency_key,channel_id,op,requested_by_principal,requested_by_actor_id,request_json,request_digest,created_at)
			VALUES (?,?,?,?,?,?,?,?,?)`, id, k, ch, "join", p, a, `{}`, "digest:"+id, 1)
		return err
	}
	if err := insert("principal", "c1", "login", "<null>", "p-key"); err != nil {
		t.Fatalf("principal-only owner rejected: %v", err)
	}
	if err := insert("actor", "c1", "<null>", "actor:one", "shared"); err != nil {
		t.Fatalf("actor-only owner rejected: %v", err)
	}
	if err := insert("neither", "c1", "<null>", "<null>", "<null>"); err == nil {
		t.Fatal("owner XOR accepted two NULL coordinates")
	}
	if err := insert("empty", "c1", "", "<null>", "<null>"); err == nil {
		t.Fatal("owner XOR accepted an empty principal")
	}
	if err := insert("both", "c1", "login-2", "actor:two", "<null>"); err == nil {
		t.Fatal("owner XOR accepted both coordinates")
	}
	if err := insert("actor-other", "c1", "<null>", "actor:two", "shared"); err != nil {
		t.Fatalf("different actor collided on idempotency key: %v", err)
	}
	if err := insert("actor-other-channel", "c2", "<null>", "actor:one", "shared"); err != nil {
		t.Fatalf("same actor spelling in another channel collided: %v", err)
	}
	if err := insert("actor-duplicate", "c1", "<null>", "actor:one", "shared"); err == nil {
		t.Fatal("same channel actor reused idempotency key")
	}
	if err := insert("principal-cross-channel", "c2", "login", "<null>", "p-key"); err == nil {
		t.Fatal("principal idempotency key unexpectedly became channel-scoped")
	}
}

func TestOpenProcessDB_StrictReopenRejectsMalformedSchemaWithoutMutation(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, string)
	}{
		{
			name: "empty",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				if err := db.Ping(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "incomplete",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				if _, err := db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra object",
			build: func(t *testing.T, path string) {
				p, err := OpenProcessDB(path, true)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.DB.Exec(`CREATE TABLE unexpected (id TEXT)`); err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "type incompatible",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				if err := initializeSchema(db); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`ALTER TABLE users ADD COLUMN incompatible BLOB`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "constraint incompatible",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				initializeSchemaVariant(t, db, func(object schemaObject) string {
					if object.name == "users" {
						return strings.Replace(object.sql, "email TEXT UNIQUE NOT NULL", "email TEXT", 1)
					}
					return object.sql
				})
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "index definition incompatible",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				initializeSchemaVariant(t, db, func(object schemaObject) string {
					if object.name == "ix_admission_pending" {
						return `CREATE INDEX ix_admission_pending ON channel_admission_operations(operation_id)`
					}
					return object.sql
				})
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra trigger",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				if err := initializeSchema(db); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`CREATE TRIGGER unexpected_trigger AFTER INSERT ON users BEGIN SELECT 1; END`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra view",
			build: func(t *testing.T, path string) {
				db := openRawSQLite(t, path)
				if err := initializeSchema(db); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`CREATE VIEW unexpected_view AS SELECT id FROM users`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.db")
			tc.build(t, path)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			p, err := OpenProcessDB(path, false)
			if err == nil {
				_ = p.Close()
				t.Fatal("strict reopen accepted malformed schema")
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("strict reopen mutated rejected database")
			}
		})
	}
}

func openRawSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func initializeSchemaVariant(t *testing.T, db *sql.DB, definition func(schemaObject) string) {
	t.Helper()
	for _, object := range appSchema {
		if _, err := db.Exec(definition(object)); err != nil {
			t.Fatalf("create schema variant object %s: %v", object.name, err)
		}
	}
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
