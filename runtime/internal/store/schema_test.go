package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/runtime/internal/store"
)

// OpenChannel installs exactly the channel-local tables — messages,
// actor_registry — and NOTHING from the retired type_registry epoch.
func TestOpenChannel_InstallsExactlyChannelLocalTables(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	cs, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = cs.Close() }()

	// Inspect the actual sqlite_master via an independent read-only handle (the
	// store confines its own *sql.DB).
	raw, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer func() { _ = raw.Close() }()

	present := map[string]bool{}
	rows, err := raw.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		present[n] = true
	}
	_ = rows.Close()

	for _, name := range store.ChannelLocalTables {
		if !present[name] {
			t.Errorf("expected channel-local table %q missing after OpenChannel", name)
		}
	}
}

// ChannelLocalTables enumerates exactly the surviving channel-local tables:
// the message log, the actor registry, the access plane's channel-scoped
// resources + resource_grants + the create/delete outbox's two server-side
// durable halves (resource_reservations + resource_tombstones, 期11 spec
// §1.3), the actor-scoped state locus actor_state, and the identity-level
// pending-timer control plane timers (type_registry's two tables +
// actor_cursors are deleted).
func TestChannelLocalTables_Set(t *testing.T) {
	want := map[string]bool{
		"messages":                true,
		"actor_registry":          true,
		"actor_decl_versions":     true,
		"channel_genesis":         true,
		"channel_daemon_bindings": true,
		"channel_routing":         true,
		"resources":               true,
		"resource_grants":         true,
		"resource_reservations":   true,
		"resource_tombstones":     true,
		"actor_state":             true,
		"timers":                  true,
		"timer_dead":              true,
	}
	if len(store.ChannelLocalTables) != len(want) {
		t.Fatalf("ChannelLocalTables=%v want exactly %v", store.ChannelLocalTables, want)
	}
	for _, n := range store.ChannelLocalTables {
		if !want[n] {
			t.Errorf("unexpected table %q in ChannelLocalTables", n)
		}
	}
}

func TestOpenChannel_FreshSchemaReopensWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	cs, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("close create: %v", err)
	}
	cs, err = store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{MustExist: true}, nil)
	if err != nil {
		t.Fatalf("reopen current schema: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}
	raw, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var rows int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions`).Scan(&rows); err != nil {
		t.Fatalf("declaration count: %v", err)
	}
}

func TestOpenChannel_MustExistDoesNotCreateMissingPath(t *testing.T) {
	ctx := context.Background()
	missingDir := filepath.Join(t.TempDir(), "missing")
	dbPath := filepath.Join(missingDir, "channel.sqlite")
	if _, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{MustExist: true}, nil); err == nil {
		t.Fatal("MustExist open of a missing path succeeded")
	}
	if _, err := os.Stat(missingDir); !os.IsNotExist(err) {
		t.Fatalf("MustExist created parent directory: stat err=%v", err)
	}
}

func TestOpenChannel_MustExistRejectsEmptyDBBeforeDDL(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "empty.sqlite")
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{MustExist: true}, nil); err == nil {
		t.Fatal("MustExist accepted an empty DB and installed schema")
	}
	raw, err = sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var tables int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("MustExist mutated empty DB: tables=%d", tables)
	}
}

// A read-only open must have NO filesystem write side-effect: it never creates
// the parent directory. A missing path surfaces as a clean open error, not a
// silently-created dir.
func TestOpenChannel_ReadOnlyDoesNotMkdirAll(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	missingDir := filepath.Join(base, "does-not-exist")
	dbPath := filepath.Join(missingDir, "ch.sqlite")

	_, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{ReadOnly: true}, nil)
	if err == nil {
		t.Fatal("ReadOnly open of a missing path must error, not create the file")
	}
	if _, statErr := os.Stat(missingDir); !os.IsNotExist(statErr) {
		t.Errorf("ReadOnly open created the parent dir %q (stat err=%v)", missingDir, statErr)
	}
}

// A non-read-only open of a fresh nested path DOES create the directory tree
// (the write path's MkdirAll), contrasting the ReadOnly behaviour above.
func TestOpenChannel_WriteCreatesParentDir(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nested", "deep", "ch.sqlite")
	cs, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("OpenChannel write: %v", err)
	}
	defer func() { _ = cs.Close() }()
	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Errorf("write open did not create parent dir: %v", err)
	}
}

// Reopening an existing valid channel DB in MustExist mode succeeds and the
// exact schema verification passes.
func TestOpenChannel_MustExistReopenValid(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	cs, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_ = cs.Close()

	cs2, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{MustExist: true}, nil)
	if err != nil {
		t.Fatalf("MustExist reopen of valid DB: %v", err)
	}
	_ = cs2.Close()
}

// Opening a non-empty file that lacks the baseline schema (MustExist, no tables)
// fails fast with a stale-DB error rather than silently migrating.
func TestOpenChannel_MustExistStaleSchemaFailsFast(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "empty.sqlite")

	// Create an empty-but-valid sqlite with no channel tables.
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `CREATE TABLE unrelated (x INTEGER)`); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	_ = raw.Close()

	if _, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{MustExist: true}, nil); err == nil {
		t.Fatal("MustExist open of a DB missing the baseline schema must fail fast")
	}
}

func TestOpenChannel_MustExistRejectsEverySchemaMismatchWithoutMutation(t *testing.T) {
	replaceOne := func(old, replacement string) func(string) string {
		return func(ddl string) string {
			if strings.Count(ddl, old) != 1 {
				t.Fatalf("schema test fixture expected one occurrence of %q", old)
			}
			return strings.Replace(ddl, old, replacement, 1)
		}
	}
	cases := []struct {
		name   string
		mutate func(string) string
	}{
		{"missing-message-column", replaceOne("  payload              TEXT NOT NULL,\n", "")},
		{"wrong-message-column-type", replaceOne("  payload              TEXT NOT NULL,\n", "  payload              BLOB NOT NULL,\n")},
		{"missing-message-unique-constraint", replaceOne("  id                   TEXT NOT NULL UNIQUE,\n", "  id                   TEXT NOT NULL,\n")},
		{"missing-partial-index", replaceOne("CREATE INDEX IF NOT EXISTS ix_messages_expires        ON messages(expires_at) WHERE expires_at IS NOT NULL AND kind='request';\n", "")},
		{"extra-table", func(ddl string) string { return ddl + `CREATE TABLE retired_shadow (x INTEGER);` }},
		{"extra-index", func(ddl string) string { return ddl + `CREATE INDEX retired_index ON messages(type);` }},
		{"extra-view", func(ddl string) string { return ddl + `CREATE VIEW retired_view AS SELECT id FROM messages;` }},
		{"extra-trigger", func(ddl string) string {
			return ddl + `CREATE TRIGGER retired_trigger AFTER INSERT ON messages BEGIN SELECT 1; END;`
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "channel.sqlite")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, tc.mutate(store.ChannelLocalDDL)); err != nil {
				_ = db.Close()
				t.Fatalf("seed mismatched schema: %v", err)
			}
			before := schemaCatalog(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if cs, err := store.OpenChannel(ctx, "C-test", path, store.OpenOptions{MustExist: true}, nil); err == nil {
				_ = cs.Close()
				t.Fatal("strict reopen accepted a mismatched channel schema")
			}

			db, err = sql.Open("sqlite", "file:"+path+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if after := schemaCatalog(t, db); after != before {
				t.Fatalf("failed strict reopen mutated schema:\nbefore=%s\nafter=%s", before, after)
			}
		})
	}
}

func schemaCatalog(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out strings.Builder
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			t.Fatal(err)
		}
		out.WriteString(typ)
		out.WriteByte('\x00')
		out.WriteString(name)
		out.WriteByte('\x00')
		out.WriteString(ddl)
		out.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out.String()
}
