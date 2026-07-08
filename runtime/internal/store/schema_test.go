package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
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
	// Retired epoch: the type_registry tables must NOT exist.
	for _, gone := range []string{"type_registry", "type_registry_schemas", "action_ledger", "worker_locks"} {
		if present[gone] {
			t.Errorf("retired table %q must not be created", gone)
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
		"messages":              true,
		"actor_registry":        true,
		"resources":             true,
		"resource_grants":       true,
		"resource_reservations": true,
		"resource_tombstones":   true,
		"actor_state":           true,
		"timers":                true,
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

// Reopening an existing valid channel DB with SkipDDL=true succeeds and the
// schema verification passes.
func TestOpenChannel_SkipDDLReopenValid(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ch.sqlite")
	cs, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_ = cs.Close()

	cs2, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{SkipDDL: true}, nil)
	if err != nil {
		t.Fatalf("SkipDDL reopen of valid DB: %v", err)
	}
	_ = cs2.Close()
}

// Opening a non-empty file that lacks the baseline schema (SkipDDL, no tables)
// fails fast with a stale-DB error rather than silently migrating.
func TestOpenChannel_SkipDDLStaleSchemaFailsFast(t *testing.T) {
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

	if _, err := store.OpenChannel(ctx, "C-test", dbPath, store.OpenOptions{SkipDDL: true}, nil); err == nil {
		t.Fatal("SkipDDL open of a DB missing the baseline schema must fail fast")
	}
}
