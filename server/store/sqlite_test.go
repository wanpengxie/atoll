package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/server/store"
)

// TestOpenAndApply creates a fresh sqlite file under t.TempDir() and
// asserts every embedded migration is applied + recorded.
func TestOpenAndApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "server.db")
	db, err := store.Open(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	names, err := store.MigrationNames()
	if err != nil {
		t.Fatalf("MigrationNames: %v", err)
	}
	if len(names) == 0 {
		t.Fatalf("MigrationNames returned empty list")
	}

	var applied int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&applied); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if applied != len(names) {
		t.Errorf("schema_migrations rows = %d, want %d", applied, len(names))
	}

	// Verify a representative schema row from each spec'd table is
	// addressable. Inserting + selecting is sufficient — schema errors
	// raise here before any prod code path runs.
	wantTables := []string{
		"users",
		"verification_codes",
		"sessions",
		"workspaces",
		"workspace_members",
		"channels",
		"channel_members",
		"channel_placements",
		"daemons",
		"device_sessions",
		"view_cache_messages",
		"view_cache_cursors",
	}
	for _, tbl := range wantTables {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil {
			t.Errorf("table %q absent or unreadable: %v", tbl, err)
		}
	}
}

// TestApplyIdempotent runs migrations twice on the same file and
// confirms the second run is a no-op (no error, same row count).
func TestApplyIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "server.db")
	db, err := store.Open(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var before int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&before); err != nil {
		t.Fatalf("count before: %v", err)
	}

	if err := store.Apply(ctx, db); err != nil {
		t.Fatalf("re-Apply: %v", err)
	}

	var after int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&after); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if before != after {
		t.Errorf("schema_migrations changed across Apply runs: %d → %d", before, after)
	}
}
