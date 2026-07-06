package app

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// TestScanCompositionRows_ScanErrorFailsClosed pins the fail-closed fix: a per-row
// scan error must ABORT with an error, not skip the row and return a partial set.
// This set feeds the reconcile ring's desired source, where a silently-missing row
// reads as "no longer desired" and culls a still-desired live cell. A NULL scanned
// into the non-nullable instance_id string is a deterministic scan error.
func TestScanCompositionRows_ScanErrorFailsClosed(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// First column NULL → Scan into *string errors (converting NULL is unsupported).
	rows, err := db.Query(`SELECT NULL, 'claude', '', ''`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	out, err := scanCompositionRows(rows)
	if err == nil {
		t.Fatalf("scan error must abort with an error, got nil (partial set %v)", out)
	}
	if out != nil {
		t.Fatalf("fail-closed must return no partial set, got %v", out)
	}
}
