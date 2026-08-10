package app_test

import (
	"path/filepath"
	"testing"
)

// Declaration ownership is a principal string, not a users-table species FK.
// The built-in space declaration is therefore allowed to belong to "system".
func TestDeclarationOwnerDoesNotRequireUserRow(t *testing.T) {
	db, err := openTestAppDB(t, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM actor_decls WHERE id='space-tool' AND owner='system'`).Scan(&n); err != nil {
		t.Fatalf("count declaration: %v", err)
	}
	if n != 1 {
		t.Fatalf("system-owned declaration missing: got %d", n)
	}
}
