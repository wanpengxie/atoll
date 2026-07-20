package app_test

import (
	"path/filepath"
	"testing"
)

// Declaration ownership is a principal string, not a users-table species FK.
// The built-in realm declaration is therefore allowed to belong to "system".
func TestDeclarationOwnerDoesNotRequireUserRow(t *testing.T) {
	db, err := openTestAppDB(t, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO actor_decls (id,name,owner,default_class,created_at,updated_at,visibility) VALUES ('realm-tool','Realm Tool','system','realm-tool',0,0,'public')`)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM actor_decls WHERE id='realm-tool' AND owner='system'`).Scan(&n); err != nil {
		t.Fatalf("count declaration: %v", err)
	}
	if n != 1 {
		t.Fatalf("system-owned declaration missing: got %d", n)
	}
}
