package app_test

import (
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/app"
)

// TestDaemonBindingsCascadeOnDelete pins the remaining app-directory FK cascade.
func TestDaemonBindingsCascadeOnDelete(t *testing.T) {
	db, err := app.OpenDB(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Seed a valid FK chain: user → workspace → channel/daemon → binding.
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password, created_at) VALUES ('u1','a@b.c','x',0)`)
	mustExec(`INSERT INTO workspaces (id, owner_id, name, created_at) VALUES ('w1','u1','ws',0)`)
	mustExec(`INSERT INTO channels (id, workspace_id, name, type, db_path, created_at)
		VALUES ('c1','w1','ch','group','/tmp/x.db',0)`)
	mustExec(`INSERT INTO daemons (id,owner_id,name,api_key_hash,created_at) VALUES ('d1','u1','box','k',0)`)
	mustExec(`INSERT INTO daemon_channels (daemon_id,channel_id) VALUES ('d1','c1')`)

	// Sanity: the composition row exists.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM daemon_channels WHERE channel_id='c1'`).Scan(&n); err != nil {
		t.Fatalf("count before: %v", err)
	}
	if n != 1 {
		t.Fatalf("composition row missing before delete: got %d", n)
	}

	// Delete the channel — its binding rows must cascade away.
	mustExec(`DELETE FROM channels WHERE id='c1'`)

	if err := db.QueryRow(`SELECT COUNT(*) FROM daemon_channels WHERE channel_id='c1'`).Scan(&n); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if n != 0 {
		t.Fatalf("ON DELETE CASCADE did not fire — %d orphan daemon bindings remain", n)
	}
}
