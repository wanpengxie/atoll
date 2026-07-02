package app_test

import (
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/app"
)

// TestChannelActorsCascadeOnDelete pins that deleting a channel also removes its
// channel_actors composition rows — i.e. ON DELETE CASCADE actually fires, which
// only happens when the app DB has PRAGMA foreign_keys=ON. Without it, deleted
// channels leak orphan composition rows and the canonical set drifts.
func TestChannelActorsCascadeOnDelete(t *testing.T) {
	db, err := app.OpenDB(filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Seed a valid FK chain: user → workspace → channel → channel_actors row.
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password, created_at) VALUES ('u1','a@b.c','x',0)`)
	mustExec(`INSERT INTO workspaces (id, owner_id, name, created_at) VALUES ('w1','u1','ws',0)`)
	mustExec(`INSERT INTO channels (id, workspace_id, name, type, db_path, default_agent, created_at)
		VALUES ('c1','w1','ch','group','/tmp/x.db','agent:boost',0)`)
	mustExec(`INSERT INTO channel_actors (channel_id, instance_id, class) VALUES ('c1','agent:boost','go-kimi')`)

	// Sanity: the composition row exists.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_actors WHERE channel_id='c1'`).Scan(&n); err != nil {
		t.Fatalf("count before: %v", err)
	}
	if n != 1 {
		t.Fatalf("composition row missing before delete: got %d", n)
	}

	// Delete the channel — its channel_actors rows must cascade away.
	mustExec(`DELETE FROM channels WHERE id='c1'`)

	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_actors WHERE channel_id='c1'`).Scan(&n); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if n != 0 {
		t.Fatalf("ON DELETE CASCADE did not fire — %d orphan channel_actors rows remain (foreign_keys not enabled?)", n)
	}
}
