package app

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/protocol/channel"
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

// TestCompositionSelect_ExcludesSoftDeletedAgent pins the deleted-agent filter (#2):
// a soft-deleted agent (agents.deleted_at set) is NEVER composed, even when a stale
// channel_actors intent row outlived it — the crash-orphan a restart must not rebuild
// (handleDeleteAgent could not reach a closed home to clear the row). A non-agent row
// (boost, no agents join match) and a live agent's row must both survive the filter.
func TestCompositionSelect_ExcludesSoftDeletedAgent(t *testing.T) {
	db, err := OpenDB(":memory:")
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
	// Minimal FK chain: user → workspace → channel.
	mustExec(`INSERT INTO users (id, email, password, display_name, created_at) VALUES ('u1','e','p','n',0)`)
	mustExec(`INSERT INTO workspaces (id, owner_id, name, created_at) VALUES ('w1','u1','ws',0)`)
	mustExec(`INSERT INTO channels (id, workspace_id, name, type, db_path, created_at) VALUES ('c1','w1','ch','group','/tmp/unused.db',0)`)
	// Live agent + its server-placed intent row.
	mustExec(`INSERT INTO agents (id, name, owner, default_looper, created_at, updated_at) VALUES ('live','L','u1','go-kimi',0,0)`)
	mustExec(`INSERT INTO channel_actors (channel_id, instance_id, class, placement) VALUES ('c1','agent:live','go-kimi','server')`)
	// Soft-deleted agent + a SURVIVING (orphan) intent row — the restart hazard.
	mustExec(`INSERT INTO agents (id, name, owner, default_looper, deleted_at, created_at, updated_at) VALUES ('dead','D','u1','go-kimi',123,0,0)`)
	mustExec(`INSERT INTO channel_actors (channel_id, instance_id, class, placement) VALUES ('c1','agent:dead','go-kimi','server')`)
	// A non-agent (boost) row — no agents join match — must survive.
	mustExec(`INSERT INTO channel_actors (channel_id, instance_id, class, placement) VALUES ('c1','agent:boost','go-kimi','server')`)

	a := &App{db: db}
	rows, err := a.serverCompositionRows(channel.ID("c1"))
	if err != nil {
		t.Fatalf("serverCompositionRows: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.instanceID] = true
	}
	if !got["agent:live"] {
		t.Fatal("live agent's composition row was excluded — filter over-reaches")
	}
	if !got["agent:boost"] {
		t.Fatal("non-agent boost row was excluded — filter dropped a row with no agents join match")
	}
	if got["agent:dead"] {
		t.Fatal("soft-deleted agent still composed — the ring would REBUILD it on restart (deleted_at filter broken)")
	}
}
