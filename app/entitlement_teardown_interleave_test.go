package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestDetachHomeReleasesAppLock proves the production detach primitive returns a
// handle with a.mu already released. The caller deliberately postpones Home.Close;
// EntitlementSnapshot must still enumerate the directory and report the detached
// channel as temporarily failed rather than blocking behind the app lock.
func TestDetachHomeReleasesAppLock(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{DB: db, ChannelDBDir: filepath.Join(dir, "channels")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	const principal = "detach-user"
	const wsID = "detach-workspace"
	const chID channel.ID = "detach-channel"
	if _, err := db.Exec(`INSERT INTO users(id,email,password,created_at) VALUES (?,?,?,?)`, principal, "detach@example.com", "x", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(id,owner_id,name,created_at) VALUES (?,?,?,?)`, wsID, principal, "w", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_members(workspace_id,user_id,role) VALUES (?,?,?)`, wsID, principal, "member"); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "channel.sqlite")
	if _, err := db.Exec(`INSERT INTO channels(id,workspace_id,name,type,db_path,created_at) VALUES (?,?,?,?,?,?)`, chID, wsID, "c", "group", dbPath, 1); err != nil {
		t.Fatal(err)
	}
	h, err := a.createHome(chID, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	detached := a.detachHome(chID)
	if detached != h {
		t.Fatal("detachHome returned the wrong Home handle")
	}
	defer detached.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routes, failed, err := a.EntitlementSnapshot(ctx, principal)
	if err != nil {
		t.Fatalf("EntitlementSnapshot after detach: %v", err)
	}
	if len(routes) != 0 || len(failed) != 1 || failed[0] != chID {
		t.Fatalf("snapshot after detach = routes %v failed %v, want no routes and [%s]", routes, failed, chID)
	}
}

func TestEntitlementSnapshotHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{DB: db, ChannelDBDir: filepath.Join(dir, "channels")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := a.EntitlementSnapshot(ctx, "nobody"); !errors.Is(err, context.Canceled) {
		t.Fatalf("EntitlementSnapshot canceled error = %v, want context.Canceled", err)
	}
}
