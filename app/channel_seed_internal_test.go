package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestSeedOpenedChannelRealFailureRollsBack(t *testing.T) {
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

	const principal = "seed-user"
	const wsID = "seed-workspace"
	const chID channel.ID = "seed-channel"
	if _, err := db.Exec(`INSERT INTO users(id,email,password,created_at) VALUES (?,?,?,?)`, principal, "seed@example.com", "x", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(id,owner_id,name,created_at) VALUES (?,?,?,?)`, wsID, principal, "w", 1); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "seed-channel.sqlite")
	h, err := a.createHome(chID, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO channels(id,workspace_id,name,type,db_path,created_at) VALUES (?,?,?,?,?,?)`, chID, wsID, "c", "group", dbPath, 1); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := a.seedOpenedChannel(context.Background(), h, chID, dbPath, principal, 1); err == nil {
		t.Fatal("seeding a closed Home must fail")
	}
	if got := a.getHome(chID); got != nil {
		t.Fatal("failed seed left the Home registered")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM channels WHERE id=?`, chID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("failed seed left the directory row behind")
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("failed seed left channel DB file: %v", err)
	}
}
