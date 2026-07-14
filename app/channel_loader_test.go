package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew_ExistingChannelDirectoryRowRequiresExistingDB(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	missingDir := filepath.Join(dir, "missing-channel-dir")
	missingDB := filepath.Join(missingDir, "channel.sqlite")
	for _, stmt := range []string{
		`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x','x',1)`,
		`INSERT INTO workspaces(id,owner_id,name,created_at) VALUES ('w','u','w',1)`,
		`INSERT INTO channels(id,workspace_id,name,type,db_path,created_at) VALUES ('c','w','c','group','` + missingDB + `',1)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	if _, err := New(Config{DB: db, ChannelDBDir: dir}); err == nil {
		t.Fatal("New succeeded with a directory row pointing at a missing channel DB")
	}
	if _, err := os.Stat(missingDir); !os.IsNotExist(err) {
		t.Fatalf("startup created replacement channel path: stat err=%v", err)
	}
}
