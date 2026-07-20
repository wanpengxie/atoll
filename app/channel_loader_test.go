package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew_ExistingChannelDirectoryRowRequiresExistingDB(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x','x',1)`,
		`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('c','c','group',1,NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	if _, err := New(Config{DB: db, ChannelDBDir: dir}); err == nil {
		t.Fatal("New succeeded with a directory row pointing at a missing channel DB")
	}
	if _, err := os.Stat(filepath.Join(dir, "c.db")); !os.IsNotExist(err) {
		t.Fatalf("startup created replacement channel path: stat err=%v", err)
	}
}
