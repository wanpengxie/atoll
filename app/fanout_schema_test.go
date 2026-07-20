package app

import (
	"path/filepath"
	"testing"
)

func TestFanoutJobSchemaStrictReopenAndDedupShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.sqlite")
	for pass := 0; pass < 2; pass++ {
		p, err := OpenProcessDB(path, pass == 0)
		if err != nil {
			t.Fatalf("OpenProcessDB pass %d: %v", pass+1, err)
		}
		db := p.DB
		if pass == 0 {
			// Each authoritative change owns a distinct stable base ref.
			for i := 0; i < 2; i++ {
				if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
					(base_ref,decl_id,op,initiator,created_at)
					VALUES (printf('fo:test:%d',?),'decl-a','restart','user-a',1)`, i); err != nil {
					t.Fatalf("restart insert %d: %v", i+1, err)
				}
			}
			if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
				(base_ref,decl_id,op,initiator,created_at)
				VALUES ('fo:test:delete','decl-a','delete','user-a',1)`); err != nil {
				t.Fatalf("delete insert: %v", err)
			}
			if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
				(base_ref,decl_id,op,initiator,created_at)
				VALUES ('fo:test:delete','decl-other','delete','user-a',2)`); err == nil {
				t.Fatal("duplicate base ref was not rejected")
			}
			if _, err := db.Exec(`INSERT INTO daemon_revoke_jobs
				(base_ref,daemon_id,initiator,created_at)
				VALUES ('fo:test:daemon','daemon-a','user-a',1)`); err != nil {
				t.Fatalf("daemon job insert: %v", err)
			}
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close pass %d: %v", pass+1, err)
		}
	}
}
