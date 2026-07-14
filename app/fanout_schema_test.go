package app

import (
	"path/filepath"
	"testing"
)

func TestFanoutJobSchemaIdempotenceAndDedupShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.sqlite")
	for pass := 0; pass < 2; pass++ {
		db, err := OpenDB(path)
		if err != nil {
			t.Fatalf("OpenDB pass %d: %v", pass+1, err)
		}
		if pass == 0 {
			// Restart is deliberately one job per request.
			for i := 0; i < 2; i++ {
				if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
					(decl_id,op,initiator,targets_json,created_at)
					VALUES ('decl-a','restart','user-a','[]',1)`); err != nil {
					t.Fatalf("restart insert %d: %v", i+1, err)
				}
			}
			if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
				(decl_id,op,initiator,targets_json,created_at)
				VALUES ('decl-a','delete','user-a','[]',1)`); err != nil {
				t.Fatalf("delete insert: %v", err)
			}
			if _, err := db.Exec(`INSERT INTO decl_fanout_jobs
				(decl_id,op,initiator,targets_json,created_at)
				VALUES ('decl-a','delete','user-a','[]',2)`); err == nil {
				t.Fatal("second pending delete was not deduplicated")
			}
			if _, err := db.Exec(`INSERT INTO daemon_revoke_jobs
				(daemon_id,op,targets_json,created_at)
				VALUES ('daemon-a','detach','[{"channel_id":"ch-a"}]',1)`); err != nil {
				t.Fatalf("daemon job insert: %v", err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close pass %d: %v", pass+1, err)
		}
	}
}
