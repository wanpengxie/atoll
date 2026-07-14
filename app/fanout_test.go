package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func fanoutTestApp(t *testing.T) *App {
	t.Helper()
	db, err := OpenDB(filepath.Join(t.TempDir(), "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &App{db: db, logger: slog.New(slog.DiscardHandler), homes: map[channel.ID]*home.Home{}, daemonLocks: newKeyedLockSet(), declLocks: newKeyedLockSet()}
}

func TestFanoutClaimCrashReplayCountsAttempt(t *testing.T) {
	a := fanoutTestApp(t)
	if _, err := a.db.Exec(`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('d','restart','u','[]',1)`); err != nil {
		t.Fatal(err)
	}
	w1 := newFanoutWorker(a)
	job, ok, err := w1.claim(0)
	if err != nil || !ok || job.attempt != 1 {
		t.Fatalf("first claim = %+v ok=%v err=%v", job, ok, err)
	}
	// Simulate a crash after claim: no apply/complete. A fresh worker must claim
	// the same pending row and count the second attempt.
	w2 := newFanoutWorker(a)
	job, ok, err = w2.claim(0)
	if err != nil || !ok || job.attempt != 2 {
		t.Fatalf("replay claim = %+v ok=%v err=%v", job, ok, err)
	}
}

func TestFanoutPoisonDoesNotStarveEitherTable(t *testing.T) {
	a := fanoutTestApp(t)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('bad','restart','u','{',1)`)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('good','restart','u','[]',2)`)
	_, _ = a.db.Exec(`INSERT INTO daemon_revoke_jobs(daemon_id,op,targets_json,created_at) VALUES ('box','delete','[]',1)`)
	w := newFanoutWorker(a)
	w.drain()

	var attempt int
	var done, last any
	if err := a.db.QueryRow(`SELECT attempt,done_at,last_error FROM decl_fanout_jobs WHERE decl_id='bad'`).Scan(&attempt, &done, &last); err != nil {
		t.Fatal(err)
	}
	if attempt != 5 || done == nil || last == nil {
		t.Fatalf("poison row attempt=%d done=%v last=%v", attempt, done, last)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM decl_fanout_jobs WHERE decl_id='good' AND done_at IS NOT NULL AND attempt=1`,
		`SELECT COUNT(*) FROM daemon_revoke_jobs WHERE daemon_id='box' AND done_at IS NOT NULL AND attempt=1`,
	} {
		var n int
		if err := a.db.QueryRowContext(context.Background(), query).Scan(&n); err != nil || n != 1 {
			t.Fatalf("completed query %q: n=%d err=%v", query, n, err)
		}
	}
}
