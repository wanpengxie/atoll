package app

import (
	"context"
	"testing"
	"time"
)

func fanoutTestApp(t *testing.T) *App {
	t.Helper()
	return newBareAppForTest(t)
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

func TestFanoutDeadLetterDoesNotStarveEitherTable(t *testing.T) {
	a := fanoutTestApp(t)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('bad','restart','u','{',1)`)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('good','restart','u','[]',2)`)
	_, _ = a.db.Exec(`INSERT INTO daemon_revoke_jobs(daemon_id,op,targets_json,created_at) VALUES ('box','delete','[]',1)`)
	w := newFanoutWorker(a)
	w.drain()

	var attempt int
	var done, dead, last any
	if err := a.db.QueryRow(`SELECT attempt,done_at,dead_at,last_error FROM decl_fanout_jobs WHERE decl_id='bad'`).Scan(&attempt, &done, &dead, &last); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || done != nil || dead == nil || last == nil {
		t.Fatalf("dead-letter row attempt=%d done=%v dead=%v last=%v", attempt, done, dead, last)
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

func TestFanoutTransientFailureWaitsForNextAttempt(t *testing.T) {
	a := fanoutTestApp(t)
	now := time.Now().UnixMilli()
	for _, stmt := range []string{
		`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@example.test','x',1)`,
		`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('c','c','group',1,NULL)`,
		`INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES ('retry','restart','u','[{"channel_id":"c","instance_id":"agent:x"}]',1)`,
	} {
		if _, err := a.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	w := newFanoutWorker(a)
	w.drain()

	var attempt int
	var next int64
	var done, dead, last any
	if err := a.db.QueryRow(`SELECT attempt,next_attempt_at,done_at,dead_at,last_error FROM decl_fanout_jobs WHERE decl_id='retry'`).Scan(&attempt, &next, &done, &dead, &last); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || next <= now || done != nil || dead != nil || last == nil {
		t.Fatalf("first failure attempt=%d next=%d done=%v dead=%v last=%v", attempt, next, done, dead, last)
	}

	// Making the row due simulates the next scheduled pass. It receives exactly
	// one more attempt rather than burning the remaining budget in the first pass.
	if _, err := a.db.Exec(`UPDATE decl_fanout_jobs SET next_attempt_at=0 WHERE decl_id='retry'`); err != nil {
		t.Fatal(err)
	}
	w.drain()
	if err := a.db.QueryRow(`SELECT attempt FROM decl_fanout_jobs WHERE decl_id='retry'`).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 {
		t.Fatalf("second scheduled pass attempt=%d, want 2", attempt)
	}
}

func TestFanoutRetryDelaySchedule(t *testing.T) {
	want := []time.Duration{250 * time.Millisecond, time.Second, 4 * time.Second, 16 * time.Second, time.Minute, time.Minute}
	for i, d := range want {
		if got := fanoutRetryDelay(int64(i + 1)); got != d {
			t.Fatalf("attempt %d delay=%v want %v", i+1, got, d)
		}
	}
}
