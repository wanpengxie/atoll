package app

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func fanoutTestApp(t *testing.T) *App {
	t.Helper()
	return newBareAppForTest(t)
}

func TestFanoutClaimCrashReplayCountsAttempt(t *testing.T) {
	a := fanoutTestApp(t)
	if _, err := a.db.Exec(`INSERT INTO decl_fanout_jobs(base_ref,decl_id,op,initiator,created_at) VALUES ('fo:claim','d','restart','u',1)`); err != nil {
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
	_, _ = a.db.Exec(`PRAGMA ignore_check_constraints=ON`)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(base_ref,decl_id,op,initiator,created_at) VALUES ('fo:bad','bad','invalid','u',1)`)
	_, _ = a.db.Exec(`PRAGMA ignore_check_constraints=OFF`)
	_, _ = a.db.Exec(`INSERT INTO decl_fanout_jobs(base_ref,decl_id,op,initiator,created_at) VALUES ('fo:good','good','restart','u',2)`)
	_, _ = a.db.Exec(`INSERT INTO daemon_revoke_jobs(base_ref,daemon_id,initiator,created_at) VALUES ('fo:box','box','u',1)`)
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
		`INSERT INTO decl_fanout_jobs(base_ref,decl_id,op,initiator,created_at) VALUES ('fo:retry','retry','restart','u',1)`,
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

func TestFanoutUnavailableChannelDoesNotBlockLaterChannelAndReplayIsExact(t *testing.T) {
	a := fanoutTestApp(t)
	ctx := context.Background()
	goodID := channel.ID("b-good")
	badID := channel.ID("a-bad")
	openTestChannelForTest(t, a, goodID, nil)
	for _, stmt := range []string{
		`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('a-bad','bad','group',1,NULL)`,
		`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('b-good','good','group',1,NULL)`,
		`INSERT INTO decl_fanout_jobs(base_ref,decl_id,op,initiator,created_at) VALUES ('fo:aggregate','decl-gone','delete','owner',1)`,
	} {
		if _, err := a.db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	w := newFanoutWorker(a)
	w.drain()
	var done any
	var attempt int
	if err := a.db.QueryRowContext(ctx, `SELECT attempt,done_at FROM decl_fanout_jobs WHERE base_ref='fo:aggregate'`).Scan(&attempt, &done); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || done != nil {
		t.Fatalf("first pass attempt=%d done=%v, want pending after unavailable channel", attempt, done)
	}
	completedCount := func() int {
		t.Helper()
		bundle, ok := a.host.Acquire(goodID)
		if !ok {
			t.Fatal("good channel unavailable")
		}
		rows, err := bundle.View().ReadAfterSeq(ctx, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		anchor := channel.RefCorrelation(channel.DerivedFanoutRef("fo:aggregate", goodID))
		count := 0
		for _, row := range rows {
			if row.Envelope.Type == "sysop_completed" && string(row.Envelope.CorrelationID) == anchor {
				count++
			}
		}
		return count
	}
	if got := completedCount(); got != 1 {
		t.Fatalf("good channel completed events after first pass=%d, want 1", got)
	}

	openTestChannelForTest(t, a, badID, nil)
	if _, err := a.db.ExecContext(ctx, `UPDATE decl_fanout_jobs SET next_attempt_at=0 WHERE base_ref='fo:aggregate'`); err != nil {
		t.Fatal(err)
	}
	w.drain()
	if err := a.db.QueryRowContext(ctx, `SELECT attempt,done_at FROM decl_fanout_jobs WHERE base_ref='fo:aggregate'`).Scan(&attempt, &done); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 || done == nil {
		t.Fatalf("replay attempt=%d done=%v, want completed on second pass", attempt, done)
	}
	if got := completedCount(); got != 1 {
		t.Fatalf("good channel completed events after replay=%d, want exactly 1", got)
	}
}
