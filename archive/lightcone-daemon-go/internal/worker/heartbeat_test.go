package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"
)

// openChannelDB opens a fresh channel sqlite with the full DDL applied
// under t.TempDir(). Shared between heartbeat + runtime tests.
func openChannelDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// acquireLock seeds a worker_locks row so heartbeat tests start in the
// "we already own the lease" state.
func acquireLock(t *testing.T, db *sql.DB, agentID, workerID string, now int64) *supervisor.Lock {
	t.Helper()
	lock, err := supervisor.Acquire(context.Background(), db, agentID, workerID, 60, func() int64 { return now })
	if err != nil {
		t.Fatalf("supervisor.Acquire: %v", err)
	}
	return lock
}

// RunHeartbeat issues UPDATE on every tick — verify by extending the
// lease, waiting for a tick, and reading the row.
func TestRunHeartbeat_ExtendsLeaseOnTick(t *testing.T) {
	db := openChannelDB(t)
	lock := acquireLock(t, db, "alice", "w1", 1700000000)

	// Heartbeat advances the clock so the UPDATE writes
	// lease_expires_at = now + LeaseTTL = 1700000005 + 60 = 1700000065.
	now := int64(1700000005)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resCh := make(chan HeartbeatResult, 1)
	go func() {
		resCh <- RunHeartbeat(ctx, HeartbeatConfig{
			DB:           db,
			AgentID:      "alice",
			WorkerID:     "w1",
			FencingToken: lock.FencingToken,
			LeaseTTL:     60,
			Interval:     10 * time.Millisecond,
			Now:          func() int64 { return now },
		}, cancel)
	}()

	// Wait long enough for at least one tick.
	time.Sleep(30 * time.Millisecond)
	got, err := supervisor.Get(context.Background(), db, "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LeaseExpiresAt < 1700000065 {
		t.Errorf("lease not extended: %d", got.LeaseExpiresAt)
	}

	cancel()
	<-resCh
}

// When another worker steals the lock (bump fencing_token), the next
// heartbeat tick MUST observe ErrFencingStale and self-destruct
// (cancel the runtime ctx, return Stale=true).
func TestRunHeartbeat_StaleTriggersCancel(t *testing.T) {
	db := openChannelDB(t)
	lock := acquireLock(t, db, "alice", "w1", 1700000000)

	// Steal: directly increment fencing_token in sqlite so the next
	// heartbeat CAS misses.
	_, err := db.ExecContext(context.Background(),
		`UPDATE worker_locks
		    SET worker_id = ?, fencing_token = ?
		  WHERE agent_id = ?`,
		"w2", lock.FencingToken+1, "alice",
	)
	if err != nil {
		t.Fatalf("steal update: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res := RunHeartbeat(ctx, HeartbeatConfig{
		DB:           db,
		AgentID:      "alice",
		WorkerID:     "w1",
		FencingToken: lock.FencingToken,
		LeaseTTL:     60,
		Interval:     5 * time.Millisecond,
		Now:          func() int64 { return 1700000010 },
	}, cancel)

	if !res.Stale {
		t.Fatalf("expected Stale=true, got %+v", res)
	}
	if ctx.Err() == nil {
		t.Error("expected runtime ctx to be cancelled")
	}
}

// When the lock row vanishes mid-run, the heartbeat reports Missing.
func TestRunHeartbeat_MissingTriggersCancel(t *testing.T) {
	db := openChannelDB(t)
	lock := acquireLock(t, db, "alice", "w1", 1700000000)

	if err := supervisor.Release(context.Background(), db, "alice", "w1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res := RunHeartbeat(ctx, HeartbeatConfig{
		DB:           db,
		AgentID:      "alice",
		WorkerID:     "w1",
		FencingToken: lock.FencingToken,
		LeaseTTL:     60,
		Interval:     5 * time.Millisecond,
		Now:          func() int64 { return 1700000010 },
	}, cancel)

	if !res.Missing {
		t.Fatalf("expected Missing=true, got %+v", res)
	}
	if ctx.Err() == nil {
		t.Error("expected runtime ctx to be cancelled")
	}
}

// ctx cancellation exits the loop with a clean (no flags) result.
func TestRunHeartbeat_CtxCancelExitsClean(t *testing.T) {
	db := openChannelDB(t)
	lock := acquireLock(t, db, "alice", "w1", 1700000000)

	ctx, cancel := context.WithCancel(context.Background())

	resCh := make(chan HeartbeatResult, 1)
	go func() {
		resCh <- RunHeartbeat(ctx, HeartbeatConfig{
			DB:           db,
			AgentID:      "alice",
			WorkerID:     "w1",
			FencingToken: lock.FencingToken,
			LeaseTTL:     60,
			Interval:     10 * time.Second, // ensure ctx wins
			Now:          func() int64 { return 1700000010 },
		}, cancel)
	}()
	cancel()
	res := <-resCh
	if res.Stale || res.Missing {
		t.Fatalf("ctx cancel should be clean: %+v", res)
	}
}

// validateHeartbeatConfig rejects bad input via wrapped errors. We use
// these failure paths in the production runtime so a misconfigured
// caller fails before the goroutine starts ticking.
func TestRunHeartbeat_ValidatesInput(t *testing.T) {
	db := openChannelDB(t)

	cases := []struct {
		name string
		cfg  HeartbeatConfig
	}{
		{"nil db", HeartbeatConfig{AgentID: "a", WorkerID: "w", FencingToken: 1, LeaseTTL: 60}},
		{"empty agent_id", HeartbeatConfig{DB: db, WorkerID: "w", FencingToken: 1, LeaseTTL: 60}},
		{"empty worker_id", HeartbeatConfig{DB: db, AgentID: "a", FencingToken: 1, LeaseTTL: 60}},
		{"zero fencing_token", HeartbeatConfig{DB: db, AgentID: "a", WorkerID: "w", LeaseTTL: 60}},
		{"zero lease_ttl", HeartbeatConfig{DB: db, AgentID: "a", WorkerID: "w", FencingToken: 1}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			res := RunHeartbeat(ctx, tc.cfg, cancel)
			if res.Err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}
}
