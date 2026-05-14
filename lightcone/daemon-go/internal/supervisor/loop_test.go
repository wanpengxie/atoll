package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Fake Worker / Spawner used by every loop_test case.
// ---------------------------------------------------------------------------

// fakeWorker simulates a worker process whose Wait blocks until Kill
// or an explicit Crash signal closes the done channel. PID counts up
// per spawn so log lines / assertions can tell instances apart.
type fakeWorker struct {
	pid  int
	done chan struct{}

	mu      sync.Mutex
	killed  bool
	exitErr error
}

func newFakeWorker(pid int) *fakeWorker {
	return &fakeWorker{
		pid:  pid,
		done: make(chan struct{}),
	}
}

func (w *fakeWorker) PID() int { return w.pid }

func (w *fakeWorker) Wait() error {
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.exitErr
}

func (w *fakeWorker) Kill() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.killed {
		return nil
	}
	w.killed = true
	w.exitErr = errors.New("killed")
	close(w.done)
	return nil
}

// Crash simulates the worker process dying on its own (not via Kill).
func (w *fakeWorker) Crash(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.killed {
		return
	}
	w.killed = true
	w.exitErr = err
	close(w.done)
}

// fakeSpawner records every SpawnContext + returns a fakeWorker. The
// optional spawnErr injects a one-shot failure (used by the "release
// after spawn failure" test).
type fakeSpawner struct {
	mu       sync.Mutex
	spawned  []SpawnContext
	workers  []*fakeWorker
	failNext error
}

func (s *fakeSpawner) Spawn(_ context.Context, sc SpawnContext) (Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return nil, err
	}
	w := newFakeWorker(1000 + len(s.workers))
	s.spawned = append(s.spawned, sc)
	s.workers = append(s.workers, w)
	return w, nil
}

func (s *fakeSpawner) LastContext() SpawnContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.spawned) == 0 {
		return SpawnContext{}
	}
	return s.spawned[len(s.spawned)-1]
}

func (s *fakeSpawner) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spawned)
}

func (s *fakeSpawner) Worker(i int) *fakeWorker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[i]
}

// silentLogger discards every event — keeps test output legible.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

// counterID hands out "w1", "w2", … so log lines / assertions stay
// readable. Returns the func and a pointer to the counter.
func counterID() (func() string, *int64) {
	var n int64
	return func() string {
		v := atomic.AddInt64(&n, 1)
		return fmt.Sprintf("w%d", v)
	}, &n
}

// fixedClockPtr exposes a clock-pointer + getter the way tests want
// to advance time deterministically. The backing int64 is updated
// via atomic.* so concurrent readers (the Run goroutine) and the
// test writer don't race under -race.
//
// Tests advance by calling atomic.AddInt64(ptr, delta) or
// atomic.StoreInt64(ptr, value) on the returned pointer.
func fixedClockPtr(start int64) (func() int64, *int64) {
	var now int64
	atomic.StoreInt64(&now, start)
	return func() int64 { return atomic.LoadInt64(&now) }, &now
}

// ---------------------------------------------------------------------------
// 1. Constructor input validation.
// ---------------------------------------------------------------------------

func TestNew_InputValidation(t *testing.T) {
	db := openChannel(t)
	cases := []struct {
		name    string
		mutator func() (*sql.DB, string, string, Spawner)
	}{
		{"nil db", func() (*sql.DB, string, string, Spawner) {
			return nil, "ch", "agent", &fakeSpawner{}
		}},
		{"empty channel", func() (*sql.DB, string, string, Spawner) {
			return db, "", "agent", &fakeSpawner{}
		}},
		{"empty agent", func() (*sql.DB, string, string, Spawner) {
			return db, "ch", "", &fakeSpawner{}
		}},
		{"nil spawner", func() (*sql.DB, string, string, Spawner) {
			return db, "ch", "agent", nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, c, a, s := tc.mutator()
			_, err := New(d, c, a, s, LoopConfig{})
			if !errors.Is(err, ErrInvalidInput) {
				t.Errorf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 2. First tick spawns a worker, populates lock row, hands backlog in
//    the SpawnContext. (Headline "supervisor reaches running state".)
// ---------------------------------------------------------------------------

func TestTick_FirstSpawn_PopulatesLockAndBacklog(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m1", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "m2", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if spawner.Count() != 1 {
		t.Fatalf("spawn count = %d, want 1", spawner.Count())
	}
	sc := spawner.LastContext()
	if sc.WorkerID != "w1" {
		t.Errorf("WorkerID = %q, want w1", sc.WorkerID)
	}
	if sc.FencingToken != 1 {
		t.Errorf("FencingToken = %d, want 1", sc.FencingToken)
	}
	if got := backlogIDs(sc.Backlog); !equalStrings(got, []string{"m1", "m2"}) {
		t.Errorf("Backlog ids = %v, want [m1 m2]", got)
	}

	// worker_locks row matches the spawned worker.
	lock, err := Get(ctx, db, "agent:writer")
	if err != nil {
		t.Fatalf("Get lock: %v", err)
	}
	if lock.WorkerID != "w1" || lock.FencingToken != 1 {
		t.Errorf("lock = %+v, want worker=w1 token=1", lock)
	}
}

// ---------------------------------------------------------------------------
// 3. Crash + restart — fake worker dies; next Tick (after lease
//    expiry) steals lock and spawns w2 with fencing_token=2 + fresh
//    backlog. (Headline acceptance: "worker crash → supervisor spawns
//    new worker".)
// ---------------------------------------------------------------------------

func TestTick_CrashThenRestart_FencingBumped(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m1", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, now := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 1st tick: spawn w1.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	w1 := spawner.Worker(0)

	// Crash w1; Tick before the lease expires sees the live lock
	// (current==nil because the wait goroutine cleared l.current) —
	// supervisor releases the lock row, but does NOT re-spawn this
	// tick.
	w1.Crash(errors.New("synthetic crash"))
	if !waitForCurrentNil(loop, 200*time.Millisecond) {
		t.Fatalf("loop did not clear current worker after crash")
	}

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2 (after crash): %v", err)
	}
	if spawner.Count() != 1 {
		t.Errorf("respawned before lease expired: count=%d", spawner.Count())
	}
	// Lock row was released.
	if _, err := Get(ctx, db, "agent:writer"); !errors.Is(err, ErrLockMissing) {
		t.Errorf("lock still held after release; err=%v", err)
	}

	// 3rd tick: lock missing → acquire fresh (fencing_token bumped
	// to 2 since Acquire only bumps on steal of expired rows. With
	// no row, INSERT writes token=1. The fencing_token=2 case is
	// covered by TestTick_LeaseExpiredHang_StealsAndBumps below).
	atomic.AddInt64(now, 1)
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #3: %v", err)
	}
	if spawner.Count() != 2 {
		t.Fatalf("spawn count = %d, want 2", spawner.Count())
	}
	sc := spawner.LastContext()
	if sc.WorkerID != "w2" {
		t.Errorf("respawn WorkerID = %q, want w2", sc.WorkerID)
	}
	if sc.FencingToken != 1 {
		t.Errorf("respawn FencingToken = %d, want 1 (fresh INSERT path)",
			sc.FencingToken)
	}

	lock, err := Get(ctx, db, "agent:writer")
	if err != nil {
		t.Fatalf("Get lock: %v", err)
	}
	if lock.WorkerID != "w2" {
		t.Errorf("lock owner = %q, want w2", lock.WorkerID)
	}

	// Cleanup — kill the second worker so Wait goroutines complete.
	_ = spawner.Worker(1).Kill()
}

// ---------------------------------------------------------------------------
// 4. Lease expired with a hung worker on another supervisor — Tick
//    steals via CAS and bumps fencing_token. Simulated by inserting
//    a stale lock row directly.
// ---------------------------------------------------------------------------

func TestTick_LeaseExpiredHang_StealsAndBumps(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// One pending backlog row so the post-R2-FIX-4 idle-respawn guard
	// does NOT short-circuit the steal+bump path under test here.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-steal", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	// Pre-write an expired lock row (owned by some long-dead worker
	// from a prior daemon run; lease past, fencing_token=7).
	const startNow = 1_700_000_000
	if _, err := db.ExecContext(ctx,
		`INSERT INTO worker_locks (agent_id, worker_id, fencing_token, lease_expires_at, acquired_at)
		 VALUES (?, ?, ?, ?, ?)`,
		"agent:writer", "dead-worker", 7, startNow-1, startNow-100,
	); err != nil {
		t.Fatalf("seed stale lock: %v", err)
	}

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(startNow)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	sc := spawner.LastContext()
	if sc.WorkerID != "w1" {
		t.Errorf("WorkerID = %q, want w1", sc.WorkerID)
	}
	if sc.FencingToken != 8 {
		t.Errorf("FencingToken = %d, want 8 (steal bumps 7→8)", sc.FencingToken)
	}

	lock, _ := Get(ctx, db, "agent:writer")
	if lock.WorkerID != "w1" || lock.FencingToken != 8 {
		t.Errorf("post-steal lock = %+v, want worker=w1 token=8", lock)
	}

	// Cleanup.
	_ = spawner.Worker(0).Kill()
}

// ---------------------------------------------------------------------------
// 5. Live worker → second Tick is a no-op.
// ---------------------------------------------------------------------------

func TestTick_LiveWorker_NoOpOnSecondTick(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row drives the first spawn past the R2-FIX-4 idle guard;
	// the fake worker keeps Wait() blocking so the second Tick is a
	// pure no-op against a live worker (not the empty-backlog skip).
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-live", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if spawner.Count() != 1 {
		t.Errorf("spawn count = %d, want 1 (second Tick must be no-op while worker alive)", spawner.Count())
	}

	_ = spawner.Worker(0).Kill()
}

// ---------------------------------------------------------------------------
// 6. Lock held by another supervisor — Tick respects the live lease
//    (no spawn, fencing_token unchanged).
// ---------------------------------------------------------------------------

func TestTick_LockHeldByPeer_BackOff(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	// Peer supervisor holds a fresh lease.
	const startNow = 1_700_000_000
	if _, err := db.ExecContext(ctx,
		`INSERT INTO worker_locks (agent_id, worker_id, fencing_token, lease_expires_at, acquired_at)
		 VALUES (?, ?, ?, ?, ?)`,
		"agent:writer", "peer-worker", 3, startNow+60, startNow,
	); err != nil {
		t.Fatalf("seed peer lock: %v", err)
	}

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(startNow + 5)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if spawner.Count() != 0 {
		t.Errorf("spawn count = %d, want 0 (peer holds live lock)", spawner.Count())
	}

	lock, _ := Get(ctx, db, "agent:writer")
	if lock.WorkerID != "peer-worker" || lock.FencingToken != 3 {
		t.Errorf("peer lock corrupted: %+v", lock)
	}
}

// ---------------------------------------------------------------------------
// 7. Spawn failure releases the lock so the next supervisor doesn't
//    have to wait for lease expiry. (Headline failsafe — covers the
//    L2 §1.4.10 "SpawnFailed → release lock" branch.)
// ---------------------------------------------------------------------------

func TestTick_SpawnFails_ReleasesLock(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row drives spawn past the R2-FIX-4 idle guard so we
	// actually exercise the spawn-failure → release branch.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-fail", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{failNext: errors.New("synthetic spawn fail")}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = loop.Tick(ctx)
	if err == nil {
		t.Fatalf("Tick returned nil, want spawn error")
	}
	if _, lockErr := Get(ctx, db, "agent:writer"); !errors.Is(lockErr, ErrLockMissing) {
		t.Errorf("lock not released after spawn fail; err=%v", lockErr)
	}

	// Next Tick: spawner clean now → spawn succeeds with token=1
	// (the freshly-INSERTed first-ever row path, since we Released).
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if spawner.Count() != 1 {
		t.Errorf("spawn count = %d, want 1", spawner.Count())
	}
	if spawner.LastContext().FencingToken != 1 {
		t.Errorf("post-release respawn FencingToken = %d, want 1",
			spawner.LastContext().FencingToken)
	}

	_ = spawner.Worker(0).Kill()
}

// ---------------------------------------------------------------------------
// 7b. FIX-4 codex t90 — worker process alive, but its heartbeat goroutine
//     died so the worker_locks row drifted out from under us. The
//     supervisor MUST detect the lease drift even when l.current is
//     non-nil and reclaim the slot the same tick.
// ---------------------------------------------------------------------------

// 7b.1 Lease lost: a peer supervisor steals the row while we still
//      hold the worker handle. Tick must Stop the orphan + spawn fresh.
func TestTick_AliveButLeaseStolen_ReclaimAndRespawn(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row keeps the first Tick past the R2-FIX-4 idle guard.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-stolen", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, now := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Tick 1: spawn w1.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if spawner.Count() != 1 {
		t.Fatalf("expected 1 spawn after first tick")
	}
	w1 := spawner.Worker(0)

	// Simulate a peer supervisor stealing our lease — overwrite the
	// worker_locks row directly with someone else as owner. The fake
	// worker keeps Wait()'ing on done so loop.current stays non-nil
	// until we Kill it.
	if _, err := db.ExecContext(ctx,
		`UPDATE worker_locks
		    SET worker_id=?, fencing_token=?, lease_expires_at=?, acquired_at=?
		  WHERE agent_id=?`,
		"peer-thief", 99, atomic.LoadInt64(now)+testTTL, atomic.LoadInt64(now), "agent:writer",
	); err != nil {
		t.Fatalf("steal lease: %v", err)
	}

	// Tick 2: alive cur != nil, lock owner == "peer-thief" != "w1" →
	// Stop + clear + lock-not-mine branch (peer holds live lease) →
	// no spawn this tick.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if !w1.killed {
		t.Errorf("expected w1 to be killed after lease drift detection")
	}
	if loop.currentWorker() != nil {
		t.Errorf("expected loop.current to be cleared after lease drift")
	}
	if spawner.Count() != 1 {
		// Peer still owns the row → no spawn. That's correct.
		t.Errorf("unexpected respawn while peer holds the lease: %d", spawner.Count())
	}
}

// 7b.2 Lease expired: our heartbeat goroutine died, the worker row
//      passed its TTL. cur is still non-nil. Tick must reclaim and
//      respawn the same iteration.
func TestTick_AliveButLeaseExpired_StealAndRespawn(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row keeps both Ticks past the R2-FIX-4 idle guard. The
	// fake worker never consumes the row (it just blocks on Wait), so
	// the row stays eligible across the lease-expiry steal path under
	// test here.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-expired", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, now := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Tick 1: spawn w1.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	w1 := spawner.Worker(0)

	// Heartbeat goroutine "dies": the worker_locks row is never
	// extended, so when we advance the clock past testTTL the row is
	// expired even though l.current is still non-nil (the OS-level
	// process is still running its echo loop; only the heartbeat
	// goroutine died).
	atomic.AddInt64(now, testTTL+5)

	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if !w1.killed {
		t.Errorf("expected w1 stopped after lease expiry detection")
	}
	if spawner.Count() != 2 {
		t.Fatalf("expected immediate respawn within the same tick: count=%d", spawner.Count())
	}
	sc := spawner.LastContext()
	if sc.WorkerID != "w2" {
		t.Errorf("respawn WorkerID = %q, want w2", sc.WorkerID)
	}
	if sc.FencingToken != 2 {
		t.Errorf("respawn FencingToken = %d, want 2 (steal bumps token)", sc.FencingToken)
	}

	// Cleanup the new spawn.
	_ = spawner.Worker(1).Kill()
}

// ---------------------------------------------------------------------------
// 7c. T110 / R2-FIX-4 — idle-respawn guard. Empty backlog + no external
//     trigger MUST NOT spawn a worker, and consecutive Ticks must stay
//     idempotent (count remains 0, lock stays missing). Without the
//     guard the worker would boot, detect no-trigger, Release the lock,
//     and the next Tick (woken by exitCh) would spawn another noop —
//     infinite loop, one worker startup overhead per cycle.
// ---------------------------------------------------------------------------

func TestTick_NoBacklogNoTrigger_DoesNotSpawn(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	// Cursor row present so BacklogScan can join; but no messages → empty
	// backlog → idle-respawn guard MUST short-circuit.
	seedCursor(t, ctx, db, "agent:writer", 0)

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Two consecutive Ticks — the guard MUST hold across both.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}

	if spawner.Count() != 0 {
		t.Errorf("spawn count = %d, want 0 (idle-respawn guard must skip empty-backlog tick)",
			spawner.Count())
	}
	// Lock row MUST be released — leaving an orphan would block peer
	// supervisors for LeaseTTL seconds. ErrLockMissing is the success
	// signal here.
	if _, err := Get(ctx, db, "agent:writer"); !errors.Is(err, ErrLockMissing) {
		t.Errorf("worker_locks row not released after idle guard; err=%v", err)
	}
	if loop.currentWorker() != nil {
		t.Errorf("loop.current should stay nil after idle tick")
	}
}

// TestTick_BacklogArrivesAfterIdle_NextTickSpawns proves the guard is
// purely conditional: once a backlog row lands, the very next Tick
// MUST spawn (with the trigger populated from the backlog row).
// Guards against an over-eager fix that, say, caches "idle" across
// Ticks or forgets to bump the fencing_token correctly after a
// release-and-return cycle.
func TestTick_BacklogArrivesAfterIdle_NextTickSpawns(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 1 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Tick 1 — idle, no spawn.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if spawner.Count() != 0 {
		t.Fatalf("Tick #1 spawned despite empty backlog: count=%d", spawner.Count())
	}

	// A new message lands addressed to us — simulate the external
	// trigger (future_scheduler dispatch / RPC ingest / peer message
	// all land here through `messages`).
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-late", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	// Tick 2 — backlog non-empty → spawn fires.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if spawner.Count() != 1 {
		t.Fatalf("Tick #2 should have spawned: count=%d", spawner.Count())
	}
	sc := spawner.LastContext()
	if sc.Trigger.MsgID != "m-late" {
		t.Errorf("trigger msg id = %q, want m-late", sc.Trigger.MsgID)
	}
	if got := backlogIDs(sc.Backlog); !equalStrings(got, []string{"m-late"}) {
		t.Errorf("backlog = %v, want [m-late]", got)
	}

	_ = spawner.Worker(0).Kill()
}

// ---------------------------------------------------------------------------
// 8. Run drives Tick on Period intervals and shuts down cleanly on
//    context cancellation. Uses a sub-millisecond period + a fake
//    spawner that records each spawn.
// ---------------------------------------------------------------------------

func TestRun_HonoursContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row drives the first Run-tick past the R2-FIX-4 idle
	// guard so cancellation has a live worker to tear down.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-cancel", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, _ := fixedClockPtr(1_700_000_000)
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 5 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- loop.Run(ctx)
	}()

	// Wait for the first spawn to land (immediate first tick).
	deadline := time.After(500 * time.Millisecond)
	for spawner.Count() == 0 {
		select {
		case <-deadline:
			t.Fatalf("first spawn never happened")
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Cancel — Run MUST return promptly (kill kills the fake worker).
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Run did not exit after context cancel")
	}
}

// ---------------------------------------------------------------------------
// 9. End-to-end crash + immediate respawn via OS-level exit hook.
//    Uses Run() (not Tick) to exercise the exitCh wake-up path.
// ---------------------------------------------------------------------------

func TestRun_CrashTriggersImmediateRespawn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)
	// Backlog row keeps both spawns past the R2-FIX-4 idle guard. The
	// fake worker never consumes the row, so it remains eligible for
	// the second spawn after w1 crashes.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m-crash", senderID: "agent:peer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
	})

	spawner := &fakeSpawner{}
	idFn, _ := counterID()
	clk, now := fixedClockPtr(1_700_000_000)

	// Period kept "long" so we KNOW the second spawn was driven by
	// the exit hook, not the ticker — if it fires within Period/4
	// that's the OS-hook path. 100ms gives generous slack.
	loop, err := New(db, "ch", "agent:writer", spawner, LoopConfig{
		Period: 500 * time.Millisecond, LeaseTTL: testTTL,
		Now: clk, NewWorkerID: idFn, Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = loop.Run(ctx) }()

	// Wait for w1 to land.
	if !waitForCount(spawner, 1, 500*time.Millisecond) {
		t.Fatalf("first spawn never happened")
	}
	w1 := spawner.Worker(0)

	// Crash w1 then advance the clock past the lease. Without the OS
	// hook the next spawn would wait 500ms; with the hook it fires
	// well within that.
	w1.Crash(errors.New("synthetic crash"))
	atomic.AddInt64(now, testTTL+1) // advance past lease expiry

	if !waitForCount(spawner, 2, 250*time.Millisecond) {
		t.Fatalf("respawn did not happen via exit hook within 250ms")
	}

	// Lock row now owned by w2.
	lock, _ := Get(ctx, db, "agent:writer")
	if lock == nil || lock.WorkerID != "w2" {
		t.Errorf("post-respawn lock = %+v, want owner=w2", lock)
	}

	cancel()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// waitForCurrentNil polls until the loop's currentWorker test seam
// reports nil (the wait goroutine cleared it post-exit) or until the
// timeout fires.
func waitForCurrentNil(l *Loop, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if l.currentWorker() == nil {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return l.currentWorker() == nil
}

func waitForCount(s *fakeSpawner, n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.Count() >= n {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return s.Count() >= n
}
