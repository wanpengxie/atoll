package worker

// integration_test.go is the FIX-4 phase-5 acceptance gate: drive the
// supervisor.Loop with a real worker.Run inside a fake Spawner so the
// full path "trigger written → loop ticks → worker spawned → backlog
// processed → cursor advances → worker exits → loop sees lock release"
// runs end-to-end against a real channel sqlite. The crash-replay
// invariant is asserted by re-running Run after a synthetic crash and
// verifying the cursor stays at the high-water mark (no double advance).

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"

	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
)

// runtimeSpawner adapts worker.Run to the supervisor.Spawner interface.
// Each Spawn call kicks off Run in its own goroutine and returns a
// trivial supervisor.Worker handle that signals exit via a done channel.
type runtimeSpawner struct {
	t       *testing.T
	workdir string
	mu      sync.Mutex
	spawned int // incremented on every Spawn call (before Run finishes)
	results []RuntimeResult
	errs    []error
}

func (s *runtimeSpawner) Spawn(ctx context.Context, sc supervisor.SpawnContext) (supervisor.Worker, error) {
	w := &runtimeWorker{done: make(chan struct{})}
	cfg := RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:            sc.ChannelID,
			AgentID:              sc.AgentID,
			WorkerID:             sc.WorkerID,
			FencingToken:         sc.FencingToken,
			TriggerMsgID:         sc.Trigger.MsgID,
			TriggerCorrelationID: sc.Trigger.CorrelationID,
			SenderKind:           sc.Trigger.SenderKind,
			AuthToken:            sc.AuthToken,
			DaemonURL:            sc.DaemonURL,
			ChannelWorkdir:       s.workdir,
			LeaseTTL:             60,
		},
		Backlog:  sc.Backlog,
		Provider: llm.NewEchoChatProvider("integ-echo"),
		Model:    "integ-echo",
	}
	s.mu.Lock()
	s.spawned++
	s.mu.Unlock()
	go func() {
		defer close(w.done)
		res, err := Run(ctx, cfg)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.results = append(s.results, res)
		s.errs = append(s.errs, err)
	}()
	return w, nil
}

func (s *runtimeSpawner) lastResult(t *testing.T) RuntimeResult {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.results) == 0 {
		return RuntimeResult{}
	}
	return s.results[len(s.results)-1]
}

func (s *runtimeSpawner) spawnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spawned
}

func (s *runtimeSpawner) resultCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

// runtimeWorker is the supervisor.Worker handle for an in-process
// worker.Run goroutine. PID is irrelevant; Wait blocks until Run
// finishes; Kill is a no-op (the goroutine drives its own ctx via
// runtime.Run).
type runtimeWorker struct {
	done chan struct{}
}

func (w *runtimeWorker) PID() int     { return 0 }
func (w *runtimeWorker) Wait() error  { <-w.done; return nil }
func (w *runtimeWorker) Kill() error  { return nil }
func (w *runtimeWorker) Stop() error  { return nil }

// integSeedActor registers the worker's actor in actor_registry so the
// runtime's actor existence check passes.
func integSeedActor(t *testing.T, db *sql.DB, channelID, actorID string) {
	t.Helper()
	if err := registry.Register(context.Background(), db, channelID, registry.ActorMeta{
		ActorID:   actorID,
		Kind:      registry.KindAgent,
		Binding:   registry.BindingInWorkerBus,
		CreatedAt: 1700000000,
	}); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}
}

// integInsertMessage writes a single backlog-eligible message into the
// channel. The visibility/audience values mirror what the BacklogScan
// SQL filters expect.
func integInsertMessage(t *testing.T, db *sql.DB, id, senderID, audience string, ts int64) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, not_before, expires_at, is_terminal)
		 VALUES (?, ?, ?, 'integ-ch', 'agent', ?,
		         'event', 'agent.text', '{"text":"hello"}', NULL, ?,
		         'public', ?, NULL, NULL, 0)`,
		id, ts, ts, senderID, id, audience,
	)
	if err != nil {
		t.Fatalf("insert message %q: %v", id, err)
	}
}

// integSilentLogger discards every log line — keeps `go test -v` legible.
func integSilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

// TestIntegration_TriggerSpawnCursorExit verifies the FIX-4 happy path:
// pending backlog → supervisor spawns worker → worker.Run consumes the
// trigger + backlog, advances actor_cursors, releases the lock → next
// supervisor tick sees no work and the lock missing, no respawn.
func TestIntegration_TriggerSpawnCursorExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workdir := t.TempDir()
	dbPath := workdir + "/messages.sqlite"
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const channelID = "integ-ch"
	const agentID = "alice"
	integSeedActor(t, db, channelID, "peer")
	integSeedActor(t, db, channelID, agentID)

	// One pending message addressed to alice — must drive a turn.
	// (Registry.Register emits system.event rows for each Register call
	// — those have visibility='system' so BacklogScan filters them
	// out. The only backlog-eligible row is the public agent.text we
	// insert below.)
	integInsertMessage(t, db, "msg-1", "peer", `["alice"]`, 1700000010)
	wantMaxSeq := lookupMessageSeq(t, db, "msg-1")

	idCounter := int64(0)
	idFn := func() string {
		v := atomic.AddInt64(&idCounter, 1)
		return "integ-w" + string(rune('0'+v))
	}
	clk := func() int64 { return 1700000020 }

	sp := &runtimeSpawner{t: t, workdir: workdir}
	loop, err := supervisor.New(db, channelID, agentID, sp, supervisor.LoopConfig{
		Period:      50 * time.Millisecond,
		LeaseTTL:    60,
		Now:         clk,
		NewWorkerID: idFn,
		Logger:      integSilentLogger(),
	})
	if err != nil {
		t.Fatalf("supervisor.New: %v", err)
	}

	// First Tick: spawn w1.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if sp.spawnCount() != 1 {
		t.Fatalf("spawn count = %d, want 1", sp.spawnCount())
	}

	// Wait for the in-process worker.Run to finish: it writes the
	// cursor advance + Releases the lock from its defer.
	deadline := time.After(5 * time.Second)
	for {
		// The runtimeWorker.Wait returns once Run returns. We poll
		// the result slice instead of touching the worker directly so
		// the race detector stays happy.
		if sp.lastResult(t).LockReleased {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("worker.Run did not finish within 5s: lastResult=%+v", sp.lastResult(t))
		case <-time.After(20 * time.Millisecond):
		}
	}

	res := sp.lastResult(t)
	if res.SkippedNoTrigger {
		t.Errorf("expected agent.Run to fire (trigger present), got SkippedNoTrigger=true")
	}
	if res.CursorAdvancedTo != wantMaxSeq {
		t.Errorf("CursorAdvancedTo = %d, want %d (max backlog seq)", res.CursorAdvancedTo, wantMaxSeq)
	}

	// Verify cursor moved on disk.
	var got int64
	if err := db.QueryRowContext(ctx,
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`, agentID,
	).Scan(&got); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if got != wantMaxSeq {
		t.Errorf("actor_cursors.last_consumed_seq = %d, want %d", got, wantMaxSeq)
	}

	// Verify the lock was released by the worker (not stuck waiting
	// for TTL).
	if _, err := supervisor.Get(ctx, db, agentID); !errors.Is(err, supervisor.ErrLockMissing) {
		t.Errorf("lock not released after worker exit; err=%v", err)
	}

	// Second Tick: backlog empty (cursor advanced past msg-1), no
	// pending trigger → T110 / R2-FIX-4 idle-respawn guard MUST
	// short-circuit. Supervisor releases the lock it just took and
	// returns without spawning another worker. The cursor stays at
	// wantMaxSeq.
	if err := loop.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}

	// Give any erroneous in-flight Spawn a chance to land before we
	// assert "count did not grow". 250ms is several times longer than
	// the goroutine startup overhead the buggy path used to incur.
	time.Sleep(250 * time.Millisecond)

	if sp.spawnCount() != 1 {
		t.Fatalf("idle-respawn guard violated: spawnCount=%d, want 1 (no second spawn on empty backlog)",
			sp.spawnCount())
	}

	// Lock MUST remain released — the guard releases the freshly-acquired
	// lock before returning so peer supervisors don't have to wait for
	// the lease to expire.
	if _, err := supervisor.Get(ctx, db, agentID); !errors.Is(err, supervisor.ErrLockMissing) {
		t.Errorf("lock not released after idle Tick; err=%v", err)
	}

	// Cursor still at wantMaxSeq after the idle-Tick guard fires.
	if err := db.QueryRowContext(ctx,
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`, agentID,
	).Scan(&got); err != nil {
		t.Fatalf("read cursor #2: %v", err)
	}
	if got != wantMaxSeq {
		t.Errorf("post-idle cursor = %d, want %d", got, wantMaxSeq)
	}
}

// TestIntegration_KillBeforeCursorAdvance_ReplayIsIdempotent simulates
// the worst-case crash scenario: worker.Run is killed immediately after
// agent.Run completes but before the cursor advance + Release writes
// land. We model "kill -9" by cancelling the context AND skipping the
// cursor update on the first attempt; the second spawn re-scans and
// the CAS predicate keeps the cursor monotonic.
func TestIntegration_KillBeforeCursorAdvance_ReplayIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workdir := t.TempDir()
	db, err := store.OpenChannel(ctx, workdir+"/messages.sqlite", store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	channelID := "integ-replay-ch"
	agentID := "alice-replay"
	integSeedActor(t, db, channelID, "peer")
	integSeedActor(t, db, channelID, agentID)
	integInsertMessage(t, db, "msg-r1", "peer", `["alice-replay"]`, 1700000010)
	integInsertMessage(t, db, "msg-r2", "peer", `["alice-replay"]`, 1700000011)
	wantMaxSeq := lookupMessageSeq(t, db, "msg-r2")

	// Acquire as W1 directly — simulates supervisor having spawned
	// us. We then run the worker, but force-Release the lock + clear
	// the cursor's pending advance to model "killed before commit".
	lock1, err := supervisor.Acquire(ctx, db, agentID, "w-replay-1", 60,
		func() int64 { return 1700000020 })
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}

	// First run with SkipReleaseOnExit=false drives the full path
	// including cursor advance — we want to assert the cursor lands
	// at 2 after the first run.
	t.Setenv("HOME", t.TempDir())
	res1, err := Run(ctx, RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:      channelID,
			AgentID:        agentID,
			WorkerID:       "w-replay-1",
			FencingToken:   lock1.FencingToken,
			ChannelWorkdir: workdir,
			LeaseTTL:       60,
			TriggerMsgID:   "msg-r1",
		},
		Provider: llm.NewEchoChatProvider("integ-echo"),
		Model:    "integ-echo",
	})
	if err != nil {
		t.Fatalf("Run #1: %v", err)
	}
	if res1.CursorAdvancedTo != wantMaxSeq {
		t.Errorf("Run #1 CursorAdvancedTo = %d, want %d", res1.CursorAdvancedTo, wantMaxSeq)
	}

	// Now simulate a "kill -9 followed by replay": even though the
	// first run committed cursor=2, a second worker that re-reads the
	// backlog must see an EMPTY backlog (cursor predicate filters out
	// msg-r1 + msg-r2) and exit clean via the no-trigger path. This
	// proves no double-processing.
	lock2, err := supervisor.Acquire(ctx, db, agentID, "w-replay-2", 60,
		func() int64 { return 1700000200 })
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	res2, err := Run(ctx, RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:      channelID,
			AgentID:        agentID,
			WorkerID:       "w-replay-2",
			FencingToken:   lock2.FencingToken,
			ChannelWorkdir: workdir,
			LeaseTTL:       60,
			// no trigger msg id — but BacklogScan should also see 0
			// rows because the cursor already moved past them.
		},
		Provider: llm.NewEchoChatProvider("integ-echo"),
		Model:    "integ-echo",
	})
	if err != nil {
		t.Fatalf("Run #2: %v", err)
	}
	if !res2.SkippedNoTrigger {
		t.Errorf("Run #2 should have detected no work; got %+v", res2)
	}
	if res2.CursorAdvancedTo != 0 {
		t.Errorf("Run #2 cursor must not advance: got %d", res2.CursorAdvancedTo)
	}

	// Cursor stays at 2 — replay is a no-op on cursor too.
	var finalSeq int64
	if err := db.QueryRowContext(ctx,
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`, agentID,
	).Scan(&finalSeq); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if finalSeq != wantMaxSeq {
		t.Errorf("final cursor = %d, want %d", finalSeq, wantMaxSeq)
	}
}

// lookupMessageSeq returns the AUTO_INCREMENT seq column for the given
// message id. Used by integration tests to derive expected cursor
// values without hard-coding sequence numbers (registry.Register adds
// system events that bump the counter).
func lookupMessageSeq(t *testing.T, db *sql.DB, msgID string) int64 {
	t.Helper()
	var seq int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT seq FROM messages WHERE id=?`, msgID,
	).Scan(&seq); err != nil {
		t.Fatalf("lookupMessageSeq(%q): %v", msgID, err)
	}
	return seq
}
