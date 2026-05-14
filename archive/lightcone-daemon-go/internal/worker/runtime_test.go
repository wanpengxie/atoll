package worker

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"

	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
)

// storeOpenChannelHelper is a small wrapper over store.OpenChannel so
// tests can share the open path without importing store into every
// _test.go file.
func storeOpenChannelHelper(path string) (*sql.DB, error) {
	return store.OpenChannel(context.Background(), path, store.OpenOptions{})
}

// seedActor registers the worker's agent in actor_registry so the
// harness Step 3 (sender identity) check passes when the wire bridge
// emits via in_worker_bus.
func seedActor(t *testing.T, db *sql.DB, channelID, actorID string) {
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

// happy-path: Runtime.Run boots, runs one echo-provider turn, releases
// the lock, returns the assistant reply.
func TestRuntimeRun_HappyPath(t *testing.T) {
	workdir := t.TempDir()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = db.Close() // Runtime.Run opens its own handle from the path.

	channelID := "ch-1"
	agentID := "alice"
	workerID := "w-1"

	// Register the actor + seed the worker_locks row.
	db2, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	seedActor(t, db2, channelID, agentID)
	lock, err := supervisor.Acquire(context.Background(), db2, agentID, workerID, 60,
		func() int64 { return 1700000010 })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = db2.Close()

	tc := TurnCtx{
		ChannelID:      channelID,
		AgentID:        agentID,
		WorkerID:       workerID,
		FencingToken:   lock.FencingToken,
		ChannelWorkdir: workdir,
		LeaseTTL:       60,
		SenderKind:     "agent",
		TriggerMsgID:   "trig-1",
	}
	home := t.TempDir()
	t.Setenv("HOME", home)

	res, err := Run(context.Background(), RuntimeConfig{
		TurnCtx:  tc,
		HomeDir:  home,
		Provider: llm.NewEchoChatProvider("echo-test"),
		Model:    "echo-test",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.AgentReply == "" {
		t.Errorf("expected non-empty reply; res=%+v", res)
	}
	if res.HeartbeatStale {
		t.Errorf("did not expect heartbeat stale")
	}
	if !res.LockReleased {
		t.Errorf("expected lock released on clean exit")
	}

	// turn-ctx.json must exist with the spawn fields.
	if _, statErr := openChannelAt(t, workdir); statErr != nil {
		t.Errorf("channel sqlite gone: %v", statErr)
	}
	verifyTurnCtxFile(t, home, tc)

	// T11 acceptance: every built-in tool actor lands in actor_registry
	// + type_registry rows (idempotent — Run does the EnsureToolActors
	// step before harness deps are built).
	verifyToolActorsInstalled(t, workdir)
}

// verifyToolActorsInstalled checks that the canonical L2 §3.9.4 tool
// actors are present in the channel sqlite after a worker boot.
func verifyToolActorsInstalled(t *testing.T, workdir string) {
	t.Helper()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("reopen channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Pick one canonical entry — fs.read + tool:fs.read. The tools
	// package owns the full list; runtime test only proves the
	// integration happened.
	row := db.QueryRowContext(context.Background(),
		`SELECT 1 FROM actor_registry WHERE actor_id = 'tool:fs.read' AND deregistered_at IS NULL`)
	var present int
	if err := row.Scan(&present); err != nil {
		t.Fatalf("tool:fs.read actor not registered: %v", err)
	}
	row = db.QueryRowContext(context.Background(),
		`SELECT max_pending_ms FROM type_registry WHERE type = 'fs.read'`)
	var maxPending sql.NullInt64
	if err := row.Scan(&maxPending); err != nil {
		t.Fatalf("fs.read type not installed: %v", err)
	}
	if !maxPending.Valid || maxPending.Int64 <= 0 {
		t.Fatalf("fs.read max_pending_ms invalid: %v", maxPending)
	}
}

// SkipAgentRun path proves the boot lifecycle (turn-ctx write +
// heartbeat + release) without invoking the LLM provider — used by
// supervisor integration tests.
func TestRuntimeRun_SkipAgentRunReleasesLock(t *testing.T) {
	workdir := t.TempDir()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	channelID, agentID, workerID := "ch-2", "alice2", "w-2"
	seedActor(t, db, channelID, agentID)
	lock, err := supervisor.Acquire(context.Background(), db, agentID, workerID, 60,
		func() int64 { return 1700000020 })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = db.Close()

	t.Setenv("HOME", t.TempDir())
	res, err := Run(context.Background(), RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:      channelID,
			AgentID:        agentID,
			WorkerID:       workerID,
			FencingToken:   lock.FencingToken,
			ChannelWorkdir: workdir,
			LeaseTTL:       60,
		},
		SkipAgentRun: true,
		Provider:     llm.NewEchoChatProvider(""),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.AgentReply != "" {
		t.Errorf("expected empty reply with SkipAgentRun; got %q", res.AgentReply)
	}
	if !res.LockReleased {
		t.Errorf("expected lock released")
	}
}

// Missing actor row → fail-fast at boot. Surfacing the misconfig early
// is cheaper than a cryptic harness reject later.
func TestRuntimeRun_RejectsMissingActor(t *testing.T) {
	workdir := t.TempDir()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Acquire the lock without registering the actor.
	lock, err := supervisor.Acquire(context.Background(), db, "ghost", "w-3", 60,
		func() int64 { return 1700000030 })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = db.Close()

	t.Setenv("HOME", t.TempDir())
	_, err = Run(context.Background(), RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:      "ch-3",
			AgentID:        "ghost",
			WorkerID:       "w-3",
			FencingToken:   lock.FencingToken,
			ChannelWorkdir: workdir,
			LeaseTTL:       60,
		},
		SkipAgentRun: true,
	})
	if err == nil {
		t.Fatal("expected error for unregistered actor")
	}
}

// Validate() failure path — Run rejects malformed TurnCtx without
// touching sqlite.
func TestRuntimeRun_RejectsBadTurnCtx(t *testing.T) {
	_, err := Run(context.Background(), RuntimeConfig{
		TurnCtx: TurnCtx{ChannelID: "", AgentID: "a"},
	})
	if err == nil {
		t.Fatal("expected validate error")
	}
	if !errors.Is(err, ErrTurnCtxInvalid) {
		t.Errorf("want ErrTurnCtxInvalid wrapping, got %v", err)
	}
}

// TestRuntimeRun_NoTriggerExitsClean verifies the FIX-4 §"backlog 为空且
// trigger 为空时进等待态" branch: when neither a trigger nor any pending
// backlog exists, Run MUST NOT call agent.Run; it logs a no_trigger
// event and exits cleanly so the supervisor stops respawning until a
// fresh trigger arrives.
func TestRuntimeRun_NoTriggerExitsClean(t *testing.T) {
	workdir := t.TempDir()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	channelID, agentID, workerID := "ch-no-trig", "alice-nt", "w-nt"
	seedActor(t, db, channelID, agentID)
	lock, err := supervisor.Acquire(context.Background(), db, agentID, workerID, 60,
		func() int64 { return 1700000040 })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = db.Close()

	t.Setenv("HOME", t.TempDir())
	res, err := Run(context.Background(), RuntimeConfig{
		TurnCtx: TurnCtx{
			ChannelID:      channelID,
			AgentID:        agentID,
			WorkerID:       workerID,
			FencingToken:   lock.FencingToken,
			ChannelWorkdir: workdir,
			LeaseTTL:       60,
			// TriggerMsgID intentionally empty.
		},
		// Backlog explicitly empty so the runtime takes the "scan ourselves"
		// path and finds nothing.
		Backlog:  []supervisor.BacklogMessage{},
		Provider: llm.NewEchoChatProvider("echo-nt"),
		Model:    "echo-nt",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.SkippedNoTrigger {
		t.Errorf("expected SkippedNoTrigger=true, got %+v", res)
	}
	if res.AgentReply != "" {
		t.Errorf("expected empty reply on no-trigger, got %q", res.AgentReply)
	}
	if !res.LockReleased {
		t.Errorf("expected lock released on clean no-trigger exit")
	}
	if res.CursorAdvancedTo != 0 {
		t.Errorf("expected cursor untouched, got %d", res.CursorAdvancedTo)
	}
}

// TestRuntimeRun_AdvancesCursorAfterTurn drives a full turn and
// verifies actor_cursors.last_consumed_seq lands on max(backlog.Seq).
// Re-running Run with the same backlog (a crash + replay scenario)
// MUST be a no-op on the cursor (CAS predicate `< maxSeq`).
func TestRuntimeRun_AdvancesCursorAfterTurn(t *testing.T) {
	workdir := t.TempDir()
	db, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	channelID, agentID, workerID := "ch-cur", "alice-cur", "w-cur"
	seedActor(t, db, channelID, agentID)
	lock, err := supervisor.Acquire(context.Background(), db, agentID, workerID, 60,
		func() int64 { return 1700000050 })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = db.Close()

	// Synthesise an injected backlog. The runtime trusts the slice
	// when cfg.Backlog != nil, so the cursor will advance to the
	// highest Seq we hand in.
	backlog := []supervisor.BacklogMessage{
		{Seq: 7, ID: "msg-7", SenderKind: "agent", SenderID: "peer"},
		{Seq: 9, ID: "msg-9", SenderKind: "agent", SenderID: "peer"},
		{Seq: 12, ID: "msg-12", SenderKind: "agent", SenderID: "peer"},
	}
	tc := TurnCtx{
		ChannelID:      channelID,
		AgentID:        agentID,
		WorkerID:       workerID,
		FencingToken:   lock.FencingToken,
		ChannelWorkdir: workdir,
		LeaseTTL:       60,
		TriggerMsgID:   "msg-7",
	}
	t.Setenv("HOME", t.TempDir())
	res, err := Run(context.Background(), RuntimeConfig{
		TurnCtx:  tc,
		Backlog:  backlog,
		Provider: llm.NewEchoChatProvider("echo-cur"),
		Model:    "echo-cur",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.CursorAdvancedTo != 12 {
		t.Errorf("CursorAdvancedTo = %d, want 12", res.CursorAdvancedTo)
	}
	if res.SkippedNoTrigger {
		t.Errorf("did not expect SkippedNoTrigger when trigger present")
	}

	// Direct read confirms the row landed.
	dbCheck, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	var got int64
	if err := dbCheck.QueryRowContext(context.Background(),
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`, agentID,
	).Scan(&got); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if got != 12 {
		t.Errorf("actor_cursors.last_consumed_seq = %d, want 12", got)
	}

	// Replay path: re-acquire the lock under a fresh worker_id +
	// re-Run with the same backlog. The CAS predicate must reject the
	// second update (cursor not < 12) so the side effect is idempotent.
	lock2, err := supervisor.Acquire(context.Background(), dbCheck, agentID, "w-cur-replay", 60,
		func() int64 { return 1700000200 })
	if err != nil {
		t.Fatalf("acquire replay: %v", err)
	}
	_ = dbCheck.Close()
	tc.WorkerID = "w-cur-replay"
	tc.FencingToken = lock2.FencingToken
	res2, err := Run(context.Background(), RuntimeConfig{
		TurnCtx:  tc,
		Backlog:  backlog,
		Provider: llm.NewEchoChatProvider("echo-cur"),
		Model:    "echo-cur",
	})
	if err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if res2.CursorAdvancedTo != 0 {
		t.Errorf("replay CursorAdvancedTo = %d, want 0 (CAS no-op)", res2.CursorAdvancedTo)
	}

	dbCheck2, err := openChannelAt(t, workdir)
	if err != nil {
		t.Fatalf("reopen2: %v", err)
	}
	var stillGot int64
	if err := dbCheck2.QueryRowContext(context.Background(),
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`, agentID,
	).Scan(&stillGot); err != nil {
		t.Fatalf("read cursor 2: %v", err)
	}
	if stillGot != 12 {
		t.Errorf("post-replay cursor = %d, want 12", stillGot)
	}
}

// openChannelAt opens (or creates) the channel sqlite at
// workdir/messages.sqlite with the full DDL applied. Mirrors
// supervisor's openChannel helper but accepts a caller-supplied
// workdir so the runtime tests can share the same file across the
// test setup phase + Runtime.Run's own open.
func openChannelAt(t *testing.T, workdir string) (*sql.DB, error) {
	t.Helper()
	path := filepath.Join(workdir, "messages.sqlite")
	db, err := storeOpenChannelHelper(path)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, nil
}

// verifyTurnCtxFile confirms the worker actually persisted the
// spawn context to the home-dir fallback path.
func verifyTurnCtxFile(t *testing.T, home string, tc TurnCtx) {
	t.Helper()
	f, err := readTurnCtxFile(home)
	if err != nil {
		t.Fatalf("readTurnCtxFile: %v", err)
	}
	if f.ChannelID != tc.ChannelID || f.SelfID != tc.AgentID || f.FencingToken != tc.FencingToken {
		t.Errorf("turn-ctx.json mismatch: got %+v", f)
	}
}
