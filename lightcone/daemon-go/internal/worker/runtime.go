package worker

// runtime.go is the top-level worker orchestrator: it ties together
// the channel sqlite, harness deps, turn-ctx persistence, heartbeat
// goroutine, the v4 wire bridge, and the go-kimi Agent into a single
// Run(ctx, cfg) entrypoint that cmd/worker/main.go calls.
//
// Lifecycle (L2 §3.9.3 + §1.4.10):
//
//  1. Validate TurnCtx (channel/agent/worker/fencing/workdir all set).
//  2. Write ~/.coagent/turn-ctx.json so coagent CLI fallback (L2
//     §3.4.2) sees the same context.
//  3. Open the channel sqlite at `<channel_workdir>/messages.sqlite`.
//  4. Build harness Deps (Store / Actors / Types / WorkerLocks) from
//     the channel sqlite — these are reused by the wire bridge.
//  5. Construct the wire bridge with a WriterFn closing over
//     harness.InWorkerBus(deps, env, callerCtx).
//  6. Build the kimi.Agent with the bridge as WireEmitter.
//  7. Spawn the heartbeat goroutine.
//  8. Drive the turn — if TurnCtx.TriggerMsgID is set, build the
//     prompt from the trigger payload and call agent.Run; otherwise
//     enter wait state and return (the supervisor respawns when a
//     new trigger arrives, so a "no trigger" worker just exits clean
//     after registering the actor).
//  9. On any exit (clean / heartbeat-stale / agent error): cancel
//     siblings, close agent, release worker_locks row.
//
// The runtime returns a structured Result so cmd/worker/main.go can
// translate to an exit code. Heartbeat-stale → exit 0 (supervisor
// already stole us; nothing to retry); agent error → exit 1.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"
	workertools "github.com/coagent-ai/daemon-go/internal/worker/tools"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	kimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// RuntimeConfig is everything Runtime.Run needs to start a worker.
// TurnCtx is the spawn-time context; HomeDir / Provider / etc. tune
// the per-test / per-deployment behaviour.
type RuntimeConfig struct {
	// TurnCtx is the spawn context. Validate() runs at boot.
	TurnCtx TurnCtx

	// HomeDir overrides $HOME for the turn-ctx.json write path. Empty
	// = read $HOME from the env. Tests pass t.TempDir().
	HomeDir string

	// Provider supplies the LLM provider. Tests pass the echo provider
	// (llm.NewEchoChatProvider). Production callers wire a real
	// provider out of daemon config.
	//
	// When nil, Runtime.Run falls back to llm.NewEchoChatProvider so a
	// misconfigured deploy still emits something — but logs a Warn
	// so the operator notices.
	Provider llm.ChatProvider

	// Model is the model name to advertise. Empty defaults to
	// "echo-worker".
	Model string

	// Prompt overrides the prompt the worker hands to agent.Run. When
	// empty, the runtime derives one from TurnCtx (channel id + agent
	// id + trigger msg id) — sufficient for echo-provider smoke tests.
	Prompt string

	// SkipAgentRun is the "register + heartbeat + exit" path used by
	// supervisor tests that just want to prove the spawn protocol.
	// Production callers leave this false.
	SkipAgentRun bool

	// Logger receives worker.* events. Defaults to noopLogger.
	Logger Logger

	// SkipReleaseOnExit, when true, leaves the worker_locks row in
	// place at shutdown. Default false — Runtime.Run releases the row
	// so the next supervisor tick spawns immediately rather than
	// waiting LeaseTTL seconds.
	SkipReleaseOnExit bool

	// Backlog optionally seeds the runtime with the supervisor's
	// pre-scanned backlog (mostly for in-process integration tests so
	// they can verify "supervisor passed N rows" end-to-end without
	// double-scanning). When nil the runtime scans the channel sqlite
	// itself via supervisor.BacklogScan — production behaviour, since
	// the spawn argv/env doesn't carry the slice across exec.
	Backlog []supervisor.BacklogMessage

	// Now returns Unix seconds for the BacklogScan call + cursor
	// advance write. Defaults to time.Now().Unix(). Tests inject a
	// fixed clock so the predicate `m.not_before <= now` behaves
	// deterministically.
	Now func() int64
}

// RuntimeResult is the structured outcome of Run. cmd/worker/main.go
// uses it to choose the process exit code.
type RuntimeResult struct {
	// AgentReply is the assistant's last text reply (when SkipAgentRun
	// is false). Empty when no turn ran.
	AgentReply string

	// HeartbeatStale signals the worker exited because its lease was
	// stolen. Maps to exit 0 (the supervisor already replaced us).
	HeartbeatStale bool

	// AgentErr captures any error from kimi.Agent.Run.
	AgentErr error

	// LockReleased reports whether the worker_locks row was
	// successfully released at shutdown.
	LockReleased bool

	// BacklogConsumed is the slice of backlog rows the runtime saw at
	// boot (after pre-scanning the channel sqlite). Empty when the
	// spawn was a "no trigger" no-op.
	BacklogConsumed []supervisor.BacklogMessage

	// CursorAdvancedTo is the actor_cursors.last_consumed_seq value
	// the runtime CAS-bumped to at end-of-turn. Zero when no turn
	// ran (no backlog) or when the CAS missed (cursor already past
	// our max — possible after a crash + replay).
	CursorAdvancedTo int64

	// SkippedNoTrigger is true when the runtime exited cleanly without
	// driving agent.Run because both Trigger.MsgID and the live
	// backlog were empty. Maps to exit 0; the supervisor sees the
	// lock released and stops respawning until a fresh trigger arrives.
	SkippedNoTrigger bool
}

// Run executes one full worker lifecycle. Returns a structured result
// + an aggregate error: nil for the happy path, ctx.Err() for
// cancellation, wrapped errors for sqlite / agent / harness failures.
//
// The function is synchronous — Run blocks until the lifecycle
// completes (agent done / ctx cancelled / heartbeat stale). It owns
// every goroutine it spawns and waits for them before returning.
func Run(parentCtx context.Context, cfg RuntimeConfig) (RuntimeResult, error) {
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	tc := cfg.TurnCtx
	if err := tc.Validate(); err != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: validate turn_ctx: %w", err)
	}

	// Step 2: persist turn-ctx.json for the coagent CLI fallback.
	if _, err := WriteTurnCtxFile(tc, cfg.HomeDir); err != nil {
		// Non-fatal — coagent CLI fallback is best-effort. Log and continue.
		cfg.Logger.Warn("worker.turn_ctx.write.error", "err", err.Error())
	}

	// Step 3: open the channel sqlite.
	dbPath := filepath.Join(tc.ChannelWorkdir, "messages.sqlite")
	db, err := store.OpenChannel(parentCtx, dbPath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: open channel sqlite %s: %w", dbPath, err)
	}
	defer func() { _ = db.Close() }()

	// Step 3a: open a sibling read-only handle for the LLM-facing
	// sqlite.query tool. `mode=ro` enforces SQLite-level read-only
	// at the driver — any DML (including DML CTEs with RETURNING)
	// fails regardless of validator strength. Keeps the harness
	// ledger executor on the writable handle untouched. See
	// R2-FIX-7 (t113). SkipDDL is required because the DDL has
	// already been applied via the writable open above, and ro
	// files cannot run CREATE TABLE.
	roDB, err := store.OpenChannel(parentCtx, dbPath, store.OpenOptions{ReadOnly: true, SkipDDL: true})
	if err != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: open channel sqlite (ro) %s: %w", dbPath, err)
	}
	defer func() { _ = roDB.Close() }()

	// Confirm the actor row exists. The bootstrap saga (T3) seeds it;
	// a missing row means the channel was created out-of-band or the
	// supervisor hands us a wrong agent_id. Surface early so the
	// supervisor logs make sense instead of a cryptic harness reject
	// down the line.
	if _, err := registry.Get(parentCtx, db, tc.AgentID); err != nil {
		if errors.Is(err, registry.ErrActorNotFound) {
			return RuntimeResult{}, fmt.Errorf("worker_runtime: actor %q not registered in channel %q", tc.AgentID, tc.ChannelID)
		}
		return RuntimeResult{}, fmt.Errorf("worker_runtime: registry.Get %q: %w", tc.AgentID, err)
	}

	// Step 3a: ensure the built-in tool actors + their type_registry
	// rows exist in this channel (idempotent — re-running registers no
	// new rows). MUST run before buildHarnessDeps so LoadTypeLookup
	// sees the new types in the cache.
	if err := workertools.EnsureToolActors(parentCtx, workertools.EnsureConfig{
		DB:        db,
		ChannelID: tc.ChannelID,
		Now:       time.Now().Unix(),
		Logger:    cfg.Logger,
	}); err != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: ensure tool actors: %w", err)
	}

	// Step 4: build harness deps backed by the channel sqlite.
	deps, derr := buildHarnessDeps(parentCtx, db, tc)
	if derr != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: build harness deps: %w", derr)
	}

	// Derive a cancellable runtime ctx — heartbeat-stale flips this to
	// signal "shut everything down".
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Step 5: build the wire bridge.
	bridge, berr := NewWireBridge(BridgeConfig{
		ChannelID:            tc.ChannelID,
		AgentID:              tc.AgentID,
		FencingToken:         tc.FencingToken,
		TriggerCorrelationID: tc.TriggerCorrelationID,
		Logger:               cfg.Logger,
		Writer: func(wctx context.Context, env *v4types.Envelope, caller pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
			return pkgharness.InWorkerBus(wctx, deps, env, caller)
		},
	})
	if berr != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: build wire bridge: %w", berr)
	}

	provider := cfg.Provider
	if provider == nil {
		cfg.Logger.Warn("worker.provider.fallback_echo",
			"channel_id", tc.ChannelID, "agent_id", tc.AgentID)
		provider = llm.NewEchoChatProvider(cfg.Model)
	}

	// Step 5a: build the V4ize-wrapped tool slice (T11). Each tool's
	// Execute emits a request + response pair via in_worker_bus harness.
	// MUST run after EnsureToolActors + buildHarnessDeps so the
	// wrapper has the populated TypeLookup + the seeded tool actor
	// rows for audience validation.
	wrappedTools, terr := workertools.BuildTools(workertools.BuildConfig{
		DB:                   db,
		ReadOnlyDB:           roDB,
		ChannelID:            tc.ChannelID,
		AgentID:              tc.AgentID,
		FencingToken:         tc.FencingToken,
		TurnID:               deriveTurnID(tc),
		TriggerCorrelationID: tc.TriggerCorrelationID,
		WorkDir:              tc.ChannelWorkdir,
		Deps:                 deps,
		Logger:               cfg.Logger,
	})
	if terr != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: BuildTools: %w", terr)
	}

	// Step 6: build the go-kimi agent. SessionID is intentionally left
	// empty — go-kimi auto-creates a fresh session under the workdir
	// per spawn. Persistent session resumption belongs to T11 / T13
	// once turn replay semantics get hardened.
	//
	// AdditionalTools = the v4-wrapped catalogue. We set
	// DisableStandardSandboxTools so go-kimi does not also expose its
	// un-wrapped default tool set — every tool call must land on the
	// channel log (L2 §3.9.4 invariant).
	agent, aerr := NewAgent(AgentConfig{
		WorkDir:                     tc.ChannelWorkdir,
		Provider:                    provider,
		Model:                       cfg.Model,
		Emitter:                     bridge,
		AdditionalTools:             wrappedTools,
		DisableStandardSandboxTools: true,
	})
	if aerr != nil {
		return RuntimeResult{}, fmt.Errorf("worker_runtime: NewAgent: %w", aerr)
	}
	defer func() { _ = agent.Close() }()

	// Step 7: start the heartbeat goroutine.
	hbResultCh := make(chan HeartbeatResult, 1)
	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		hbResultCh <- RunHeartbeat(ctx, HeartbeatConfig{
			DB:           db,
			AgentID:      tc.AgentID,
			WorkerID:     tc.WorkerID,
			FencingToken: tc.FencingToken,
			LeaseTTL:     tc.LeaseTTL,
			Logger:       cfg.Logger,
		}, cancel)
	}()

	// Step 7a: load backlog. The supervisor passes its pre-scan result
	// in cfg.Backlog when wired in-process (integration tests); the
	// production exec-spawned worker re-scans the channel sqlite here
	// because the spawn argv/env can't carry the slice across exec.
	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}
	backlog := cfg.Backlog
	if backlog == nil {
		scanned, scanErr := supervisor.BacklogScan(ctx, db, tc.AgentID, now())
		if scanErr != nil {
			return RuntimeResult{}, fmt.Errorf("worker_runtime: backlog scan: %w", scanErr)
		}
		backlog = scanned
	}

	cfg.Logger.Info("worker.runtime.ready",
		"channel_id", tc.ChannelID,
		"agent_id", tc.AgentID,
		"worker_id", tc.WorkerID,
		"fencing_token", tc.FencingToken,
		"trigger_msg_id", tc.TriggerMsgID,
		"backlog_size", len(backlog),
	)

	// Step 8: drive the turn.
	//
	// Per FIX-4 §"backlog 为空且 trigger 为空时进等待态"（exit 让
	// supervisor 不重 spawn）the runtime MUST NOT call agent.Run when
	// neither a trigger nor backlog is available — otherwise the
	// supervisor wakes on exit, finds no work, spawns another noop,
	// and loops forever.
	var result RuntimeResult
	result.BacklogConsumed = backlog
	hasTrigger := strings.TrimSpace(tc.TriggerMsgID) != "" || len(backlog) > 0
	switch {
	case cfg.SkipAgentRun:
		cfg.Logger.Info("worker.runtime.skip_agent_run",
			"channel_id", tc.ChannelID, "agent_id", tc.AgentID)
	case !hasTrigger:
		result.SkippedNoTrigger = true
		cfg.Logger.Info("worker.runtime.no_trigger_exit",
			"channel_id", tc.ChannelID,
			"agent_id", tc.AgentID,
			"worker_id", tc.WorkerID,
		)
	default:
		prompt := cfg.Prompt
		if prompt == "" {
			prompt = derivePrompt(tc)
		}
		if rerr := agent.Run(ctx, prompt); rerr != nil {
			cfg.Logger.Error("worker.agent.run.error",
				"agent_id", tc.AgentID, "err", rerr.Error())
			result.AgentErr = rerr
		} else {
			result.AgentReply = lastReplyText(agent)
			cfg.Logger.Info("worker.agent.run.ok",
				"agent_id", tc.AgentID, "reply_chars", len(result.AgentReply))
		}
	}

	// Step 8a: advance actor_cursors only when the turn completed
	// without an agent error. The CAS predicate
	// `last_consumed_seq < ?new` mirrors L1 §6.3.4.3 + the channel
	// invariant test (TestActorCursors_MonotonicCAS): a replay after
	// a crash sees the same backlog, but the WHERE clause makes the
	// second UPDATE a no-op so side effects (cursor wise) stay
	// idempotent.
	if !cfg.SkipAgentRun && result.AgentErr == nil && len(backlog) > 0 {
		maxSeq := backlog[len(backlog)-1].Seq
		advanced, advErr := advanceCursor(ctx, db, tc.AgentID, maxSeq, now())
		if advErr != nil {
			cfg.Logger.Warn("worker.cursor.advance.error",
				"agent_id", tc.AgentID, "max_seq", maxSeq, "err", advErr.Error())
		} else if advanced {
			result.CursorAdvancedTo = maxSeq
			cfg.Logger.Info("worker.cursor.advance.ok",
				"agent_id", tc.AgentID, "max_seq", maxSeq)
		} else {
			cfg.Logger.Info("worker.cursor.advance.noop",
				"agent_id", tc.AgentID, "max_seq", maxSeq,
				"reason", "cursor already past max_seq (replay)")
		}
	}

	// Step 9: shutdown. Cancel siblings (so heartbeat exits), wait, then
	// release the lock.
	cancel()
	hbWG.Wait()
	hbRes := <-hbResultCh
	result.HeartbeatStale = hbRes.Stale

	if !cfg.SkipReleaseOnExit && !hbRes.Stale {
		// We only release if we still own the lock — a stale lease
		// belongs to the new worker, not us.
		relCtx, relCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer relCancel()
		if err := supervisor.Release(relCtx, db, tc.AgentID, tc.WorkerID); err != nil {
			if !errors.Is(err, supervisor.ErrLockMissing) {
				cfg.Logger.Warn("worker.release.error",
					"agent_id", tc.AgentID, "worker_id", tc.WorkerID, "err", err.Error())
			}
		} else {
			result.LockReleased = true
		}
	}

	return result, result.AgentErr
}

// buildHarnessDeps mirrors how internal/harness wires sqlite-backed
// dependencies into pkgharness.Deps for binding_daemon_rpc. The
// worker uses the same wiring for in_worker_bus binding — no HTTP, just
// in-process calls.
func buildHarnessDeps(ctx context.Context, db *sql.DB, tc TurnCtx) (pkgharness.Deps, error) {
	types, err := harness.LoadTypeLookup(ctx, db)
	if err != nil {
		return pkgharness.Deps{}, fmt.Errorf("load_type_lookup: %w", err)
	}
	return pkgharness.New(
		harness.NewSQLiteStore(db),
		harness.NewSQLiteActors(db),
		types,
		harness.NewSQLiteWorkerLocks(db),
		tc.ChannelID,
	), nil
}

// deriveTurnID synthesises the action_ledger turn key from the spawn
// context. Spec §3.9.3 mandates `hash(actor_id, min_seq_in_batch)` but
// the supervisor does not yet hand the worker `min_seq` — M1.3 baseline
// substitutes `turn:<agent_id>:<trigger_msg_id|noop>`. The value is
// stable across worker respawns for the same trigger, which is the
// property action_ledger Reserve cares about.
func deriveTurnID(tc TurnCtx) string {
	if strings.TrimSpace(tc.TriggerMsgID) == "" {
		return "turn:" + tc.AgentID + ":noop"
	}
	return "turn:" + tc.AgentID + ":" + tc.TriggerMsgID
}

// derivePrompt builds a minimal prompt for the echo-provider path. M1.3
// baseline keeps it simple — the supervisor's backlog scan + L2 §4
// prompt-context injection will replace this in a later ticket once
// the channel template + L4 domain prompt land.
func derivePrompt(tc TurnCtx) string {
	if tc.TriggerMsgID != "" {
		return fmt.Sprintf("channel=%s agent=%s trigger=%s", tc.ChannelID, tc.AgentID, tc.TriggerMsgID)
	}
	return fmt.Sprintf("channel=%s agent=%s no-trigger", tc.ChannelID, tc.AgentID)
}

// advanceCursor CAS-bumps actor_cursors.last_consumed_seq to maxSeq
// for the given actor. Returns (true, nil) when the row updated, (false,
// nil) when the predicate `last_consumed_seq < maxSeq` ruled the
// update out (cursor already at or past maxSeq — typical replay path),
// (false, err) on driver failures.
//
// Single-statement so no enclosing tx is needed; mirrors the
// invariants_test.TestActorCursors_MonotonicCAS contract.
func advanceCursor(ctx context.Context, db *sql.DB, actorID string, maxSeq, now int64) (bool, error) {
	if actorID == "" {
		return false, fmt.Errorf("advance_cursor: empty actor_id")
	}
	if maxSeq <= 0 {
		return false, fmt.Errorf("advance_cursor: max_seq must be positive, got %d", maxSeq)
	}
	res, err := db.ExecContext(ctx,
		`UPDATE actor_cursors
		    SET last_consumed_seq = ?, updated_at = ?
		  WHERE actor_id = ?
		    AND last_consumed_seq < ?`,
		maxSeq, now, actorID, maxSeq,
	)
	if err != nil {
		return false, fmt.Errorf("advance_cursor: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("advance_cursor rowsAffected: %w", err)
	}
	return affected == 1, nil
}

// lastReplyText pulls the assistant reply text out of agent.LastResult.
// Returns "" when the agent did not produce any text content.
func lastReplyText(agent *kimi.Agent) string {
	res := agent.LastResult()
	var buf string
	for _, p := range res.Content {
		if tp, ok := p.(types.TextPart); ok {
			buf += tp.Text
		}
	}
	return buf
}
