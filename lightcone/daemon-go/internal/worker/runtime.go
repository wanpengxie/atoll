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
	"sync"
	"time"

	"github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"
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

	// Step 6: build the go-kimi agent. SessionID is intentionally left
	// empty — go-kimi auto-creates a fresh session under the workdir
	// per spawn. Persistent session resumption belongs to T11 / T13
	// once turn replay semantics get hardened.
	agent, aerr := NewAgent(AgentConfig{
		WorkDir:  tc.ChannelWorkdir,
		Provider: provider,
		Model:    cfg.Model,
		Emitter:  bridge,
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

	cfg.Logger.Info("worker.runtime.ready",
		"channel_id", tc.ChannelID,
		"agent_id", tc.AgentID,
		"worker_id", tc.WorkerID,
		"fencing_token", tc.FencingToken,
		"trigger_msg_id", tc.TriggerMsgID,
	)

	// Step 8: drive the turn.
	var result RuntimeResult
	if !cfg.SkipAgentRun {
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
	} else {
		cfg.Logger.Info("worker.runtime.skip_agent_run",
			"channel_id", tc.ChannelID, "agent_id", tc.AgentID)
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
