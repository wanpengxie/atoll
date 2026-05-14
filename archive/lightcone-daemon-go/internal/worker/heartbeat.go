package worker

// heartbeat.go runs the per-worker lease renewal goroutine demanded by
// L2 §1.4.9 spawn protocol:
//
//   - Every lease_ttl/2 seconds (default 30s) call supervisor.Heartbeat
//     to extend the worker_locks row.
//   - If CAS misses (ErrFencingStale) → the worker has been stolen by
//     a steal_lock from another supervisor; we MUST self-destruct so
//     no further side effects happen under the old token.
//   - If the row vanished entirely (ErrLockMissing) → the supervisor
//     intentionally released the lease; also self-destruct.
//
// "Self-destruct" here means cancel the root context handed in by the
// Runtime: the agent loop pivots to ctx.Err() at next yield, the
// harness write path returns ctx done immediately, and main exits
// cleanly. Hard SIGKILL is unnecessary — co-operative cancel keeps
// half-written state out of the channel sqlite.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/coagent-ai/daemon-go/internal/supervisor"
)

// HeartbeatConfig wires the heartbeat goroutine. All fields except
// Now / Logger / Interval are required.
type HeartbeatConfig struct {
	// DB is the channel-local sqlite (the same handle the harness uses).
	DB *sql.DB

	// AgentID is the actor this worker represents (worker_locks PK).
	AgentID string

	// WorkerID is the worker_locks.worker_id assigned at spawn.
	WorkerID string

	// FencingToken is the token the worker booted with. Heartbeat
	// CAS predicates this — a steal increments the token, so a stolen
	// worker's first Heartbeat call returns ErrFencingStale.
	FencingToken int64

	// LeaseTTL is the supervisor's lease_ttl in seconds. The
	// heartbeat ticks at LeaseTTL/2 by default.
	LeaseTTL int64

	// Interval optionally overrides the heartbeat tick cadence. Defaults
	// to LeaseTTL/2 seconds. Tests inject a sub-second interval to keep
	// the case fast.
	Interval time.Duration

	// Now returns Unix seconds. Defaults to time.Now().Unix. Tests
	// inject a fixed clock.
	Now func() int64

	// Logger receives heartbeat.* events. Defaults to noopLogger.
	Logger Logger
}

// HeartbeatResult is what RunHeartbeat returns when its loop exits. The
// caller (Runtime) uses Stale / Missing to decide whether to log a
// steal vs a graceful release.
type HeartbeatResult struct {
	// Stale is true when the loop exited because the CAS missed (the
	// worker was stolen). Stale is the "self-destruct" signal.
	Stale bool

	// Missing is true when the worker_locks row vanished mid-run (the
	// supervisor released it without spawning a replacement first —
	// shouldn't happen in normal operation but we handle it).
	Missing bool

	// Err is the last sql / driver error encountered, if any. nil on a
	// clean shutdown via ctx cancel.
	Err error
}

// RunHeartbeat blocks until ctx is done OR the lease becomes
// unrecoverable. It is designed to run in its own goroutine; the caller
// passes a cancel func so the goroutine can request a runtime-wide
// shutdown when the lease goes stale.
//
// Returns once the lease is unrecoverable or ctx is cancelled. Always
// returns a non-nil result.
func RunHeartbeat(ctx context.Context, cfg HeartbeatConfig, cancelRuntime context.CancelFunc) HeartbeatResult {
	if err := validateHeartbeatConfig(&cfg); err != nil {
		return HeartbeatResult{Err: err}
	}

	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()

	cfg.Logger.Info("worker.heartbeat.start",
		"agent_id", cfg.AgentID,
		"worker_id", cfg.WorkerID,
		"fencing_token", cfg.FencingToken,
		"interval_ms", cfg.Interval.Milliseconds(),
	)

	for {
		select {
		case <-ctx.Done():
			cfg.Logger.Info("worker.heartbeat.stop",
				"agent_id", cfg.AgentID, "worker_id", cfg.WorkerID,
				"reason", "ctx_done",
			)
			return HeartbeatResult{}
		case <-tick.C:
		}

		err := supervisor.Heartbeat(
			ctx, cfg.DB,
			cfg.AgentID, cfg.WorkerID,
			cfg.FencingToken, cfg.LeaseTTL,
			cfg.Now,
		)
		switch {
		case errors.Is(err, supervisor.ErrFencingStale):
			cfg.Logger.Warn("worker.heartbeat.stale",
				"agent_id", cfg.AgentID, "worker_id", cfg.WorkerID,
				"fencing_token", cfg.FencingToken,
			)
			if cancelRuntime != nil {
				cancelRuntime()
			}
			return HeartbeatResult{Stale: true}
		case errors.Is(err, supervisor.ErrLockMissing):
			cfg.Logger.Warn("worker.heartbeat.missing",
				"agent_id", cfg.AgentID, "worker_id", cfg.WorkerID,
			)
			if cancelRuntime != nil {
				cancelRuntime()
			}
			return HeartbeatResult{Missing: true}
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Treat as graceful shutdown — caller cancelled.
			return HeartbeatResult{}
		case err != nil:
			// Driver / sql infrastructure error. Log + retry on next
			// tick: a transient driver hiccup is recoverable, and a
			// persistent one will be caught by the lease expiring at
			// the supervisor side.
			cfg.Logger.Error("worker.heartbeat.error",
				"agent_id", cfg.AgentID, "worker_id", cfg.WorkerID,
				"err", err.Error(),
			)
		default:
			cfg.Logger.Info("worker.heartbeat.ok",
				"agent_id", cfg.AgentID, "worker_id", cfg.WorkerID,
			)
		}
	}
}

// validateHeartbeatConfig fills defaults + rejects missing required
// fields. Mutates cfg in place so the caller's struct stays valid for
// the rest of the goroutine's lifetime.
func validateHeartbeatConfig(cfg *HeartbeatConfig) error {
	if cfg.DB == nil {
		return fmt.Errorf("heartbeat: db is nil")
	}
	if cfg.AgentID == "" {
		return fmt.Errorf("heartbeat: agent_id required")
	}
	if cfg.WorkerID == "" {
		return fmt.Errorf("heartbeat: worker_id required")
	}
	if cfg.FencingToken <= 0 {
		return fmt.Errorf("heartbeat: fencing_token must be positive, got %d", cfg.FencingToken)
	}
	if cfg.LeaseTTL <= 0 {
		return fmt.Errorf("heartbeat: lease_ttl must be positive, got %d", cfg.LeaseTTL)
	}
	if cfg.Interval <= 0 {
		// Default per L2 §1.4.9: lease_ttl/2.
		cfg.Interval = time.Duration(cfg.LeaseTTL) * time.Second / 2
		if cfg.Interval <= 0 {
			cfg.Interval = time.Second
		}
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().Unix() }
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	return nil
}
