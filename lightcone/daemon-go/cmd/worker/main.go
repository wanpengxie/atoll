// Command worker is the v4 worker process — a go-kimi Agent instance
// running inside the M1.3 v4 protocol baseline (L2 §3.9). The daemon
// supervisor (T6) spawns one of these per (channel, agent) pair; the
// process self-registers via worker_locks heartbeat, drives one go-kimi
// turn against the trigger backlog, and exits.
//
// Spec entrypoints:
//
//   - L2 §3.9.3 spawn protocol (argv + env): channel_id / agent_id /
//     worker_id / fencing_token / trigger_*.
//   - L2 §3.4.2 turn-ctx propagation (env-first + file fallback).
//   - L2 §1.4.9 worker_locks heartbeat (lease_ttl/2).
//   - L2 §3.9.5 wire event bridge (24 go-kimi wire types → v4 emit).
//
// Exit codes:
//
//   0 — clean exit (agent.Run ok + lock released), OR heartbeat stale
//       (supervisor already stole us; nothing to retry).
//   1 — agent.Run returned an error.
//   2 — boot failure (flag parse, sqlite open, registry not found, etc.).
//
// The T11 V4ize-wrapped tools land in a later ticket; this worker
// uses whatever AdditionalTools the runtime defaults to (currently
// empty + go-kimi's standard sandbox tools).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/coagent-ai/daemon-go/internal/worker"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
)

// Version is bumped per ticket so log lines + crash reports identify
// the worker build.
const Version = "0.1.0-t10"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	tc, err := parseTurnCtx(os.Args[1:], os.Getenv)
	if err != nil {
		slog.Error("worker boot: parse turn-ctx", "err", err.Error())
		os.Exit(2)
	}
	if err := tc.Validate(); err != nil {
		slog.Error("worker boot: invalid turn-ctx", "err", err.Error())
		os.Exit(2)
	}
	slog.Info("worker.start",
		"version", Version,
		"pid", os.Getpid(),
		"channel_id", tc.ChannelID,
		"agent_id", tc.AgentID,
		"worker_id", tc.WorkerID,
		"fencing_token", tc.FencingToken,
		"trigger_msg_id", tc.TriggerMsgID,
		"lease_ttl_s", tc.LeaseTTL,
	)

	// Catch SIGINT / SIGTERM so a supervisor SIGTERM walks through
	// the same graceful-shutdown path as an in-process ctx cancel
	// (release lock, close agent, drain wire bridge).
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	provider := buildProvider(os.Getenv("COAGENT_PROVIDER"))
	res, runErr := worker.Run(ctx, worker.RuntimeConfig{
		TurnCtx:  tc,
		Provider: provider,
		Model:    os.Getenv("COAGENT_MODEL"),
		Logger:   slogAdapter{l: logger},
	})

	slog.Info("worker.exit",
		"reply_chars", len(res.AgentReply),
		"heartbeat_stale", res.HeartbeatStale,
		"lock_released", res.LockReleased,
	)
	if runErr != nil {
		var ctxErr error
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			ctxErr = runErr
		}
		if ctxErr != nil {
			// Treat ctx cancellation as a clean exit — the parent
			// signalled us to stop, not the agent failing.
			os.Exit(0)
		}
		slog.Error("worker.run.error", "err", runErr.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

// parseTurnCtx wires the worker.TurnCtx ParseFlags + FromEnv pipeline.
// CLI flags win, env vars fill any gaps. Returns the populated struct
// (still needs Validate() before use).
func parseTurnCtx(args []string, getenv func(string) string) (worker.TurnCtx, error) {
	var tc worker.TurnCtx
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	tc.ParseFlags(fs)
	if err := fs.Parse(args); err != nil {
		return worker.TurnCtx{}, fmt.Errorf("flag parse: %w", err)
	}
	tc.FromEnv(getenv)
	return tc, nil
}

// buildProvider picks an LLM provider for this worker. Production
// would resolve a daemon-config-driven choice (Moonshot / OpenAI /
// Anthropic / …); M1.3 baseline supports only the in-process echo
// provider because the ticket forbids real LLM calls during the PM
// unattended phase.
//
// The COAGENT_PROVIDER env switches the implementation — "echo" (the
// default) builds llm.NewEchoChatProvider; anything else logs a Warn
// and falls back to echo. T13+ will wire real providers.
func buildProvider(name string) llm.ChatProvider {
	switch name {
	case "", "echo":
		return llm.NewEchoChatProvider(os.Getenv("COAGENT_MODEL"))
	default:
		slog.Warn("worker.provider.unknown", "name", name, "fallback", "echo")
		return llm.NewEchoChatProvider(os.Getenv("COAGENT_MODEL"))
	}
}

// slogAdapter wraps *slog.Logger to satisfy worker.Logger. Keeps the
// internal/worker pkg free of a hard dependency on log/slog.
type slogAdapter struct{ l *slog.Logger }

func (s slogAdapter) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s slogAdapter) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s slogAdapter) Error(msg string, args ...any) { s.l.Error(msg, args...) }
