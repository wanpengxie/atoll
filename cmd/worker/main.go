// Package main wires the coagent worker subprocess binary.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (worker IPC-only
// constraint — codex review #9).
//
// The worker binary is a thin shell: it constructs runtime/worker.Runtime
// with os.Stdin / os.Stdout as the IPC stream and calls Run. The agent
// loop itself (go-kimi or replacement) is wired by composing a
// worker.Bridge implementation here so that runtime/worker stays
// vendor-light (no go-kimi import inside runtime/worker).
//
// M1.6-T1 wires the deterministic MockBridge so the daemon ↔ worker
// trigger loop can be exercised end-to-end without a real LLM. The
// real-LLM bridge lands in M1.6-T7 phase-4 (kimi via DeepSeek
// anthropic-compat endpoint).
//
// Logging note: stdout is the IPC frame stream (read by the parent
// daemon as length-prefixed binary frames). Logs MUST go to stderr
// otherwise we corrupt the protocol. cmd/worker installs the
// structured logger with Writer=os.Stderr accordingly.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/wanpengxie/ActOS/pkg/logger"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

// version is set via -ldflags at build time.
var version = "dev"

func main() {
	leaseID := flag.String("lease-id", os.Getenv("COAGENT_WORKER_LEASE_ID"),
		"lease id assigned by daemon (also via COAGENT_WORKER_LEASE_ID)")
	maxTurns := flag.Int("max-turns", 8,
		"mock bridge: cap on trigger reactions before next_action=done exit")
	pretty := flag.Bool("pretty-logs", false,
		"emit human-readable console logs on stderr (dev only; production stays on JSON)")
	flag.Parse()

	if *leaseID == "" {
		// Pre-logger fail-fast — print to stderr directly so the daemon
		// supervisor sees the reason even if logger init is skipped.
		fmt.Fprintln(os.Stderr, "worker: --lease-id required (or COAGENT_WORKER_LEASE_ID env)")
		os.Exit(2)
	}

	// M1.6-T7 phase-2 — structured logger. CRITICAL: writer MUST be
	// os.Stderr because os.Stdout is the daemon ↔ worker IPC frame
	// stream (binary length-prefixed frames).
	lg := logger.New(logger.Config{
		Component: "worker",
		Version:   version,
		Writer:    os.Stderr,
		Pretty:    *pretty,
		Level:     os.Getenv("COAGENT_LOG_LEVEL"),
	})
	restore := lg.RedirectStdlib()
	defer restore()

	lg.Z().Info().
		Str("event", "worker.starting").
		Str("lease_id", *leaseID).
		Int("max_turns", *maxTurns).
		Msg("coagent-worker starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bridge := worker.NewMockBridge()
	bridge.MaxTurns = *maxTurns

	rt, err := worker.New(worker.Config{
		LeaseID: *leaseID,
		In:      os.Stdin,
		Out:     os.Stdout,
		Bridge:  bridge,
	})
	if err != nil {
		lg.Z().Error().Err(err).Str("event", "worker.assemble_failed").Msg("worker assemble failed")
		os.Exit(1)
	}
	if err := rt.Run(ctx); err != nil {
		lg.Z().Error().Err(err).Str("event", "worker.exit_error").Msg("worker exited with error")
		os.Exit(1)
	}
	lg.Z().Info().Str("event", "worker.stopped").Msg("worker stopped cleanly")
}
