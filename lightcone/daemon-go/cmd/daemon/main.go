// Command daemon is the Go re-implementation of the lightcone daemon
// (replacing lightcone/daemon/src/index.js once M1.3 closes).
//
// T0 scope: placeholder entrypoint. It logs a startup banner via log/slog,
// waits for SIGINT / SIGTERM, then exits cleanly. Real subsystems
// (bootstrap saga / supervisor / harness / trigger gateway / scheduler)
// are wired in by tickets T1–T16.
//
// The Node daemon under lightcone/daemon/ stays online during the M1.3
// dual-stack window; both processes coexist until the cutover described
// in M1.3-T16.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// Version is the daemon-go placeholder version. Bumped per ticket.
const Version = "0.0.0-t0"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("daemon-go starting",
		"event", "daemon.start",
		"version", Version,
		"pid", os.Getpid(),
	)

	// Listen for SIGINT / SIGTERM so the production lifecycle works
	// even while the daemon body is still a placeholder.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// T0 placeholder: no work to do. Channels of work are added
	// incrementally by T1–T9 (bootstrap saga / supervisor / trigger
	// gateway / scheduler / harness HTTP binding).
	<-ctx.Done()

	slog.Info("daemon-go stopping",
		"event", "daemon.stop",
		"reason", "signal",
	)
}
