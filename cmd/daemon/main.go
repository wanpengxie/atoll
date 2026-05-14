// Package main wires the coagent daemon binary.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (acceptance gate #1
// — cmd/daemon can start; scans channels/ directory in T1.6 phase 1/2/3/4
// order).
//
// In M1.5-T3 this binary is a thin assembly:
//
//  1. Open daemon.sqlite (bootstrap_registry).
//  2. Reconciler.Run — roll back any in-progress crashed sagas.
//  3. lifecycle.Bootstrapper.LoadLocal — scan channels/ dir, refresh
//     daemon_epoch.
//  4. (placeholder) Connect to server via daemonbus transit — replaced
//     in T6 with real WS client. For now uses the in-process MockBus
//     when the --mock-bus flag is set.
//  5. ReportReclaim → MarkRecovering → MarkAcceptingNew.
//  6. Block until shutdown signal.
//
// Real adapter / scheduler / supervisor wiring lands in T4 / T5 / T6.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/coagent-ai/coagent/runtime"
)

func main() {
	var (
		dataDir     = flag.String("data-dir", defaultDataDir(), "daemon data directory")
		daemonID    = flag.String("daemon-id", "daemon-local", "stable daemon identifier")
		daemonEpoch = flag.Int64("daemon-epoch", 0, "daemon process epoch (0 = use unix-second)")
		mockBus     = flag.Bool("mock-bus", true, "use in-process mock bus (M1.5-T3 default; T6 replaces with WS)")
	)
	flag.Parse()

	if *daemonEpoch == 0 {
		*daemonEpoch = time.Now().Unix()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := runtime.DaemonConfig{
		DataDir:     *dataDir,
		ChannelsDir: filepath.Join(*dataDir, "channels"),
		DaemonID:    *daemonID,
		DaemonEpoch: *daemonEpoch,
		UseMockBus:  *mockBus,
	}
	if err := runtime.RunDaemon(ctx, cfg); err != nil {
		log.Fatalf("daemon exit: %v", err)
	}
}

func defaultDataDir() string {
	if v := os.Getenv("COAGENT_DATA_DIR"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".coagent"
	}
	return fmt.Sprintf("%s/.coagent", home)
}
