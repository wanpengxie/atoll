// Package main wires the coagent daemon binary.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (acceptance gate #1
// — cmd/daemon can start; scans channels/ directory in T1.6 phase 1/2/3/4
// order) and FIX-T2 (operational loop assembly + WS client + write_message
// handler).
//
// Wiring (FIX-T2 spec):
//
//  1. Open daemon.sqlite (bootstrap_registry).
//  2. Reconciler.Run — roll back any in-progress crashed sagas.
//  3. lifecycle.Bootstrapper.LoadLocal — scan channels/ dir, refresh
//     daemon_epoch.
//  4. Connect to server via daemonbus transit:
//     - production: gorilla/websocket dialer (transit.WSClient).
//     - dev/test : in-process MockBus when --mock-bus is set.
//  5. ReportReclaim → MarkRecovering → MarkAcceptingNew (Phase 3 starts
//     the dispatcher + per-channel pusher + scheduler goroutines).
//  6. Block until shutdown signal.
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

	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

func main() {
	var (
		dataDir     = flag.String("data-dir", defaultDataDir(), "daemon data directory")
		daemonID    = flag.String("daemon-id", "daemon-local", "stable daemon identifier")
		daemonEpoch = flag.Int64("daemon-epoch", 0, "daemon process epoch (0 = use unix-second)")
		mockBus     = flag.Bool("mock-bus", false, "use in-process mock bus (dev only; production uses --server-url WS)")
		serverURL   = flag.String("server-url", "", "daemonbus WS URL, e.g. ws://localhost:8080/api/daemonbus")
		daemonKey   = flag.String("key", "", "shared key for daemonbus auth (must match server.daemonbus.SharedSecret)")
		humanSecret = flag.String("human-caller-secret", "",
			"HMAC secret matching server.gateway.HumanCallerSecret; "+
				"required when the daemon should accept control.write_message frames")
		replayWindowMs = flag.Int64("replay-window-ms", 0,
			"reject control.write_message frames whose ts differs from now() by more than this many milliseconds (0 = disabled)")
		host    = flag.String("host", "", "optional host metadata reported to the daemonbus registry")
		version = flag.String("version", "", "optional version metadata reported to the daemonbus registry")
	)
	flag.Parse()

	if *daemonEpoch == 0 {
		*daemonEpoch = time.Now().Unix()
	}

	// M1.6-T2 — wire the in_process xhs scaffold into every booted
	// channel. ChannelTemplate seeds the actor_registry row up-front so
	// framework.Manager.Install can find it; OnChannelBoot constructs
	// the Manager + installs the module + registers the Deliverer
	// handler. T3 swaps this for the adapters/device/xhs factory when
	// DeviceTransit is wired.
	cfg := runtime.DaemonConfig{
		DataDir:      *dataDir,
		ChannelsDir:  filepath.Join(*dataDir, "channels"),
		DaemonID:     *daemonID,
		DaemonEpoch:  *daemonEpoch,
		UseMockBus:   *mockBus,
		ReplayWindow: time.Duration(*replayWindowMs) * time.Millisecond,
		ChannelTemplate: runtime.ChannelTemplate{
			AdapterActorSeeds: []actor.Record{xhs.DefaultActorSeed()},
		},
		OnChannelBoot: wireAdapterFramework(XHSScaffoldFactory(xhs.Config{})),
	}

	if !*mockBus {
		if *serverURL == "" || *daemonKey == "" {
			log.Fatal("daemon: --server-url and --key are required when --mock-bus=false")
		}
		// T0.5 — production wiring MUST carry the shared HMAC secret used
		// to authenticate human-caller tokens; without it the
		// control.write_message handler is silently skipped and POST
		// /api/channels/:id/messages returns 'no daemon for channel'.
		// Fail-fast so misconfiguration is loud instead of stealthy.
		if *humanSecret == "" {
			log.Fatal("daemon: --human-caller-secret required when --mock-bus=false")
		}
		cfg.WSConfig = &transit.WSClientConfig{
			URL:      *serverURL,
			DaemonID: *daemonID,
			Key:      *daemonKey,
			Host:     *host,
			Version:  *version,
		}
	}

	if *humanSecret != "" {
		cfg.HumanCallerSecret = []byte(*humanSecret)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
