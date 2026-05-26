// Package main wires the coagent daemon binary.
//
// Authoritative spec: launch-ticket notes §T3 (acceptance gate #1
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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/pkg/logger"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/pkg/observability"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/transit"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
)

// version is set via -ldflags at build time.
var version = "dev"

const defaultReplayWindowMs int64 = 300_000

func main() {
	var (
		dataDir     = flag.String("data-dir", defaultDataDir(), "daemon data directory")
		daemonID    = flag.String("daemon-id", "daemon-local", "stable daemon identifier")
		daemonEpoch = flag.Int64("daemon-epoch", 0, "daemon process epoch (0 = use unix-second)")
		mockBus     = flag.Bool("mock-bus", false, "use in-process mock bus (dev only; production uses --server-url WS)")
		serverURL   = flag.String("server-url", "", "daemonbus WS URL, e.g. ws://localhost:8832/api/daemonbus")
		daemonKey   = flag.String("key", "", "shared key for daemonbus auth (must match server.daemonbus.SharedSecret)")
		humanSecret = flag.String("human-caller-secret", "",
			"HMAC secret matching server.gateway.HumanCallerSecret; "+
				"required when the daemon should accept control.write_message frames")
		replayWindowMs = flag.Int64("replay-window-ms", defaultReplayWindowMs,
			"reject control.write_message frames whose ts differs from now() by more than this many milliseconds (0 = disabled; mock-bus/dev only)")
		host         = flag.String("host", "", "optional host metadata reported to the daemonbus registry")
		versionFlag  = flag.String("version", "", "optional version metadata reported to the daemonbus registry")
		capacity     = flag.Int("capacity", 0, "optional max active channel capacity reported to the daemonbus registry")
		allowDevMode = flag.Bool("allow-dev-secrets", false,
			"dev mode: pretty-printed console logs + relax --key / --human-caller-secret requirement when paired with --mock-bus")
		useScaffoldXHS = flag.Bool("use-scaffold-xhs", false,
			"dev/test fallback: install the in-process xhs scaffold instead of the production device transit adapter")
		workerBin = flag.String("worker-bin", defaultWorkerBin(),
			"path to the coagent-worker subprocess binary; empty disables worker spawning (channel-agent triggers become no-op)")
		workerProvider = flag.String("worker-provider", envOrDefault("COAGENT_WORKER_PROVIDER", "kimi"),
			"value passed as --provider to spawned workers (mock|kimi). Also via COAGENT_WORKER_PROVIDER env.")
		debugAddr = flag.String("debug-addr", envOrDefault("COAGENT_DAEMON_DEBUG_ADDR", ":9091"),
			"Debug listen address for non-contract /metrics and /debug/pprof endpoints; empty disables")
	)
	flag.Parse()

	if *daemonEpoch == 0 {
		*daemonEpoch = time.Now().Unix()
	}

	// M1.6-T7 phase-2 — structured logger first so every subsequent
	// failure path emits JSON instead of a bare stdlib log line.
	lg := logger.New(logger.Config{
		Component: "daemon",
		Version:   version,
		Writer:    os.Stdout,
		Pretty:    *allowDevMode,
		Level:     os.Getenv("COAGENT_LOG_LEVEL"),
	})
	restore := lg.RedirectStdlib()
	defer restore()

	// M1.6-T7 phase-2 — production fail-fast on missing secrets, matching
	// cmd/server's gateway.withDefaults gate. `--mock-bus` keeps the
	// dev-only loopback path open without requiring real keys.
	if !*mockBus {
		if *serverURL == "" || *daemonKey == "" {
			lg.Z().Error().Str("event", "daemon.fail_fast").
				Msg("--server-url and --key are required when --mock-bus=false")
			os.Exit(1)
		}
		// T0.5 — production wiring MUST carry the shared HMAC secret used
		// to authenticate human-caller tokens; without it the
		// control.write_message handler is silently skipped and POST
		// /api/channels/:id/messages returns 'no daemon for channel'.
		// Fail-fast so misconfiguration is loud instead of stealthy.
		if *humanSecret == "" {
			lg.Z().Error().Str("event", "daemon.fail_fast").
				Msg("--human-caller-secret required when --mock-bus=false")
			os.Exit(1)
		}
		if *replayWindowMs <= 0 {
			lg.Z().Error().Str("event", "daemon.fail_fast").
				Msg("--replay-window-ms must be > 0 when --mock-bus=false")
			os.Exit(1)
		}
	}

	// M1.6-T5 phase-2 — register both the legacy / generic "group"
	// template (empty seeds, no domain prompt) and the xhs-creator
	// template (tool:xhs-adapter actor seed + published-notes/drafts/
	// assets/ workdir + L4 §2.4 domain prompt). cmd/daemon converts
	// the adapter-owned xhs.Template into the runtime projection here
	// because the conversion is a composition-root concern (keeps
	// `runtime` independent of `adapters/**` per arch-lint).
	//
	if err := migrateLegacyDeviceMirrorFile(context.Background(), *dataDir, lg.Z()); err != nil {
		lg.Z().Error().Err(err).Str("event", "daemon.fail_fast").
			Msg("legacy device route mirror migration failed")
		os.Exit(1)
	}
	xhsFactory := DeviceXHSFactory(devicexhs.Config{})
	if *useScaffoldXHS {
		xhsFactory = XHSScaffoldFactory(xhs.Config{})
	}
	adapterCredentialSecret := []byte(*humanSecret)
	if len(adapterCredentialSecret) == 0 && *mockBus {
		adapterCredentialSecret = []byte(devAdapterCredentialSecret)
	}
	adapterBootHook, err := wireAdapterFrameworkWithCredentialSecret(adapterCredentialSecret, xhsFactory)
	if err != nil {
		lg.Z().Error().Err(err).Str("event", "daemon.fail_fast").
			Msg("adapter credential encryption key is required")
		os.Exit(1)
	}
	daemonLogger := lg.Z()
	cfg := runtime.DaemonConfig{
		DataDir:                   *dataDir,
		ChannelsDir:               filepath.Join(*dataDir, "channels"),
		DaemonID:                  *daemonID,
		DaemonEpoch:               *daemonEpoch,
		UseMockBus:                *mockBus,
		ReplayWindow:              time.Duration(*replayWindowMs) * time.Millisecond,
		AllowReplayWindowDisabled: *mockBus && *replayWindowMs <= 0,
		ChannelTemplates:          buildChannelTemplates(*useScaffoldXHS),
		OnChannelBoot:             adapterBootHook,
		Logger:                    daemonLogger,
	}

	if !*mockBus {
		cfg.WSConfig = &transit.WSClientConfig{
			URL:      *serverURL,
			DaemonID: *daemonID,
			Key:      *daemonKey,
			Host:     *host,
			Version:  *versionFlag,
			Capacity: *capacity,
		}
	}

	if *humanSecret != "" {
		cfg.HumanCallerSecret = []byte(*humanSecret)
	}

	// M1.6-T1 P4 — wire the worker subprocess spawner so channel-agent
	// trigger envelopes reach a real worker (mock or kimi). When
	// --worker-bin is empty (or the binary is missing on disk at boot)
	// we fall back to the P2 counter-stub handler by leaving
	// cfg.WorkerSpawner nil; that path is silent (handler is no-op) but
	// at least the daemon boots — useful for smoke tests where the
	// worker binary is not built yet.
	if *workerBin != "" {
		args := []string{"--provider=" + *workerProvider}
		cfg.WorkerSpawner = &workerhost.ExecSpawner{
			BinaryPath: *workerBin,
			Args:       args,
			// Env left nil — ExecSpawner already inherits os.Environ()
			// at Spawn time (KIMI_*, COAGENT_* propagate naturally).
		}
		lg.Z().Info().
			Str("event", "daemon.worker_spawner_wired").
			Str("worker_bin", *workerBin).
			Str("worker_provider", *workerProvider).
			Msg("workerhost.ExecSpawner installed")
	} else {
		lg.Z().Warn().
			Str("event", "daemon.worker_spawner_disabled").
			Msg("--worker-bin empty; channel-agent triggers will be no-op")
	}

	lg.Z().Info().
		Str("event", "daemon.starting").
		Str("daemon_id", *daemonID).
		Int64("daemon_epoch", *daemonEpoch).
		Str("data_dir", *dataDir).
		Bool("mock_bus", *mockBus).
		Bool("use_scaffold_xhs", *useScaffoldXHS).
		Msg("coagent-daemon starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var debugSrv *http.Server
	if *debugAddr != "" {
		debugSrv = observability.NewServer(*debugAddr, metrics.Default())
		go func() {
			lg.Z().Info().
				Str("event", "daemon.debug_listen").
				Str("addr", *debugAddr).
				Msg("debug listen")
			if err := debugSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				lg.Z().Error().Err(err).Str("event", "daemon.debug_error").Msg("debug server error")
				os.Exit(1)
			}
		}()
	}

	if err := runtime.RunDaemon(ctx, cfg); err != nil {
		lg.Z().Error().Err(err).Str("event", "daemon.exit_error").Msg("daemon exited with error")
		os.Exit(1)
	}
	if debugSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			lg.Z().Error().Err(err).Str("event", "daemon.debug_shutdown_error").Msg("debug shutdown error")
			os.Exit(1)
		}
	}
	lg.Z().Info().Str("event", "daemon.stopped").Msg("daemon stopped cleanly")
}

// defaultWorkerBin returns the worker subprocess path. Honours
// COAGENT_WORKER_BIN env so ops can override the location without
// shipping a flag — falls back to the make-build output (./bin/
// coagent-worker) which lives alongside the daemon binary in
// production deployments.
func defaultWorkerBin() string {
	if v := os.Getenv("COAGENT_WORKER_BIN"); v != "" {
		return v
	}
	return "./bin/coagent-worker"
}

// envOrDefault returns the env value (trimmed) when non-empty, else
// fallback. Inlined helper so cmd/daemon stays free of utility imports.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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

// buildChannelTemplates assembles the DaemonConfig.ChannelTemplates map
// (M1.6-T5 phase-2). The map keys are catalog.Channel.Type values:
//
//   - ""          legacy / unspecified channels — no template seeds.
//   - "group"     generic group chat — no template seeds.
//   - "xhs-creator" domain-xhs xhs-creator template (§2): seeds
//     tool:xhs-adapter into actor_registry, mkdirs published-notes/ /
//     drafts/ / assets/ inside the channel workdir, and ships the
//     §2.4 domain prompt segment for the worker spawn env (phase-3).
//
// The conversion from the adapter-owned xhs.Template to the runtime
// projection lives here because the composition root is the only layer
// that may import both `adapters/**` and `runtime/**` per arch-lint.
func buildChannelTemplates(useScaffoldXHS bool) map[string]runtime.ChannelTemplate {
	out := make(map[string]runtime.ChannelTemplate, 3)
	// Convention: HumanCaller (UI) message defaults route to the channel
	// agent for handling. Templates that need different routing override
	// HumanCallerDefaultAudience.
	defaultHumanAudience := []actor.ActorID{runtime.ChannelAgentID}

	// Empty + "group" share the generic no-template projection. We
	// register both so the daemon resolver returns a stable zero-value
	// for either key without falling through to the "" default twice.
	generic := runtime.ChannelTemplate{
		HumanCallerDefaultAudience: defaultHumanAudience,
	}
	out[""] = generic
	out["group"] = generic

	tpl := xhs.XHSCreatorTemplate()
	adapterSeeds := []actorreg.Record{DeviceXHSActorSeed()}
	if useScaffoldXHS {
		adapterSeeds = tpl.AdapterActorSeeds
	}
	out[tpl.ChannelType] = runtime.ChannelTemplate{
		AdapterActorSeeds:          adapterSeeds,
		WorkdirSubdirs:             tpl.WorkdirSubdirs,
		DomainPrompt:               tpl.DomainPrompt,
		HumanCallerDefaultAudience: defaultHumanAudience,
	}
	return out
}
