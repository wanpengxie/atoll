// Command daemon is the M1.3 production daemon — the Go re-implementation
// of the lightcone daemon (replacing lightcone/daemon/src/index.js once
// M1.3 closes).
//
// All of the composition root wiring lives in server.go (Run + Config);
// this file is the thin process entrypoint that hands signal handling
// to that body so tests can drive Run directly without exec'ing a
// child binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// Version is the daemon-go binary version. Bumped per ticket.
const Version = "0.1.0-t101"

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintln(os.Stderr, "daemon:", err)
		os.Exit(1)
	}
}

// mainErr is the testable shell. main wraps it to translate errors into
// a non-zero exit code. Returning an error from mainErr keeps `defer`
// cleanup orderly (signal.Stop runs even on early-exit paths).
func mainErr() error {
	cfg, err := parseFlags(os.Args[1:], os.Getenv)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	cfg.Logger = logger

	logger.Info("daemon.boot",
		"event", "daemon.boot",
		"version", Version,
		"pid", os.Getpid(),
		"http_listen", cfg.HTTPListen,
		"daemon_db", cfg.DaemonDBPath,
		"channel_root", cfg.ChannelRoot,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := Run(ctx, cfg); err != nil {
		return err
	}
	return nil
}

// parseFlags reads command-line flags into a Config, applying env
// fallbacks for every flag the ops playbook expects.
//
// Flag table (kept in sync with docs/deployment-cutover.md Phase 3):
//
//	--daemon-db        path to daemon-level sqlite (default ${COAGENT_HOME}/daemon.sqlite)
//	--channel-root     dir under which each channel's workdir lives
//	                   (default ${COAGENT_HOME}/channels)
//	--http-listen      bind address (default :3101)
//	--auth-token       shared bearer token for daemon_rpc + view.resync_channel
//	                   + xhs callback. Falls back to env COAGENT_AUTH_TOKEN.
//	--worker-binary    path to the worker binary spawned by supervisor.
//	                   Defaults to "${dirname(daemon_binary)}/worker".
//	                   When empty, supervisor loops are skipped (smoke-friendly).
//	--server-url       optional view-sync server origin (e.g. http://server:3001).
//	                   Empty → view-sync push disabled.
//	--xhs-device-id    optional default device_id forwarded to the xhs adapter.
//	--home             convenience flag that defaults daemon-db + channel-root
//	                   under <home>/. Defaults to env COAGENT_HOME / ${HOME}/.coagent.
func parseFlags(args []string, getenv func(string) string) (Config, error) {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)

	home := fs.String("home", "", "Optional COAGENT_HOME (default: env or ~/.coagent)")
	daemonDB := fs.String("daemon-db", "", "Path to daemon-level sqlite (default: <home>/daemon.sqlite)")
	channelRoot := fs.String("channel-root", "", "Channel workdir root (default: <home>/channels)")
	httpListen := fs.String("http-listen", ":3101", "HTTP bind address")
	authToken := fs.String("auth-token", "", "Shared bearer token (fallback env COAGENT_AUTH_TOKEN)")
	workerBin := fs.String("worker-binary", "", "Worker binary path (default: <daemon-binary-dir>/worker; empty = supervisor disabled)")
	serverURL := fs.String("server-url", "", "View-sync server URL (empty = view-sync push disabled)")
	xhsDevice := fs.String("xhs-device-id", "", "Default device_id forwarded to xhs adapter")
	sched := fs.Duration("scheduler-period", time.Second, "Long-pending + future scheduler tick period")
	supPeriod := fs.Duration("supervisor-period", 10*time.Second, "Supervisor scan period")
	leaseTTL := fs.Int64("lease-ttl", 60, "Worker lease TTL seconds")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	resolvedHome := *home
	if resolvedHome == "" {
		resolvedHome = getenv("COAGENT_HOME")
	}
	if resolvedHome == "" {
		if h := getenv("HOME"); h != "" {
			resolvedHome = filepath.Join(h, ".coagent")
		}
	}

	cfg := Config{
		DaemonDBPath:       *daemonDB,
		ChannelRoot:        *channelRoot,
		HTTPListen:         *httpListen,
		AuthToken:          *authToken,
		WorkerBinaryPath:   *workerBin,
		ServerURL:          *serverURL,
		XHSDefaultDeviceID: *xhsDevice,
		SchedulerPeriod:    *sched,
		SupervisorPeriod:   *supPeriod,
		LeaseTTL:           *leaseTTL,
	}

	if cfg.DaemonDBPath == "" && resolvedHome != "" {
		cfg.DaemonDBPath = filepath.Join(resolvedHome, "daemon.sqlite")
	}
	if cfg.ChannelRoot == "" && resolvedHome != "" {
		cfg.ChannelRoot = filepath.Join(resolvedHome, "channels")
	}
	if cfg.AuthToken == "" {
		cfg.AuthToken = getenv("COAGENT_AUTH_TOKEN")
	}
	if cfg.WorkerBinaryPath == "" {
		if exe, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(exe), "worker")
			if _, statErr := os.Stat(candidate); statErr == nil {
				cfg.WorkerBinaryPath = candidate
			}
		}
	}
	return cfg, nil
}
