// Package main is the cmd/server entry point — wires the gateway
// composition root, applies migrations, and serves HTTP + WS over
// the configured address.
//
// Authoritative spec: launch-ticket notes §T6.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/pkg/logger"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/pkg/observability"
	"github.com/wanpengxie/ActOS/server/gateway"
)

// version is set via -ldflags at build time.
var version = "dev"

func main() {
	if err := run(); err != nil {
		// `run` already logged the detailed structured error; exit 1
		// with a final plain stderr line so non-JSON parsers in CI also
		// see what happened.
		fmt.Fprintf(os.Stderr, "[server] fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()

	// M1.6-T7 phase-1 — production default: GIN_MODE=release.
	// `--allow-dev-secrets` is the established dev / CI gate; reuse it to
	// flip gin back to debug (which prints the route table + per-request
	// logger). Production binaries default to release so:
	//   1. /api/* responses don't carry the gin debug banner;
	//   2. gin's per-request `Logger` middleware is suppressed (we have
	//      our own structured handler logging downstream);
	//   3. there's no risk of leaking handler internals via the verbose
	//      console output.
	// COAGENT_GIN_MODE overrides (escape hatch for ops, e.g. forcing
	// debug in a staging deploy without touching CLI flags).
	applyGinMode(cfg.AllowDevSecrets)

	// M1.6-T7 phase-2 — structured JSON logger. `--allow-dev-secrets`
	// (matching the existing dev/prod gate) flips zerolog into pretty
	// ConsoleWriter for hand-readable dev output; prod stays on JSON
	// so logs can be shipped to loki / docker logs / etc. without
	// post-processing.
	lg := logger.New(logger.Config{
		Component: "server",
		Version:   version,
		Writer:    os.Stdout,
		Pretty:    cfg.AllowDevSecrets,
		Level:     os.Getenv("COAGENT_LOG_LEVEL"),
	})
	restore := lg.RedirectStdlib()
	defer restore()

	lg.Z().Info().
		Str("event", "server.starting").
		Str("addr", cfg.HTTPAddr).
		Str("debug_addr", cfg.DebugAddr).
		Str("db", cfg.DBPath).
		Str("gin_mode", gin.Mode()).
		Bool("allow_dev_secrets", cfg.AllowDevSecrets).
		Msg("coagent-server starting")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	app, err := gateway.New(ctx, gateway.Config{
		DBPath:                    cfg.DBPath,
		SessionSecret:             cfg.SessionSecret,
		DaemonSharedSecret:        cfg.DaemonSharedSecret,
		DeviceAllowedOrigins:      cfg.DeviceAllowedOrigins,
		DeviceAllowMissingOrigin:  cfg.DeviceAllowMissingOrigin,
		PushhubAllowedOrigins:     cfg.PushhubAllowedOrigins,
		DaemonbusAllowedOrigins:   cfg.DaemonbusAllowedOrigins,
		HumanCallerSecret:         cfg.HumanCallerSecret,
		BcryptCost:                cfg.BcryptCost,
		AllowDevSecrets:           cfg.AllowDevSecrets,
		UIDistDir:                 cfg.UIDistDir,
		ReconcileGracePeriod:      cfg.ReconcileGracePeriod,
		ReconcileCreateTimeout:    cfg.ReconcileCreateTimeout,
		ReconcileHeartbeatTimeout: cfg.ReconcileHeartbeatTimeout,
	})
	if err != nil {
		lg.Z().Error().Err(err).Str("event", "server.gateway_init_failed").Msg("gateway init failed")
		return fmt.Errorf("gateway init: %w", err)
	}
	defer func() { _ = app.Close() }()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           app.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		lg.Z().Info().Str("event", "server.http_listen").Str("addr", cfg.HTTPAddr).Msg("http listen")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	var debugSrv *http.Server
	if cfg.DebugAddr != "" {
		debugSrv = observability.NewServer(cfg.DebugAddr, metrics.Default())
		go func() {
			lg.Z().Info().
				Str("event", "server.debug_listen").
				Str("addr", cfg.DebugAddr).
				Msg("debug listen")
			if err := debugSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("debug server: %w", err)
			}
		}()
	}

	go app.RunReconcile(ctx)

	select {
	case <-ctx.Done():
		lg.Z().Info().Str("event", "server.shutdown_signal").Msg("signal received, shutting down")
	case err := <-errCh:
		lg.Z().Error().Err(err).Str("event", "server.http_error").Msg("http server error")
		return fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		lg.Z().Error().Err(err).Str("event", "server.shutdown_error").Msg("http shutdown error")
		return fmt.Errorf("http shutdown: %w", err)
	}
	if debugSrv != nil {
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			lg.Z().Error().Err(err).Str("event", "server.debug_shutdown_error").Msg("debug shutdown error")
			return fmt.Errorf("debug shutdown: %w", err)
		}
	}
	lg.Z().Info().Str("event", "server.stopped").Msg("server stopped cleanly")
	return nil
}

type config struct {
	HTTPAddr                  string
	DebugAddr                 string
	DBPath                    string
	SessionSecret             string
	DaemonSharedSecret        string
	DeviceAllowedOrigins      []string
	DeviceAllowMissingOrigin  bool
	PushhubAllowedOrigins     []string
	DaemonbusAllowedOrigins   []string
	HumanCallerSecret         string
	BcryptCost                int
	AllowDevSecrets           bool
	UIDistDir                 string
	ReconcileGracePeriod      time.Duration
	ReconcileCreateTimeout    time.Duration
	ReconcileHeartbeatTimeout time.Duration
}

func loadConfig() config {
	deviceOrigins := os.Getenv("COAGENT_DEVICEBUS_ALLOWED_ORIGINS")
	pushhubOrigins := os.Getenv("COAGENT_PUSHHUB_ALLOWED_ORIGINS")
	daemonbusOrigins := os.Getenv("COAGENT_DAEMONBUS_ALLOWED_ORIGINS")
	cfg := config{
		HTTPAddr:  envOr("COAGENT_SERVER_ADDR", ":8832"),
		DebugAddr: envOr("COAGENT_DEBUG_ADDR", ":9090"),
		DBPath:    envOr("COAGENT_SERVER_DB", "data/server.db"),
		// FIX-T8: env defaults are empty so gateway.New fails fast when
		// the operator forgets to set the secrets. Pass --allow-dev-secrets
		// (or COAGENT_ALLOW_DEV_SECRETS=1) to fall back to the dev sentinels
		// with a startup warning.
		SessionSecret:             os.Getenv("COAGENT_SESSION_SECRET"),
		DaemonSharedSecret:        os.Getenv("COAGENT_DAEMON_SECRET"),
		HumanCallerSecret:         os.Getenv("COAGENT_HUMAN_SECRET"),
		BcryptCost:                envInt("COAGENT_BCRYPT_COST", 0),
		AllowDevSecrets:           os.Getenv("COAGENT_ALLOW_DEV_SECRETS") == "1",
		DeviceAllowMissingOrigin:  envBool("COAGENT_DEVICEBUS_ALLOW_MISSING_ORIGIN"),
		UIDistDir:                 os.Getenv("COAGENT_UI_DIST"),
		ReconcileGracePeriod:      60 * time.Second,
		ReconcileCreateTimeout:    30 * time.Second,
		ReconcileHeartbeatTimeout: 90 * time.Second,
	}

	flag.StringVar(&cfg.HTTPAddr, "addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&cfg.DebugAddr, "debug-addr", cfg.DebugAddr, "Debug listen address for non-contract /metrics and /debug/pprof endpoints; empty disables")
	flag.StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to server sqlite database")
	flag.StringVar(&cfg.UIDistDir, "ui-dist", cfg.UIDistDir,
		"Path to ui/dist directory; when set, served as SPA at /. Empty (default) = API only.")
	flag.BoolVar(&cfg.AllowDevSecrets, "allow-dev-secrets", cfg.AllowDevSecrets,
		"Allow empty / dev-sentinel secrets (dev-only; required for the legacy --change-me defaults)")
	flag.StringVar(&deviceOrigins, "devicebus-allowed-origins", deviceOrigins,
		"Comma-separated exact Origin allowlist for /devicebus WebSocket handshakes")
	flag.BoolVar(&cfg.DeviceAllowMissingOrigin, "devicebus-allow-missing-origin", cfg.DeviceAllowMissingOrigin,
		"Allow /devicebus WebSocket handshakes with no Origin header (non-browser clients only)")
	flag.StringVar(&pushhubOrigins, "pushhub-allowed-origins", pushhubOrigins,
		"Comma-separated exact Origin allowlist for /ws WebSocket handshakes")
	flag.StringVar(&daemonbusOrigins, "daemonbus-allowed-origins", daemonbusOrigins,
		"Comma-separated exact Origin allowlist for /daemonbus WebSocket handshakes")
	flag.IntVar(&cfg.BcryptCost, "bcrypt-cost", cfg.BcryptCost,
		"Bcrypt cost for password hashes (production default is 10; low values require --allow-dev-secrets)")
	flag.DurationVar(&cfg.ReconcileGracePeriod, "reconcile-grace", cfg.ReconcileGracePeriod, "Cold start grace before stale reconcile begins")
	flag.DurationVar(&cfg.ReconcileCreateTimeout, "create-timeout", cfg.ReconcileCreateTimeout, "Placement creating→orphan timeout")
	flag.DurationVar(&cfg.ReconcileHeartbeatTimeout, "heartbeat-timeout", cfg.ReconcileHeartbeatTimeout, "Placement active→stale heartbeat timeout")
	flag.Parse()

	cfg.DeviceAllowedOrigins = splitCSV(deviceOrigins)
	cfg.PushhubAllowedOrigins = splitCSV(pushhubOrigins)
	cfg.DaemonbusAllowedOrigins = splitCSV(daemonbusOrigins)
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// applyGinMode sets gin.Mode() according to the production / dev gate.
//
// Precedence (highest first):
//  1. COAGENT_GIN_MODE env override (`release` / `debug` / `test`) — escape
//     hatch for ops; mostly unused.
//  2. GIN_MODE env (gin's built-in convention) — respected verbatim so
//     operators can still flip the toggle from the standard knob.
//  3. allowDevSecrets=true → debug (matches the dev-friendly default the
//     `--allow-dev-secrets` gate already enables for secrets).
//  4. otherwise → release (production default).
//
// Returns the resolved mode so callers can log it.
func applyGinMode(allowDevSecrets bool) string {
	if v := strings.TrimSpace(os.Getenv("COAGENT_GIN_MODE")); v != "" {
		gin.SetMode(v)
		return gin.Mode()
	}
	if v := strings.TrimSpace(os.Getenv("GIN_MODE")); v != "" {
		gin.SetMode(v)
		return gin.Mode()
	}
	if allowDevSecrets {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	return gin.Mode()
}
