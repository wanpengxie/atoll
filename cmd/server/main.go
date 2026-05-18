// Package main is the cmd/server entry point — wires the gateway
// composition root, applies migrations, and serves HTTP + WS over
// the configured address.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/server/gateway"
)

// version is set via -ldflags at build time.
var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Fatalf("[server] fatal: %v", err)
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

	log.Printf("[server] coagent-server %s starting on %s (db=%s gin_mode=%s)",
		version, cfg.HTTPAddr, cfg.DBPath, gin.Mode())

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	app, err := gateway.New(ctx, gateway.Config{
		DBPath:                    cfg.DBPath,
		SessionSecret:             cfg.SessionSecret,
		DaemonSharedSecret:        cfg.DaemonSharedSecret,
		DeviceTokenSecret:         cfg.DeviceTokenSecret,
		HumanCallerSecret:         cfg.HumanCallerSecret,
		AllowDevSecrets:           cfg.AllowDevSecrets,
		ReconcileGracePeriod:      cfg.ReconcileGracePeriod,
		ReconcileCreateTimeout:    cfg.ReconcileCreateTimeout,
		ReconcileHeartbeatTimeout: cfg.ReconcileHeartbeatTimeout,
	})
	if err != nil {
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
		log.Printf("[server] http listen %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go app.RunReconcile(ctx)

	select {
	case <-ctx.Done():
		log.Printf("[server] signal received, shutting down")
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}

type config struct {
	HTTPAddr                  string
	DBPath                    string
	SessionSecret             string
	DaemonSharedSecret        string
	DeviceTokenSecret         string
	HumanCallerSecret         string
	AllowDevSecrets           bool
	ReconcileGracePeriod      time.Duration
	ReconcileCreateTimeout    time.Duration
	ReconcileHeartbeatTimeout time.Duration
}

func loadConfig() config {
	cfg := config{
		HTTPAddr: envOr("COAGENT_SERVER_ADDR", ":8080"),
		DBPath:   envOr("COAGENT_SERVER_DB", "data/server.db"),
		// FIX-T8: env defaults are empty so gateway.New fails fast when
		// the operator forgets to set the secrets. Pass --allow-dev-secrets
		// (or COAGENT_ALLOW_DEV_SECRETS=1) to fall back to the dev sentinels
		// with a startup warning.
		SessionSecret:             os.Getenv("COAGENT_SESSION_SECRET"),
		DaemonSharedSecret:        os.Getenv("COAGENT_DAEMON_SECRET"),
		DeviceTokenSecret:         os.Getenv("COAGENT_DEVICE_SECRET"),
		HumanCallerSecret:         os.Getenv("COAGENT_HUMAN_SECRET"),
		AllowDevSecrets:           os.Getenv("COAGENT_ALLOW_DEV_SECRETS") == "1",
		ReconcileGracePeriod:      60 * time.Second,
		ReconcileCreateTimeout:    30 * time.Second,
		ReconcileHeartbeatTimeout: 90 * time.Second,
	}

	flag.StringVar(&cfg.HTTPAddr, "addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to server sqlite database")
	flag.BoolVar(&cfg.AllowDevSecrets, "allow-dev-secrets", cfg.AllowDevSecrets,
		"Allow empty / dev-sentinel secrets (dev-only; required for the legacy --change-me defaults)")
	flag.DurationVar(&cfg.ReconcileGracePeriod, "reconcile-grace", cfg.ReconcileGracePeriod, "Cold start grace before stale reconcile begins")
	flag.DurationVar(&cfg.ReconcileCreateTimeout, "create-timeout", cfg.ReconcileCreateTimeout, "Placement creating→orphan timeout")
	flag.DurationVar(&cfg.ReconcileHeartbeatTimeout, "heartbeat-timeout", cfg.ReconcileHeartbeatTimeout, "Placement active→stale heartbeat timeout")
	flag.Parse()

	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
