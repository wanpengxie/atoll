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
	"syscall"
	"time"

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

	log.Printf("[server] coagent-server %s starting on %s (db=%s)", version, cfg.HTTPAddr, cfg.DBPath)

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
