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

	"github.com/coagent-ai/coagent/server/gateway"
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
	ReconcileGracePeriod      time.Duration
	ReconcileCreateTimeout    time.Duration
	ReconcileHeartbeatTimeout time.Duration
}

func loadConfig() config {
	cfg := config{
		HTTPAddr:                  envOr("COAGENT_SERVER_ADDR", ":8080"),
		DBPath:                    envOr("COAGENT_SERVER_DB", "data/server.db"),
		SessionSecret:             envOr("COAGENT_SESSION_SECRET", "dev-session-secret-change-me"),
		DaemonSharedSecret:        envOr("COAGENT_DAEMON_SECRET", "dev-daemon-secret-change-me"),
		DeviceTokenSecret:         envOr("COAGENT_DEVICE_SECRET", "dev-device-secret-change-me"),
		HumanCallerSecret:         envOr("COAGENT_HUMAN_SECRET", "dev-human-caller-secret-change-me"),
		ReconcileGracePeriod:      60 * time.Second,
		ReconcileCreateTimeout:    30 * time.Second,
		ReconcileHeartbeatTimeout: 90 * time.Second,
	}

	flag.StringVar(&cfg.HTTPAddr, "addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to server sqlite database")
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
