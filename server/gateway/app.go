// Package gateway is the server composition root — wires identity /
// catalog / placements / viewcache / daemonbus / devicebus / pushhub
// into one gin Engine and runs the background reconcile / expire
// sweeps.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6.
package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/coagent-ai/coagent/server/catalog"
	"github.com/coagent-ai/coagent/server/daemonbus"
	"github.com/coagent-ai/coagent/server/devicebus"
	"github.com/coagent-ai/coagent/server/identity"
	"github.com/coagent-ai/coagent/server/placements"
	"github.com/coagent-ai/coagent/server/pushhub"
	"github.com/coagent-ai/coagent/server/store"
	"github.com/coagent-ai/coagent/server/viewcache"
)

// Config bundles the construction-time settings.
type Config struct {
	DBPath                    string
	SessionSecret             string
	DaemonSharedSecret        string
	DeviceTokenSecret         string
	HumanCallerSecret         string
	ReconcileGracePeriod      time.Duration
	ReconcileCreateTimeout    time.Duration
	ReconcileHeartbeatTimeout time.Duration

	// DB overrides DBPath when non-nil (tests).
	DB *sql.DB
}

// App is the server façade. cmd/server holds one.
type App struct {
	cfg    Config
	db     *sql.DB
	closer func() error
	engine *gin.Engine

	identity   *identity.Service
	catalog    *catalog.Service
	placements *placements.Service
	viewcache  *viewcache.Service
	pushhub    *pushhub.Hub
	daemonbus  *daemonbus.Service
	devicebus  *devicebus.Service
}

// New builds a composed App.
func New(ctx context.Context, cfg Config) (*App, error) {
	cfg = withDefaults(cfg)

	var (
		db     *sql.DB
		closer func() error
		err    error
	)
	if cfg.DB != nil {
		db = cfg.DB
		closer = func() error { return nil }
	} else {
		db, err = store.Open(ctx, cfg.DBPath, store.OpenOptions{})
		if err != nil {
			return nil, fmt.Errorf("open db: %w", err)
		}
		closer = db.Close
	}

	app := &App{
		cfg:    cfg,
		db:     db,
		closer: closer,
	}
	app.identity = identity.NewService(db, identity.Config{
		SessionSecret: cfg.SessionSecret,
	})
	app.catalog = catalog.NewService(db)
	app.placements = placements.NewService(db, placements.Config{
		GracePeriod:      cfg.ReconcileGracePeriod,
		CreateTimeout:    cfg.ReconcileCreateTimeout,
		HeartbeatTimeout: cfg.ReconcileHeartbeatTimeout,
	})
	app.viewcache = viewcache.NewService(db)
	app.pushhub = pushhub.NewHub()
	app.daemonbus = daemonbus.NewService(db, daemonbus.Config{
		SharedSecret: cfg.DaemonSharedSecret,
	})
	app.devicebus = devicebus.NewService(db, devicebus.Config{
		TokenSecret: cfg.DeviceTokenSecret,
	})

	// Wire viewcache → daemon resync via daemonbus.
	app.viewcache.SetResyncer(&busResyncer{bus: app.daemonbus, viewcache: app.viewcache})

	app.engine = buildEngine(app)
	return app, nil
}

// Close releases the database handle.
func (a *App) Close() error {
	if a.closer != nil {
		return a.closer()
	}
	return nil
}

// Handler returns the http.Handler for cmd/server.
func (a *App) Handler() http.Handler { return a.engine }

// Identity / Catalog / Placements / Viewcache / Daemonbus / Devicebus
// / Pushhub: test helpers.
func (a *App) Identity() *identity.Service   { return a.identity }
func (a *App) Catalog() *catalog.Service     { return a.catalog }
func (a *App) Placements() *placements.Service { return a.placements }
func (a *App) Viewcache() *viewcache.Service { return a.viewcache }
func (a *App) Daemonbus() *daemonbus.Service { return a.daemonbus }
func (a *App) Devicebus() *devicebus.Service { return a.devicebus }
func (a *App) Pushhub() *pushhub.Hub         { return a.pushhub }
func (a *App) DB() *sql.DB                   { return a.db }

// RunReconcile blocks until ctx is cancelled, running the placements
// reconcile loop.
func (a *App) RunReconcile(ctx context.Context) {
	a.placements.RunReconcile(ctx)
}

func withDefaults(cfg Config) Config {
	if cfg.ReconcileGracePeriod <= 0 {
		cfg.ReconcileGracePeriod = 60 * time.Second
	}
	if cfg.ReconcileCreateTimeout <= 0 {
		cfg.ReconcileCreateTimeout = 30 * time.Second
	}
	if cfg.ReconcileHeartbeatTimeout <= 0 {
		cfg.ReconcileHeartbeatTimeout = 90 * time.Second
	}
	if cfg.SessionSecret == "" {
		cfg.SessionSecret = "dev-session-secret-change-me"
	}
	if cfg.DaemonSharedSecret == "" {
		cfg.DaemonSharedSecret = "dev-daemon-secret-change-me"
	}
	if cfg.DeviceTokenSecret == "" {
		cfg.DeviceTokenSecret = "dev-device-secret-change-me"
	}
	if cfg.HumanCallerSecret == "" {
		cfg.HumanCallerSecret = "dev-human-caller-secret-change-me"
	}
	return cfg
}
