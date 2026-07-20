// Package app is the reference realm: principal identity, channel directory and
// lifecycle, daemon registry, and HTTP API. Per-channel truth belongs to the
// platform membrane.
package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// WSGateway is the human-ingress serving面 the assembly root (cmd/server) injects:
// the gateway 期 connector (drivers/gateway/connector/web) satisfies it. app → drivers
// is fenced, so the app names only this interface and cmd/server bridges the concrete
// connector in. The app membrane authenticates (session→principal ONLY — 连接即人, no
// connection-level channel ACL; channel eligibility is the gateway's live per-frame
// resolve) then hands the upgraded-pending request off. nil (unwired) → /ws answers 503.
type WSGateway interface {
	ServeWeb(w http.ResponseWriter, r *http.Request, principal string)
}

// App is the product application server.
type App struct {
	db     *sql.DB
	logger *slog.Logger
	engine *gin.Engine
	srv    *http.Server // set by Run; drained by Shutdown

	mu     sync.RWMutex
	host   channelhost.LocalHost
	uiDist string

	// wsGateway is the injected human-ingress connector (gateway 期 S3); membershipPoke
	// is the injected direct Gateway.Poke callback that the platform emission
	// points (home.Config.OnMembershipChange, wired in createHome — Admit/Remove) feed.
	// Both are set by the assembly root via SetGateway/SetMembershipPoke after New (the
	// gateway needs the app's routing/entitlement面, breaking the構造 cycle).
	wsGateway      WSGateway
	membershipPoke func(principal string)

	daemonLocks  *keyedLockSet
	channelLocks *keyedLockSet
	fanout       *fanoutWorker
	lifecycle    *lifecycleWorker
	admission    *admissionService
}

type HostFactory func(channelhost.HomeDeps) (channelhost.LocalHost, error)

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/atoll-dev/channels"
	HostFactory  HostFactory

	// UIDist is the on-disk path of the built web UI (the atoll-web repo's
	// dist/ — the UI lives in its own repository since the open-source split).
	// Empty = UI routes are not registered and the server is API-only.
	UIDist string
}

// New assembles the App: gin engine, routes, and loads existing channels.
func New(cfg Config) (*App, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	a := &App{
		db: cfg.DB, logger: logger, uiDist: cfg.UIDist,
		daemonLocks: newKeyedLockSet(), channelLocks: newKeyedLockSet(),
	}
	factory := cfg.HostFactory
	if factory == nil {
		// Transitional default for existing embedders; cmd/server supplies the
		// factory explicitly and Phase 5 removes this fallback.
		factory = func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(cfg.ChannelDBDir, deps)
		}
	}
	host, err := factory(channelhost.HomeDeps{
		CompositionResolver:  compositionResolver{app: a},
		IntroductionResolver: compositionResolver{app: a},
		Logger:               logger,
		OnMembershipChange: func(chID channel.ID, affected []string) {
			for _, principal := range affected {
				a.reconcilePrincipalChannel(context.Background(), chID, principal)
				if a.membershipPoke != nil {
					a.membershipPoke(principal)
				}
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("app: construct ChannelHost: %w", err)
	}
	a.host = host

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.CORS())

	a.engine = engine
	a.registerRoutes()

	// Load existing channels from DB.
	if err := a.loadChannels(); err != nil {
		return nil, fmt.Errorf("app: load channels: %w", err)
	}
	a.fanout = newFanoutWorker(a)
	a.fanout.start()
	a.admission = newAdmissionService(a)
	a.admission.start()
	a.lifecycle = newLifecycleWorker(a)
	a.lifecycle.start()

	return a, nil
}

// SetGateway injects the human-ingress connector (gateway 期 S3). The assembly
// root calls it after New — the gateway is constructed with the app's routing面,
// so the app cannot hold it at New time (construction cycle). /ws answers 503 until
// it is set.
func (a *App) SetGateway(g WSGateway) { a.wsGateway = g }

// SetMembershipPoke injects Gateway.Poke directly. createHome
// forwards it into each home's home.Config.OnMembershipChange (Admit/Remove emit points).
// nil → no live poke (reconnect re-auth + the resolver's每批 recheck / sweep remain the
// correctness正门 — a lost poke only delays convergence).
func (a *App) SetMembershipPoke(fn func(principal string)) { a.membershipPoke = fn }

// Run starts the HTTP server and blocks until it is Shutdown (or errors). It
// holds an explicit http.Server so cmd can drain in-flight requests on signal;
// a clean Shutdown returns nil (ErrServerClosed is not an error).
func (a *App) Run(addr string) error {
	a.mu.Lock()
	a.srv = &http.Server{Addr: addr, Handler: a.engine}
	srv := a.srv
	a.mu.Unlock()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown stops accepting new connections and drains in-flight requests within
// ctx's deadline. It is step ① of the graceful teardown (before Close): stop the
// entry before dismantling the homes behind it.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.RLock()
	srv := a.srv
	a.mu.RUnlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// Close tears down all channel homes. 锁纪律 (连接模型勘误期 §3.2 P1-6): snapshot +
// clear the map UNDER a.mu, then Close each home OUTSIDE the lock — a home.Close held
// under a.mu would block every concurrent getHome (a.mu.RLock), and the entitlement
// resolver's bounded read (T_read) cannot cancel a lock wait.
func (a *App) Close() error {
	if a.lifecycle != nil {
		a.lifecycle.close()
	}
	if a.admission != nil {
		a.admission.close()
	}
	if a.fanout != nil {
		a.fanout.close()
	}
	return a.host.Close()
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

func (a *App) registerRoutes() {
	// Identity (no auth required).
	identity := a.engine.Group("/api/identity")
	{
		identity.POST("/register", a.handleRegister)
		identity.POST("/login", a.handleLogin)
		identity.POST("/logout", a.handleLogout)
		// /me is the frontend's "am I logged in?" probe — it requires a valid
		// session, so it carries the Auth guard directly (logout above stays
		// public: it must clear the cookie even for an already-expired session).
		identity.GET("/me", middleware.Auth(a.db), a.handleMe)
		identity.POST("/verification/issue", a.handleVerificationIssue)
	}

	// Authenticated API routes.
	api := a.engine.Group("/api")
	api.Use(middleware.Auth(a.db))
	{
		api.GET("/channels", a.handleListChannels)
		api.POST("/channels", a.handleCreateChannel)
		api.GET("/channels/:chID", a.handleGetChannel)
		api.DELETE("/channels/:chID", a.handleDeleteChannel)
		api.POST("/channels/:chID/join", a.handleJoinChannel)
		api.POST("/channels/:chID/actors", a.handleIntroduceActor)
		api.PUT("/channels/:chID/actors/:actorID/config", a.handleEditActorConfig)
		api.GET("/channels/:chID/candidates", a.handleListCandidates)
		api.GET("/operations/:ref", a.handleGetOperation)
		// A user's actor-instance declarations (world layer, kind-neutral).
		api.GET("/actor-decls", a.handleListDecls)
		api.POST("/actor-decls", a.handleCreateDecl)
		api.PATCH("/actor-decls/:declID", a.handleUpdateDecl)
		api.DELETE("/actor-decls/:declID", a.handleDeleteDecl)

		api.GET("/daemons", a.handleListDaemons)
		api.POST("/daemons", a.handleCreateDaemon)
		api.DELETE("/daemons/:id", a.handleDeleteDaemon)

		api.GET("/channels/:chID/daemons", a.handleListChannelDaemons)
		api.POST("/channels/:chID/daemons", a.handleAttachDaemon)
		api.DELETE("/channels/:chID/daemons/:id", a.handleDetachDaemon)
	}

	// Health check (no auth).
	a.engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// WebSocket endpoints.
	a.engine.GET("/ws", a.handleWS)
	a.engine.GET("/compute", a.handleCompute)
	// Static files — only when a built UI is supplied (the UI lives in its
	// own repository, atoll-web; empty UIDist = API-only server).
	if a.uiDist != "" {
		a.engine.Static("/assets", filepath.Join(a.uiDist, "assets"))
		a.engine.StaticFile("/favicon.svg", filepath.Join(a.uiDist, "favicon.svg"))
	}
	a.engine.NoRoute(func(c *gin.Context) {
		// For non-API, non-asset paths, serve index.html (SPA fallback).
		if a.uiDist != "" &&
			!strings.HasPrefix(c.Request.URL.Path, "/api/") &&
			!strings.HasPrefix(c.Request.URL.Path, "/ws") &&
			!strings.HasPrefix(c.Request.URL.Path, "/compute") {
			c.File(filepath.Join(a.uiDist, "index.html"))
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	})
}

// ---------------------------------------------------------------------------
// Channel home management
// ---------------------------------------------------------------------------

func (a *App) getHome(chID channel.ID) *home.Home {
	h, _ := a.host.Borrow(chID)
	return h
}

func (a *App) snapshotHomes(ctx context.Context) map[channel.ID]*home.Home {
	out := make(map[channel.ID]*home.Home)
	rows, err := a.db.QueryContext(ctx, `SELECT id FROM channels`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if rows.Scan(&raw) == nil {
			id := channel.ID(raw)
			if h := a.getHome(id); h != nil {
				out[id] = h
			}
		}
	}
	return out
}

// homeOrError resolves the open Home for chID, or writes the honest two-state
// error to c and returns nil (A-P8):
//   - the directory (channels table) has NO such channel → 404 (permanent).
//   - the directory HAS it but its universe is not open (getHome==nil) → 503
//     "channel unavailable" (retryable, logged) — never the misleading 404 that
//     conflated "gone" with "not up yet".
//
// The two states must not collapse: a caller retrying a 503 is right to; a caller
// retrying a 404 is not. Every handler that needs a live Home routes through here.
func (a *App) homeOrError(c *gin.Context, chID channel.ID) *home.Home {
	if home := a.getHome(chID); home != nil {
		return home
	}
	if !a.channelExists(c.Request.Context(), string(chID)) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return nil
	}
	a.logger.Warn("channel unavailable: directory has channel but its home is not open",
		"channel", string(chID))
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
	return nil
}

const (
	defaultAgentPrincipal = "boost"
	// defaultBoostClass is the engine CLASS the always-there boost floor runs.
	// An agent's engine IS its actor class — claude/go-kimi are flat registry
	// classes (kind=agent), there is NO umbrella "agent" class. boost has no
	// actor_decls declaration row, so it can't carry a default_class; it runs
	// this fixed fallback engine.
	defaultBoostClass = "go-kimi"
	// placementServer marks a composition instance the SERVER hosts (embedded
	// cell). The reconcile ring only embodies these; daemon-placed rows are pulled
	// by the daemon's own plan.
	placementServer = "server"
	// placementDaemon marks a composition instance a connected DAEMON hosts. The
	// server never spawns these; the daemon pulls them over its bound link and
	// builds them with its LOCAL creds.
	placementDaemon = "daemon"
)
