// Package app is the reference realm: principal identity, channel directory and
// lifecycle, daemon registry, and HTTP API. Per-channel truth belongs to the
// platform membrane.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
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
	// points (home.Config.OnMembershipChange, wired by ChannelHost) feed.
	// Both are set by the assembly root via SetGateway/SetMembershipPoke after New (the
	// gateway needs the app's routing/entitlement面, breaking the構造 cycle).
	wsGateway      WSGateway
	membershipPoke func(principal string)

	daemonLocks  *keyedLockSet
	channelLocks *keyedLockSet
	lifecycle    *lifecycleWorker
	admission    *admissionService
}

type HostFactory func(channelhost.HomeDeps) (channelhost.LocalHost, error)

// Config configures the App.
type Config struct {
	DB          *sql.DB
	Logger      *slog.Logger
	HostFactory HostFactory

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
	if cfg.HostFactory == nil {
		return nil, errors.New("app: HostFactory required")
	}
	host, err := cfg.HostFactory(channelhost.HomeDeps{
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

	// Reconcile the directory with ChannelHost serving state.
	if err := a.reconcileServingChannels(context.Background()); err != nil {
		return nil, fmt.Errorf("app: load channels: %w", err)
	}
	a.sweepMembershipProjection(context.Background())
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

// SetMembershipPoke injects Gateway.Poke directly. ChannelHost forwards membership
// changes from every channel through the HomeDeps callback.
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
// entry before dismantling the serving bundles behind it.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.RLock()
	srv := a.srv
	a.mu.RUnlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// Close joins realm workers before transferring process teardown to ChannelHost,
// the sole owner of serving Home instances and their physical stores.
func (a *App) Close() error {
	if a.lifecycle != nil {
		a.lifecycle.close()
	}
	if a.admission != nil {
		a.admission.close()
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
		api.GET("/channels/:chID/observe", a.handleObserveChannel)
		api.GET("/channels/:chID/messages", a.handleListMessages)
		api.GET("/channels/:chID/resources", a.handleListResources)
		api.GET("/channels/:chID/resources/:rid", a.handleStatResource)
		api.GET("/channels/:chID/resources/:rid/bytes", a.handleFetchResource)
		api.DELETE("/channels/:chID", a.handleDeleteChannel)
		api.POST("/channels/:chID/join", a.handleJoinChannel)
		api.POST("/channels/:chID/actors", a.handleIntroduceActor)
		api.DELETE("/channels/:chID/actors/:actorID", a.handleRemoveChannelActor)
		api.PUT("/channels/:chID/decls/:declID/config", a.handlePutDeclarationOverlay)
		api.DELETE("/channels/:chID/decls/:declID/config", a.handleDeleteDeclarationOverlay)
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

var (
	errChannelNotFound    = errors.New("app: channel not found")
	errChannelUnavailable = errors.New("app: channel unavailable")
)

func (a *App) acquireBundle(ctx context.Context, chID channel.ID) (channelhost.Bundle, error) {
	if !a.channelExists(ctx, string(chID)) {
		return nil, errChannelNotFound
	}
	if bundle, ok := a.host.Acquire(chID); ok {
		return bundle, nil
	}
	return nil, errChannelUnavailable
}

func (a *App) snapshotBundles(ctx context.Context) (map[channel.ID]channelhost.Bundle, error) {
	out := make(map[channel.ID]channelhost.Bundle)
	ids, err := a.directoryChannelIDs(ctx)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if bundle, ok := a.host.Acquire(id); ok {
			out[id] = bundle
		} else {
			return nil, fmt.Errorf("%w: %s", errChannelUnavailable, id)
		}
	}
	return out, nil
}

// bundleOrError resolves the open Bundle for chID, or writes the honest two-state
// error to c and returns nil (A-P8):
//   - the directory (channels table) has NO such channel → 404 (permanent).
//   - the directory HAS it but ChannelHost cannot acquire a serving Bundle → 503
//     "channel unavailable" (retryable, logged) — never the misleading 404 that
//     conflated "gone" with "not up yet".
//
// The two states must not collapse: a caller retrying a 503 is right to; a caller
// retrying a 404 is not. Every handler that uses this HTTP helper gets the same split.
func (a *App) bundleOrError(c *gin.Context, chID channel.ID) channelhost.Bundle {
	bundle, err := a.acquireBundle(c.Request.Context(), chID)
	if err == nil {
		return bundle
	}
	if errors.Is(err, errChannelNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return nil
	}
	a.logger.Warn("channel unavailable: directory has channel but its home is not open",
		"channel", string(chID))
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
	return nil
}

const (
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
