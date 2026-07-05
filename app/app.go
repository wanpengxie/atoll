// Package app is the product application layer: identity, workspace, channel
// lifecycle, daemon management, and HTTP API. It sits above platform (which
// owns per-channel truth) and below cmd (which wires concrete config).
package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// App is the product application server.
type App struct {
	db     *sql.DB
	logger *slog.Logger
	engine *gin.Engine
	srv    *http.Server // set by Run; drained by Shutdown

	mu    sync.RWMutex
	homes map[channel.ID]*platform.Home

	channelDBDir string
	uiDist       string

	// controlShimTimeout bounds how long a channel-control HTTP shim waits for the
	// door's terminal reply before returning 202 + request_id (前端语义不变). A test
	// seam sets it tiny to exercise the timeout branch deterministically.
	controlShimTimeout time.Duration
}

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/atoll-dev/channels"

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

	if err := os.MkdirAll(cfg.ChannelDBDir, 0o755); err != nil {
		return nil, fmt.Errorf("app: mkdir channel db dir: %w", err)
	}

	a := &App{
		db:                 cfg.DB,
		logger:             logger,
		homes:              make(map[channel.ID]*platform.Home),
		channelDBDir:       cfg.ChannelDBDir,
		uiDist:             cfg.UIDist,
		controlShimTimeout: defaultControlShimTimeout,
	}

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

	return a, nil
}

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

// Close tears down all channel homes.
func (a *App) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var firstErr error
	for id, home := range a.homes {
		if err := home.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(a.homes, id)
	}
	return firstErr
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
		api.GET("/workspaces", a.handleListWorkspaces)
		api.POST("/workspaces", a.handleCreateWorkspace)

		api.GET("/workspaces/:wsID/channels", a.handleListChannels)
		api.POST("/workspaces/:wsID/channels", a.handleCreateChannel)

		api.GET("/channels/:chID", a.handleGetChannel)
		api.DELETE("/channels/:chID", a.handleDeleteChannel)
		api.GET("/channels/:chID/workspace-members", a.handleListWorkspaceMembers)
		// DEPRECATED (第二链路, H2 defer): channel-internal reads move onto the
		// gateway ws (roster/tail/cursor frames). Kept as read shims until帧化; no
		// new consumers. The channel-internal WRITE path is already gone — the ws
		// message frame replaced POST /messages (H1=a, zero backward-compat).
		api.GET("/channels/:chID/actors", a.handleListActors)
		api.GET("/channels/:chID/actors/:actorID/status", a.handleActorStatus)
		api.GET("/channels/:chID/cursor", a.handleCursor)
		api.GET("/channels/:chID/messages", a.handleListMessages)

		// A user's agents (global declaration) + introduce / restart.
		api.GET("/agents", a.handleListAgents)
		api.POST("/agents", a.handleCreateAgent)
		api.PATCH("/agents/:agentID", a.handleUpdateAgent)
		api.DELETE("/agents/:agentID", a.handleDeleteAgent)
		api.POST("/agents/:agentID/restart", a.handleRestartAgent)
		api.POST("/channels/:chID/agents", a.handleIntroduceAgent)
		api.DELETE("/channels/:chID/actors/:instanceID", a.handleRemoveActor)
		api.PUT("/channels/:chID/default_agent", a.handleSetDefaultAgent)

		api.GET("/daemons", a.handleListDaemons)
		api.POST("/daemons", a.handleCreateDaemon)
		api.DELETE("/daemons/:id", a.handleDeleteDaemon)

		api.GET("/channels/:chID/daemons", a.handleListChannelDaemons)
		api.POST("/channels/:chID/daemons", a.handleCreateAndAttachDaemon)
		api.POST("/channels/:chID/daemons/attach", a.handleAttachDaemons)
		api.DELETE("/channels/:chID/daemons/:id/attach", a.handleDetachDaemon)
	}

	// Health check (no auth).
	a.engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// WebSocket endpoints.
	a.engine.GET("/ws", a.handleWS)
	a.engine.GET("/compute", a.handleCompute)
	// Daemon composition pull: the daemon GETs its channel's placement='daemon'
	// assignment, then builds exactly that set (no blind-build). Same
	// ?key=+?channel= auth as /compute.
	a.engine.GET("/compute/plan", a.handleComputePlan)

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

func (a *App) getHome(chID channel.ID) *platform.Home {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.homes[chID]
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
func (a *App) homeOrError(c *gin.Context, chID channel.ID) *platform.Home {
	if home := a.getHome(chID); home != nil {
		return home
	}
	if _, ok := a.channelWorkspaceID(c.Request.Context(), string(chID)); !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return nil
	}
	a.logger.Warn("channel unavailable: directory has channel but its home is not open",
		"channel", string(chID))
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
	return nil
}

func (a *App) createHome(chID channel.ID, dbPath string) (*platform.Home, error) {
	home, err := platform.Open(platform.HomeConfig{
		ChannelID: chID,
		DBPath:    dbPath,
		Logger:    a.logger,
		// Fill the two eager-activation injection points with the组合域 supply:
		// Desired = server-placed intent (the reconcile ring's desired half),
		// Builder = the id→ActorFactory table (activation/reviver resolve). The
		// user域 (per-channel human members) is derived by the platform ring itself
		// (see Home.reconcileActivation) — the app cannot enumerate it. Both non-nil
		// (double-nil灭: a nil pair leaves the ring inert).
		Desired: compositionDesired{app: a, chID: chID},
		Builder: compositionBuilder{app: a, chID: chID},
		// Fill the operate-face injection point: the in-gate control plane's
		// executor half (intent write + Home call). One instance, channel-resolved.
		Operate: a.operateFace(),
	})
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.homes[chID] = home
	a.mu.Unlock()

	// Composition embodiment is the reconcile ring's job now (compositionDesired ∩
	// membership → SpawnIfAbsent), run by Open's synchronous startup sweep — the app
	// no longer hand-spawns at open time (spawnComposition retired, A-P1=A′).
	return home, nil
}

// defaultAgentInstanceID is the canonical fallback/bootstrap agent instance —
// the always-there server-embedded "boost" floor: every channel gets a
// server-cell agent for never-brainless behavior plus onboarding.
// default_agent points here by default; it is a name-agnostic pointer, so a
// channel may later repoint it at any other instance id.
const (
	defaultAgentInstanceID = actor.ActorID("agent:boost")
	// defaultBoostLooper is the engine CLASS the always-there boost floor runs.
	// An agent's engine IS its actor class — claude/go-kimi are flat registry
	// classes (kind=agent), there is NO umbrella "agent" class. boost has no
	// agents declaration row, so it can't carry a default_looper; it runs this
	// fixed fallback engine.
	defaultBoostLooper = "go-kimi"
	// placementServer marks a composition instance the SERVER hosts (embedded
	// cell). The reconcile ring only embodies these; daemon-placed rows are pulled
	// by the daemon's own plan.
	placementServer = "server"
	// placementDaemon marks a composition instance a connected DAEMON hosts. The
	// server never spawns these; the daemon pulls them (GET /compute/plan) and
	// builds them with its LOCAL creds.
	placementDaemon = "daemon"
)

// mergeConfig layers the per-channel config_json OVER the global agents config
// (one config, two layers). Shallow object merge — channel keys
// win. Empty / non-object inputs degrade gracefully (a raw per-channel blob is
// preserved as-is so a non-agent class still gets its config_json).
func mergeConfig(global, perChannel string) json.RawMessage {
	gl := strings.TrimSpace(global)
	pc := strings.TrimSpace(perChannel)
	g := map[string]any{}
	c := map[string]any{}
	gObj := gl != "" && json.Unmarshal([]byte(gl), &g) == nil
	cObj := pc != "" && json.Unmarshal([]byte(pc), &c) == nil
	// Two-layer shallow merge ONLY when both are JSON objects (channel keys win).
	if gObj && cObj {
		for k, v := range c {
			g[k] = v
		}
		if out, err := json.Marshal(g); err == nil {
			return out
		}
	}
	// Otherwise the per-channel blob is the more-specific layer — preserve it
	// verbatim (never silently drop a non-object per-channel config); fall back
	// to the global blob.
	if pc != "" {
		return json.RawMessage(pc)
	}
	if gl != "" {
		return json.RawMessage(gl)
	}
	return nil
}

func (a *App) loadChannels() error {
	rows, err := a.db.Query(`SELECT id, db_path FROM channels`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, dbPath string
		if err := rows.Scan(&id, &dbPath); err != nil {
			continue
		}
		if _, err := a.createHome(channel.ID(id), dbPath); err != nil {
			a.logger.Warn("app: load channel failed", "channel", id, "err", err.Error())
			// Continue loading other channels.
		}
	}
	return nil
}
