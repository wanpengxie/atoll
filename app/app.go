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
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// App is the product application server.
type App struct {
	db     *sql.DB
	logger *slog.Logger
	engine *gin.Engine

	mu    sync.RWMutex
	homes map[channel.ID]*platform.Home

	channelDBDir string
}

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/coagent-dev/channels"
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
		db:           cfg.DB,
		logger:       logger,
		homes:        make(map[channel.ID]*platform.Home),
		channelDBDir: cfg.ChannelDBDir,
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

// Run starts the HTTP server.
func (a *App) Run(addr string) error {
	return a.engine.Run(addr)
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
		api.GET("/channels/:chID/members", a.handleListChannelMembers)
		api.GET("/channels/:chID/actors", a.handleListActors)
		api.GET("/channels/:chID/actors/:actorID/status", a.handleActorStatus)

		api.GET("/channels/:chID/cursor", a.handleCursor)
		api.GET("/channels/:chID/messages", a.handleListMessages)
		api.POST("/channels/:chID/messages", a.handleSendMessage)

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

	// Placements stub (no auth -- CLI diagnostic tool).
	a.engine.GET("/api/placements", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"placements": []any{}})
	})

	// Daemon version stub.
	a.engine.GET("/api/daemon/version", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"latest": "dev", "force": false})
	})

	// WebSocket endpoints.
	a.engine.GET("/ws", a.handleWS)
	a.engine.GET("/compute", a.handleCompute)

	// Static files.
	a.engine.Static("/assets", "web/ui/dist/assets")
	a.engine.StaticFile("/favicon.svg", "web/ui/dist/favicon.svg")
	a.engine.NoRoute(func(c *gin.Context) {
		// For non-API, non-asset paths, serve index.html (SPA fallback).
		if !strings.HasPrefix(c.Request.URL.Path, "/api/") &&
			!strings.HasPrefix(c.Request.URL.Path, "/ws") &&
			!strings.HasPrefix(c.Request.URL.Path, "/compute") {
			c.File("web/ui/dist/index.html")
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

func (a *App) createHome(chID channel.ID, dbPath string) (*platform.Home, error) {
	home, err := platform.Open(platform.HomeConfig{
		ChannelID: chID,
		DBPath:    dbPath,
		Logger:    a.logger,
	})
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.homes[chID] = home
	a.mu.Unlock()

	// Spawn the channel's DESIRED composition (channel_actors) — the server-placed
	// instances. Intent lives in channel_actors; what actually comes up lands in
	// the substrate's actor_registry (actor-instance-model §3).
	a.spawnComposition(chID, home)

	return home, nil
}

// defaultAgentInstanceID is the canonical fallback/bootstrap agent instance —
// the always-there server-embedded "boost" floor (default-agent-deployment §0:
// every channel gets a server-cell agent for never-brainless + onboarding).
// default_agent points here by default; it is a name-agnostic pointer, so a
// channel may later repoint it at any other instance id (actor-instance-model §7).
const (
	defaultAgentInstanceID = actor.ActorID("agent:boost")
	defaultAgentClass      = "agent"
	// placementServer marks a composition instance the SERVER hosts (embedded
	// cell). spawnComposition only spawns these; 'daemon'-placed rows are claimed
	// by daemon hosts (delivery is additive). See actor-instance-model §6.
	placementServer = "server"
)

// spawnComposition reads the channel's DESIRED composition (channel_actors) and
// spawns each instance from the actor catalog via the generic Build → Spawn
// path — no special "the agent" case. The composition is INTENT; a build/spawn
// failure (e.g. agent with no LLM creds) is logged and skipped: the row stays
// (intent), the instance just isn't running (현상 = actor_registry has no row),
// and default_agent still points at it. Each instance gets a caller-stamped pen
// (an embedded cell emits through the raw home gate, which the substrate requires
// a CallerContext on). Server placement: Deps carries NO WorkspaceDir, so the
// agent class derives the server-embedded Situation.
func (a *App) spawnComposition(chID channel.ID, home *platform.Home) {
	type instanceSpec struct{ id, class, cfg string }
	var specs []instanceSpec
	rows, err := a.db.Query(
		`SELECT instance_id, class, COALESCE(config_json, '') FROM channel_actors WHERE channel_id = ? AND placement = ?`,
		string(chID), placementServer)
	if err != nil {
		a.logger.Error("app: read composition", "channel", string(chID), "err", err)
		return
	}
	for rows.Next() {
		var s instanceSpec
		if err := rows.Scan(&s.id, &s.class, &s.cfg); err != nil {
			continue
		}
		specs = append(specs, s)
	}
	rows.Close()

	for _, s := range specs {
		decl, err := registry.Build(s.class, registry.InstanceSpec{
			ID:     actor.ActorID(s.id),
			Config: json.RawMessage(s.cfg),
		}, registry.Deps{ChannelID: chID, Logger: a.logger})
		if err != nil {
			a.logger.Debug("app: composition instance not built", "channel", string(chID), "instance", s.id, "reason", err.Error())
			continue
		}
		pen := &callerStampedWriter{inner: home.Gate(), caller: harness.CallerContext{
			ActorID: decl.ID, ChannelID: chID,
		}}
		impl := decl.Factory(pen)
		if err := home.Spawn(context.Background(), decl.ID, decl.Kind, impl); err != nil {
			a.logger.Warn("app: spawn composition instance failed", "channel", string(chID), "instance", string(decl.ID), "err", err.Error())
		}
	}
}

// callerStampedWriter wraps the home write gate with a fixed CallerContext so an
// embedded cell's emits authenticate as that cell. (A daemon cell gets this from
// the link's emit-sink; an embedded cell's pen is stamped here at the app
// composition root.)
type callerStampedWriter struct {
	inner  harness.Writer
	caller harness.CallerContext
}

func (w *callerStampedWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return w.inner.Write(harness.CtxWithCaller(ctx, w.caller), env)
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
