// Package app is the product application layer: identity, workspace, channel
// lifecycle, daemon management, and HTTP API. It sits above platform (which
// owns per-channel truth) and below cmd (which wires concrete config).
package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/actors/agent"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
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
	agentFactory AgentFactory
}

// AgentFactory builds a channel's bundled default-agent cell. The returned Actor
// is spawned as an embedded server cell (kind=agent, binding=embedded); w is its
// pen — a caller-stamped harness.Writer so the agent's emits authenticate as
// itself (the app composition root stamps, exactly as channelkit does for the
// system cell). A nil factory falls back to the env-based go-kimi built-in (no
// KIMI creds → no built-in agent, dev/e2e unaffected). Tests inject a stub here.
type AgentFactory func(chID channel.ID, agentID actor.ActorID, w harness.Writer) (actorrt.Actor, error)

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/coagent-dev/channels"
	// AgentFactory overrides how the bundled default agent is built (tests inject
	// a stub; production leaves it nil for the env-based go-kimi built-in).
	AgentFactory AgentFactory
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
		agentFactory: cfg.AgentFactory,
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// CORS (allow all for dev).
	engine.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type,Authorization")
		c.Header("Access-Control-Allow-Credentials", "true")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	a.engine = engine
	a.registerRoutes()

	// Load existing channels from DB.
	if err := a.loadChannels(); err != nil {
		return nil, fmt.Errorf("app: load channels: %w", err)
	}

	return a, nil
}

// Handler returns the http.Handler (gin engine) for use with httptest or
// direct ServeHTTP calls. The app layer keeps ownership of routes; callers
// only get a read-only Handler reference.
func (a *App) Handler() http.Handler {
	return a.engine
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
		identity.GET("/me", a.handleMe)
		identity.POST("/verification/issue", a.handleVerificationIssue)
	}

	// Authenticated API routes.
	api := a.engine.Group("/api")
	api.Use(a.authMiddleware())
	{
		api.GET("/workspaces", a.handleListWorkspaces)
		api.POST("/workspaces", a.handleCreateWorkspace)

		api.GET("/workspaces/:wsID/channels", a.handleListChannels)
		api.POST("/workspaces/:wsID/channels", a.handleCreateChannel)
		api.POST("/workspaces/:wsID/channels/:chID/bind", a.handleBindChannel)

		api.GET("/channels/:chID", a.handleGetChannel)
		api.DELETE("/channels/:chID", a.handleDeleteChannel)
		api.GET("/channels/:chID/members", a.handleListChannelMembers)
		api.GET("/channels/:chID/actors", a.handleListActors)

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

	// Built-in agent (the channel's bundled brain): spawned as a server cell
	// when the server carries LLM credentials. Guarded — a server without
	// KIMI_API_KEY simply has no built-in agent (dev/e2e unaffected).
	a.spawnBuiltinAgent(chID, home)

	return home, nil
}

// builtinAgentID is the bundled server-cell agent's channel-scoped id.
const builtinAgentID = actor.ActorID("agent:main")

// spawnBuiltinAgent best-effort spawns the bundled agent cell. Failure is
// logged, never fatal: a channel without its brain still serves path-1
// (explicit audience) traffic. The agent's pen is caller-stamped with its own
// identity (embedded cells emit through the raw home gate, which the substrate
// requires a CallerContext on — the app stamps here, as channelkit does for the
// system cell).
func (a *App) spawnBuiltinAgent(chID channel.ID, home *platform.Home) {
	pen := &callerStampedWriter{inner: home.Gate(), caller: harness.CallerContext{
		ActorID: builtinAgentID, ChannelID: chID,
	}}

	var impl actorrt.Actor
	if a.agentFactory != nil {
		built, err := a.agentFactory(chID, builtinAgentID, pen)
		if err != nil {
			a.logger.Warn("app: agent factory failed", "channel", string(chID), "err", err.Error())
			return
		}
		impl = built
	} else {
		cfg, err := agent.NewConfigFromEnv(agent.BuildSystemPrompt(
			agent.Situation{Host: "server"},
			os.Getenv(agent.EnvKeyChannelType), os.Getenv(agent.EnvKeyDomainPrompt)))
		if err != nil {
			return // no LLM credentials on this server — no built-in agent
		}
		bridge, err := agent.NewBridge(cfg, builtinAgentID, chID, pen)
		if err != nil {
			a.logger.Warn("app: builtin agent build failed", "channel", string(chID), "err", err.Error())
			return
		}
		impl = bridge
	}

	if err := home.Spawn(context.Background(), builtinAgentID, actor.KindAgent, impl); err != nil {
		a.logger.Warn("app: builtin agent spawn failed", "channel", string(chID), "err", err.Error())
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
