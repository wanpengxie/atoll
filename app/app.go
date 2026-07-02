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

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// App is the product application server.
type App struct {
	db     *sql.DB
	logger *slog.Logger
	engine *gin.Engine

	mu    sync.RWMutex
	homes map[channel.ID]*platform.Home
	// humans indexes each channel's live HUMAN write front-ends by user actor id.
	// A front-end is the app's reference to a home-scoped human cell (admitted via
	// Home.Spawn with a pen welded to "user:<id>") — the ONLY write path a person
	// has now that the app holds no write gate (sealed-pen). Home-scoped: spawned
	// on a user's first write and dropped when the channel home is torn down.
	humans map[channel.ID]map[actor.ActorID]*humanFront

	channelDBDir string
}

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/atoll-dev/channels"
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
		humans:       make(map[channel.ID]map[actor.ActorID]*humanFront),
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
		a.forgetHumans(id)
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

		// §五 创建与控制: a user's agents (global declaration) + introduce / restart.
		api.GET("/agents", a.handleListAgents)
		api.POST("/agents", a.handleCreateAgent)
		api.PATCH("/agents/:agentID", a.handleUpdateAgent)
		api.DELETE("/agents/:agentID", a.handleDeleteAgent)
		api.POST("/agents/:agentID/restart", a.handleRestartAgent)
		api.POST("/channels/:chID/agents", a.handleIntroduceAgent)
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
	// Daemon composition pull (daemon-composition spec §3): the daemon GETs its
	// channel's placement='daemon' assignment, then builds exactly that set
	// (no blind-build). Same ?key=+?channel= auth as /compute.
	a.engine.GET("/compute/plan", a.handleComputePlan)

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
	// defaultBoostLooper is the engine CLASS the always-there boost floor runs.
	// An agent's engine IS its actor class — claude/go-kimi are flat registry
	// classes (kind=agent), there is NO umbrella "agent" class. boost has no
	// agents declaration row, so it can't carry a default_looper; it runs this
	// fixed fallback engine.
	defaultBoostLooper = "go-kimi"
	// placementServer marks a composition instance the SERVER hosts (embedded
	// cell). spawnComposition only spawns these.
	placementServer = "server"
	// placementDaemon marks a composition instance a connected DAEMON hosts. The
	// server never spawns these; the daemon pulls them (GET /compute/plan,
	// daemon-composition spec §3) and builds them with its LOCAL creds.
	placementDaemon = "daemon"
)

// spawnComposition reads the channel's DESIRED composition (channel_actors) and
// spawns each instance from the actor catalog via the generic Build → Spawn
// path — no special "the agent" case. The composition is INTENT; a build/spawn
// failure (e.g. agent with no LLM creds) is logged and skipped: the row stays
// (intent), the instance just isn't running (현상 = actor_registry has no row),
// and default_agent still points at it. Each instance is admitted via Home.Spawn,
// which Mints a pen welded to its (id, chID) inside the membrane and hands it to
// the factory — the app never sees a bare writer (sealed-pen). Server placement:
// Deps carries NO WorkspaceDir, so the agent class derives the server-embedded
// Situation.
func (a *App) spawnComposition(chID channel.ID, home *platform.Home) {
	type instanceSpec struct{ id, class, cfg, state, gcfg string }
	var specs []instanceSpec
	// LEFT JOIN agents: for an agent instance (instance_id = 'agent:'||agents.id)
	// overlay its GLOBAL identity config (persona/skills) UNDER the per-channel
	// config (agent-spec §二). The engine is NOT read here — it IS ca.class (the
	// per-channel concrete engine class: claude/go-kimi). A non-agent class never
	// matches the join (no global overlay), which is correct.
	rows, err := a.db.Query(
		`SELECT ca.instance_id, ca.class, COALESCE(ca.config_json, ''), COALESCE(ca.state, ''),
		        COALESCE(a.config_json, '')
		   FROM channel_actors ca
		   LEFT JOIN agents a ON ca.instance_id = 'agent:' || a.id
		  WHERE ca.channel_id = ? AND ca.placement = ?`,
		string(chID), placementServer)
	if err != nil {
		a.logger.Error("app: read composition", "channel", string(chID), "err", err)
		return
	}
	for rows.Next() {
		var s instanceSpec
		if err := rows.Scan(&s.id, &s.class, &s.cfg, &s.state, &s.gcfg); err != nil {
			continue
		}
		specs = append(specs, s)
	}
	rows.Close()

	// Durable per-instance session dirs live under the data root (sibling of the
	// channel DBs), platform-managed so a looper's opaque session survives a
	// restart — keystone: durable state in a platform-controlled area, not a tmp
	// dir or the user's home (agent-spec §四).
	sessionsRoot := filepath.Join(filepath.Dir(a.channelDBDir), "agent-sessions")

	for _, s := range specs {
		inst := s.id // bind for the checkpoint closure
		dir := filepath.Join(sessionsRoot, pathSafe(string(chID)), pathSafe(inst))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			a.logger.Warn("app: session dir", "channel", string(chID), "instance", inst, "err", err.Error())
			dir = "" // fall back to the looper's ephemeral default
		}
		// store persists a looper-authored opaque checkpoint into the state slot.
		// The looper is the slot's ONLY author (agent-spec §三).
		store := func(blob json.RawMessage) error {
			_, err := a.db.Exec(
				`UPDATE channel_actors SET state = ? WHERE channel_id = ? AND instance_id = ?`,
				string(blob), string(chID), inst)
			return err
		}
		// The engine = ca.class (per-channel concrete class). config = global
		// identity overlaid by per-channel (mergeConfig); no looper-DSN packing.
		cfg := mergeConfig(s.gcfg, s.cfg)
		decl, err := registry.Build(s.class, registry.InstanceSpec{
			ID:     actor.ActorID(inst),
			Config: cfg,
		}, registry.Deps{
			ChannelID: chID,
			Logger:    a.logger,
			State: registry.StateSlot{
				Dir:   dir,
				Seed:  json.RawMessage(s.state),
				Store: store,
			},
		})
		if err != nil {
			a.logger.Debug("app: composition instance not built", "channel", string(chID), "instance", inst, "reason", err.Error())
			continue
		}
		// Home.Spawn Mints the pen welded to (decl.ID, chID) inside the admission
		// membrane and hands it to the factory — the app supplies WHAT to place
		// (id + factory), never a bare writer or a Minter (sealed-pen §3.1).
		if err := home.Spawn(context.Background(), decl.ID, decl.Kind, decl.Factory); err != nil {
			a.logger.Warn("app: spawn composition instance failed", "channel", string(chID), "instance", string(decl.ID), "err", err.Error())
		}
	}
}

// pathSafe maps an actor/channel id to a filesystem-safe path segment (ids may
// carry ':' like "agent:boost").
func pathSafe(s string) string {
	return strings.NewReplacer(":", "-", "/", "_", "\\", "_").Replace(s)
}

// mergeConfig layers the per-channel config_json OVER the global agents config
// (agent-spec §二: one config, two layers). Shallow object merge — channel keys
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
	// to the global blob. [codex P2]
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
