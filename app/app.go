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
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/jsondepth"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
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

	mu    sync.RWMutex
	homes map[channel.ID]*home.Home

	channelDBDir string
	uiDist       string

	// extraDropKinds: drivers-side producers' diagnostic obs kinds, injected via
	// Config (see Config.ExtraDropKinds).
	extraDropKinds []actorrt.ObsKind

	// wsGateway is the injected human-ingress connector (gateway 期 S3); membershipPoke
	// is the injected membership-change poke fan-in (PokeHub.Poke) the platform emit
	// points (home.Config.OnMembershipChange, wired in createHome — Admit/Remove) feed.
	// Both are set by the assembly root via SetGateway/SetMembershipPoke after New (the
	// gateway needs the app's routing/entitlement面, breaking the構造 cycle).
	wsGateway      WSGateway
	membershipPoke func(principal string)

	// controlShimTimeout bounds how long a channel-control HTTP shim waits for the
	// door's terminal reply before returning 202 + request_id (前端语义不变). A test
	// seam sets it tiny to exercise the timeout branch deterministically.
	controlShimTimeout time.Duration

	// seedAdmitFailHook / revokeFailHook are test-only injected failure seams (nil
	// in production — no if-bool branch survives in the production handlers, mirroring
	// platform's reviverStraddleHook). When set (via export_test.go) they return a
	// non-nil error to force a specific persist failure so a test can exercise the
	// transactional rollback path: seedAdmitFailHook fails a create-channel seeding
	// Admit (→ close home + delete channel row + 5xx); revokeFailHook aborts the
	// daemon-delete revocation tx (→ rollback + 5xx, no Kick, no false ok).
	seedAdmitFailHook func() error
	revokeFailHook    func() error
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

	// ExtraDropKinds is the domain producers' diagnostic obs vocabulary (e.g.
	// agentbase's), supplied by the assembly root (cmd/server) — app sits below
	// drivers/ (fence: only cmd imports drivers/*), so it cannot union the
	// drivers-side kinds itself. Appended to actorbase's own kinds. Nil is fine
	// (substrate kinds only).
	ExtraDropKinds []actorrt.ObsKind
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
		homes:              make(map[channel.ID]*home.Home),
		channelDBDir:       cfg.ChannelDBDir,
		uiDist:             cfg.UIDist,
		controlShimTimeout: defaultControlShimTimeout,
		extraDropKinds:     cfg.ExtraDropKinds,
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

// SetGateway injects the human-ingress connector (gateway 期 S3). The assembly
// root calls it after New — the gateway is constructed with the app's routing面,
// so the app cannot hold it at New time (construction cycle). /ws answers 503 until
// it is set.
func (a *App) SetGateway(g WSGateway) { a.wsGateway = g }

// SetMembershipPoke injects the membership-change poke fan-in (PokeHub.Poke). createHome
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
	a.mu.Lock()
	homes := a.homes
	a.homes = make(map[channel.ID]*home.Home)
	a.mu.Unlock()
	var firstErr error
	for _, h := range homes {
		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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
		api.GET("/channels/:chID/presence-drops", a.handleChannelPresenceDrops)
		api.GET("/channels/:chID/cursor", a.handleCursor)
		api.GET("/channels/:chID/messages", a.handleListMessages)

		// A user's actor-instance declarations (world layer, kind-neutral) +
		// introduce-to-channel / restart.
		api.GET("/actor-decls", a.handleListDecls)
		api.POST("/actor-decls", a.handleCreateDecl)
		api.PATCH("/actor-decls/:declID", a.handleUpdateDecl)
		api.DELETE("/actor-decls/:declID", a.handleDeleteDecl)
		api.POST("/actor-decls/:declID/restart", a.handleRestartDecl)
		api.POST("/channels/:chID/actors", a.handleIntroduceActor)
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

func (a *App) getHome(chID channel.ID) *home.Home {
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
func (a *App) homeOrError(c *gin.Context, chID channel.ID) *home.Home {
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

// eventDropKinds is the union of every producer's diagnostic obs kinds, handed
// to the presence fold's drop buckets: actorbase's own (substrate side, app may
// import lib) + the injected drivers-side vocabulary (agentbase's — supplied by
// cmd/server, the only legal drivers/* importer; drivers 围栏 Fence B).
func (a *App) eventDropKinds() []actorrt.ObsKind {
	return append(actorbase.ObsDropKinds(), a.extraDropKinds...)
}

func (a *App) createHome(chID channel.ID, dbPath string) (*home.Home, error) {
	home, err := home.Open(home.Config{
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
		// The presence fold's drop-bucket vocabulary: substrate kinds + the
		// drivers-side kinds the assembly root injected (Config.ExtraDropKinds).
		EventDropKinds: a.eventDropKinds(),
		// Membership-change poke emit point (连接模型勘误期 §3.2 表②): Home.Admit (入籍)
		// and Home.Remove (注销, principal captured before the dereg cascade) fire this;
		// the app forwards the principal into the injected poke fan-in so the gateway
		// re-resolves that principal's channel set (subscriptions + presence). Channel is
		// no longer part of the address — the resolver enumerates the whole set.
		OnMembershipChange: func(principal string) {
			if a.membershipPoke != nil {
				a.membershipPoke(principal)
			}
		},
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
	// server never spawns these; the daemon pulls them (GET /compute/plan) and
	// builds them with its LOCAL creds.
	placementDaemon = "daemon"
)

// mergeConfig layers the per-channel config_json OVER the global actor_decls
// config (one config, two layers). Shallow object merge — channel keys
// win. Empty / non-object inputs degrade gracefully (a raw per-channel blob is
// preserved as-is so a non-agent class still gets its config_json).
func mergeConfig(global, perChannel string) json.RawMessage {
	gl := strings.TrimSpace(global)
	pc := strings.TrimSpace(perChannel)
	g := map[string]any{}
	c := map[string]any{}
	// Bounded-depth pre-check on BOTH layers before the map[string]any Unmarshal:
	// a poison config persists in the actor_decls/channel_actors tables and is re-hit on
	// EVERY startup (loadChannels → reconcile build → mergeConfig), so a deeply-
	// nested blob would fatally overflow the stack every boot. An over-deep layer is
	// treated as NOT an object (skips the merge); the raw bytes are never recursed
	// into here — a two-poison case falls through to the verbatim raw return below,
	// where the looper self-parses opaquely in its own subprocess.
	gObj := gl != "" && jsondepth.Bounded([]byte(gl)) == nil && json.Unmarshal([]byte(gl), &g) == nil
	cObj := pc != "" && jsondepth.Bounded([]byte(pc)) == nil && json.Unmarshal([]byte(pc), &c) == nil
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
