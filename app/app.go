// Package app is the reference space: principal identity, channel directory and
// lifecycle, daemon registry, and HTTP API. Per-channel truth belongs to the
// platform membrane.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/app/internal/middleware"
	relationstore "github.com/wanpengxie/atoll/app/internal/relation"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/daemonhost"
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

	mu         sync.RWMutex
	createMu   sync.Mutex
	host       channelhost.LocalHost
	daemonHost *daemonhost.Host
	uiDist     string

	// wsGateway is the injected human-ingress connector (gateway 期 S3); membershipPoke
	// is the injected direct Gateway.Poke callback that the platform emission
	// points (home.Config.OnRelationChange, wired by ChannelHost) feed.
	// Both are set by the assembly root via SetGateway/SetMembershipPoke after New (the
	// gateway needs the app's routing/entitlement面, breaking the構造 cycle).
	wsGateway      WSGateway
	membershipPoke func(principal string)

	daemonLocks  *keyedLockSet
	channelLocks *keyedLockSet
	lifecycle    *lifecycleWorker
	relations    *relationstore.Store
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

// New assembles the App: gin engine, routes, wiring — and nothing running.
// Existing channels are loaded by the convergence arm, which starts in Start
// (called by Run) after the assembly root finishes every setter injection.
func New(cfg Config) (*App, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	a := &App{
		db: cfg.DB, logger: logger, uiDist: cfg.UIDist,
		daemonLocks: newKeyedLockSet(), channelLocks: newKeyedLockSet(),
	}
	a.relations = relationstore.New(a.db)
	a.daemonHost = daemonhost.New(daemonhost.Config{
		Logger: logger,
		Present: func(ctx context.Context) ([]channel.ID, error) {
			return a.directoryChannelIDs(ctx)
		},
		DaemonFact: func(ctx context.Context, daemonID string) daemonhost.DaemonFact {
			var deleted sql.NullInt64
			err := a.db.QueryRowContext(ctx, `SELECT deleted_at FROM daemons WHERE id=?`, daemonID).Scan(&deleted)
			switch {
			case errors.Is(err, sql.ErrNoRows), err == nil && deleted.Valid:
				return daemonhost.DaemonDeleted
			case err != nil:
				return daemonhost.DaemonUnavailable
			default:
				return daemonhost.DaemonAlive
			}
		},
	})
	if cfg.HostFactory == nil {
		return nil, errors.Join(
			errors.New("app: HostFactory required"),
			a.daemonHost.Close(context.Background()))
	}
	host, err := cfg.HostFactory(channelhost.HomeDeps{
		CompositionResolver:  compositionResolver{app: a},
		IntroductionResolver: compositionResolver{app: a},
		Logger:               logger,
		DaemonRoutes:         a.daemonHost,
		OnMembraneOpen:       a.daemonHost.Register,
		OnMembraneClose:      a.daemonHost.Unregister,
		OnRelationChange: func(chID channel.ID, deltas []channelspec.RelationDelta) {
			// Two independent consumers of one delivery: the relation index
			// records, the gateway poke derives from the deltas themselves.
			// A failed index write must not mute the poke — the re-resolve it
			// triggers reads membrane truth and is itself the repair path.
			if err := a.relations.Apply(context.Background(), chID, deltas); err != nil {
				a.logger.Warn("relation event apply failed", "channel", chID, "err", err)
			}
			for _, delta := range deltas {
				if (delta.Kind == channelspec.RelationJoined || delta.Kind == channelspec.RelationLeft) &&
					delta.Principal != "" && a.membershipPoke != nil {
					a.membershipPoke(delta.Principal)
				}
			}
			a.daemonHost.Scan()
		},
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("app: construct ChannelHost: %w", err),
			a.daemonHost.Close(context.Background()))
	}
	a.host = host

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.CustomRecovery(func(c *gin.Context, _ any) {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		c.Abort()
	}))
	engine.Use(middleware.CORS())

	a.engine = engine
	a.registerRoutes()

	a.lifecycle = newLifecycleWorker(a)

	return a, nil
}

// Start begins background work (the convergence arm, including its boot full
// scan). Construction and assembly must be complete before this point: New
// only wires, Start runs. The assembly root calls every setter
// (SetGateway/SetMembershipPoke) between New and Start, so no reader of those
// fields exists until all writes are done — construction stays side-effect
// free and a failed assembly exits cleanly with nothing running. Run calls
// Start; call it directly only in harnesses that never Run. Idempotent.
func (a *App) Start() {
	if a.lifecycle != nil {
		a.lifecycle.start()
	}
}

// SetGateway injects the human-ingress connector (gateway 期 S3). The assembly
// root calls it after New — the gateway is constructed with the app's routing面,
// so the app cannot hold it at New time (construction cycle). /ws answers 503 until
// it is set.
func (a *App) SetGateway(g WSGateway) { a.wsGateway = g }

// SetMembershipPoke injects Gateway.Poke directly. ChannelHost forwards relation
// deltas from every channel through the HomeDeps callback.
// nil means no live poke; reconnect re-auth and relation Snapshot/read-time
// verification remain the correctness paths, so a lost poke only delays routing.
func (a *App) SetMembershipPoke(fn func(principal string)) { a.membershipPoke = fn }

// Run starts the HTTP server and blocks until it is Shutdown (or errors). It
// holds an explicit http.Server so cmd can drain in-flight requests on signal;
// a clean Shutdown returns nil (ErrServerClosed is not an error).
func (a *App) Run(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.Serve(ln)
}

// PrepareServe registers the HTTP server for ln SYNCHRONOUSLY — from the
// moment it returns, Shutdown reaches this server — and hands back the
// blocking serve loop. The split from Serve exists so an assembly root that
// runs the loop in a goroutine can order things truthfully: register, THEN
// announce ready, THEN serve. Registering inside the goroutine would leave a
// window where a teardown's Shutdown sees no server while the loop starts
// serving right after it (http.Server closes that residue itself: Serve after
// Shutdown returns ErrServerClosed immediately).
func (a *App) PrepareServe(ln net.Listener) func() error {
	a.Start()
	a.mu.Lock()
	a.srv = &http.Server{Handler: a.engine}
	srv := a.srv
	a.mu.Unlock()
	return func() error {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

// Serve serves the HTTP entry on an already-bound listener and blocks until
// Shutdown (or error).
func (a *App) Serve(ln net.Listener) error { return a.PrepareServe(ln)() }

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

// Close joins space workers before transferring process teardown to ChannelHost,
// the sole owner of serving Home instances and their physical stores. The
// caller's budget bounds every join: whatever refuses to leave in time is
// abandoned with its account in the returned error, because process death —
// not this ordering — is what actually reclaims a worker that ignores
// cancellation, and the stores' crash safety — not this ordering — is what
// keeps their data intact.
func (a *App) Close(ctx context.Context) error {
	var lifecycleErr error
	if a.lifecycle != nil {
		lifecycleErr = a.lifecycle.close(ctx)
	}
	return errors.Join(lifecycleErr, a.daemonHost.Close(ctx), a.host.Close(ctx))
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

func (a *App) registerRoutes() {
	handlers := map[string]gin.HandlerFunc{
		"GET /api/meta": func(c *gin.Context) {
			c.JSON(http.StatusOK, contract.Meta{ContractVersion: contract.Version})
		},
		"POST /api/identity/register":                     a.handleRegister,
		"POST /api/identity/login":                        a.handleLogin,
		"POST /api/identity/logout":                       a.handleLogout,
		"GET /api/identity/me":                            a.handleMe,
		"POST /api/identity/verification/issue":           a.handleVerificationIssue,
		"GET /api/channels":                               a.handleListChannels,
		"POST /api/channels":                              a.handleCreateChannel,
		"GET /api/channels/:chID":                         a.handleGetChannel,
		"DELETE /api/channels/:chID":                      a.handleDeleteChannel,
		"POST /api/channels/:chID/join":                   a.handleJoinChannel,
		"GET /api/channels/:chID/observe":                 a.handleObserveChannel,
		"GET /api/experimental/channels/:chID/observe":    a.handleObserveChannel,
		"GET /api/channels/:chID/messages":                a.handleListMessages,
		"GET /api/channels/:chID/resources":               a.handleListResources,
		"GET /api/channels/:chID/resources/:rid":          a.handleStatResource,
		"GET /api/channels/:chID/resources/:rid/bytes":    a.handleFetchResource,
		"POST /api/channels/:chID/actors":                 a.handleIntroduceActor,
		"DELETE /api/channels/:chID/actors/:actorID":      a.handleRemoveChannelActor,
		"PUT /api/channels/:chID/decls/:declID/config":    a.handlePutDeclarationOverlay,
		"DELETE /api/channels/:chID/decls/:declID/config": a.handleDeleteDeclarationOverlay,
		"GET /api/channels/:chID/candidates":              a.handleListCandidates,
		"GET /api/actor-decls":                            a.handleListDecls,
		"POST /api/actor-decls":                           a.handleCreateDecl,
		"PATCH /api/actor-decls/:declID":                  a.handleUpdateDecl,
		"DELETE /api/actor-decls/:declID":                 a.handleDeleteDecl,
		"GET /api/daemons":                                a.handleListDaemons,
		"POST /api/daemons":                               a.handleCreateDaemon,
		"DELETE /api/daemons/:id":                         a.handleDeleteDaemon,
		"GET /api/channels/:chID/daemons":                 a.handleListChannelDaemons,
		"POST /api/channels/:chID/daemons":                a.handleAttachDaemon,
		"DELETE /api/channels/:chID/daemons/:id":          a.handleDetachDaemon,
		"GET /ws":                                         a.handleWS,
	}
	experimental := a.engine.Group("/api/experimental")
	for _, method := range contract.Methods() {
		key := method.Method + " " + method.Path
		handler, ok := handlers[key]
		if !ok {
			panic("contract method has no handler: " + key)
		}
		delete(handlers, key)
		if method.Path == "/ws" {
			a.engine.Handle(method.Method, method.Path, handler)
			continue
		}
		chain := make([]gin.HandlerFunc, 0, 3)
		if method.Auth == contract.AuthSession {
			chain = append(chain, middleware.Auth(a.db))
		} else if method.Auth != contract.AuthNone {
			panic("contract method has unknown auth: " + key)
		}
		if method.Method != http.MethodGet && method.Method != http.MethodHead && !method.HasBody() {
			chain = append(chain, rejectRequestBody)
		}
		chain = append(chain, handler)
		if method.Experimental {
			path := strings.TrimPrefix(method.Path, "/api/experimental")
			experimental.Handle(method.Method, path, chain...)
		} else {
			a.engine.Handle(method.Method, method.Path, chain...)
		}
	}
	if len(handlers) != 0 {
		panic("handler missing from contract method registry")
	}

	// Health check (no auth).
	a.engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Internal compute carrier is deliberately outside the shell contract.
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
		writeAPIError(c, http.StatusNotFound, contract.CodeNotFound, "not found")
	})
}

// ---------------------------------------------------------------------------
// Channel home management
// ---------------------------------------------------------------------------

var (
	errChannelNotFound    = errors.New("app: channel not found")
	errChannelUnavailable = errors.New("app: channel unavailable")
)

// acquireBundle resolves the open Bundle for chID with the honest two-state
// failure split (A-P8):
//   - the directory (channels table) has NO such channel → errChannelNotFound
//     (HTTP 404, permanent).
//   - the directory HAS it but ChannelHost cannot acquire a serving Bundle →
//     errChannelUnavailable (HTTP 503, retryable) — never the misleading 404
//     that conflated "gone" with "not up yet".
//
// The two states must not collapse: a caller retrying a 503 is right to; a
// caller retrying a 404 is not. Every HTTP path maps exactly these two errors.
func (a *App) acquireBundle(ctx context.Context, chID channel.ID) (channelhost.Bundle, error) {
	exists, err := a.channelExists(ctx, string(chID))
	if err != nil {
		return nil, errors.Join(errChannelUnavailable, err)
	}
	if !exists {
		return nil, errChannelNotFound
	}
	if bundle, ok := a.host.Acquire(chID); ok {
		return bundle, nil
	}
	return nil, errChannelUnavailable
}

const (
	// defaultBoostClass is the engine class used by the ordinary boost agent
	// declaration. A client may choose that member as the channel default via
	// the normal set-default operation after joining.
	// An agent's engine IS its actor class — codex/script are flat registry
	// classes (kind=agent), there is NO umbrella "agent" class. boost has no
	// actor_decls declaration row, so it can't carry a default_class; it runs
	// this fixed deterministic engine. script requires a tool_id at construction;
	// channel genesis derives it from the co-created space-tool instance.
	defaultBoostClass = "script"
)
