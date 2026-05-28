// Package gateway is the server composition root — wires identity /
// catalog / placements / viewcache / daemonbus / devicebus / pushhub
// into one gin Engine and runs the background reconcile sweeps.
//
// Authoritative spec: launch-ticket notes §T6.
package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/catalog"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/devicebus"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/placements"
	"github.com/wanpengxie/ActOS/server/pushhub"
	"github.com/wanpengxie/ActOS/server/store"
	"github.com/wanpengxie/ActOS/server/viewcache"
)

// viewcacheReplayer adapts *viewcache.Service to the pushhub.Replayer
// interface: subscribe(since_seq=N) walks the persisted channel log via
// the viewcache messages projection. We keep the adapter package-local
// in gateway because pushhub deliberately does NOT import viewcache —
// the dependency points one way (gateway wires the adapter at boot,
// pushhub stays storage-agnostic so unit tests can fake the replay
// source without touching sqlite).
type viewcacheReplayer struct {
	vc *viewcache.Service
}

func (a viewcacheReplayer) ReplayMessages(ctx context.Context, channelID channel.ID, afterSeq viewsync.Seq, limit int) ([]pushhub.ReplayMessage, error) {
	if a.vc == nil {
		return nil, nil
	}
	rows, err := a.vc.Messages(ctx, channelID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	out := make([]pushhub.ReplayMessage, 0, len(rows))
	for _, r := range rows {
		out = append(out, pushhub.ReplayMessage{Seq: r.Seq, Envelope: r.Envelope})
	}
	return out, nil
}

// pkgLogger is the package-level zerolog handle used for boot-time
// warnings (dev-sentinel use). Tests can swap it via SetLogger.
//
// We don't import pkg/logger here — that would create a circular dep
// once the server package starts exposing helpers consumed by cmd/*.
// zerolog is already on the canUse list (M1.6-T7 phase-2 arch-lint
// update), so this is the canonical way to emit JSON warnings from
// inside the server tree.
var pkgLogger = zerolog.New(os.Stdout).With().
	Timestamp().
	Str("component", "server").
	Str("source", "gateway").
	Logger()

// SetLogger overrides the package-level logger. Used by tests to
// capture the dev-sentinel warning output without scraping stdlib log.
func SetLogger(l zerolog.Logger) { pkgLogger = l }

// Config bundles the construction-time settings.
type Config struct {
	DBPath                    string
	SessionSecret             string
	DaemonSharedSecret        string
	DeviceAllowedOrigins      []string
	DeviceAllowMissingOrigin  bool
	PushhubAllowedOrigins     []string
	DaemonbusAllowedOrigins   []string
	HumanCallerSecret         string
	BcryptCost                int
	ReconcileGracePeriod      time.Duration
	ReconcileCreateTimeout    time.Duration
	ReconcileHeartbeatTimeout time.Duration

	// UIDistDir is the absolute path to the ui/dist/ directory produced
	// by `pnpm --filter ui build`. When non-empty, buildEngine wires a
	// SPA static handler at "/" that serves the directory and falls back
	// to index.html for any non-API path. Empty (the default) preserves
	// the historical API-only behaviour — useful when ui/ is served by
	// a separate vite dev server during local development.
	UIDistDir string

	// InstallerDir is the absolute path to a directory containing
	// coagent-proxy_<os>_<arch> binaries served at /install/<filename>.
	// When non-empty, the gateway also serves /install/proxy.sh (the
	// one-line bootstrap script templated with this server's origin).
	// Empty (the default) disables the /install/* routes entirely.
	InstallerDir string

	// AllowDevSecrets controls FIX-T8 secret fail-fast. When false (the
	// production default), any empty secret or any value equal to a
	// well-known dev sentinel causes gateway.New to return an error.
	// Set true in dev / CI to keep the legacy "fall back to dev-*-change-
	// me" behavior with a startup warning.
	AllowDevSecrets bool

	// DB overrides DBPath when non-nil (tests).
	DB *sql.DB
}

// Dev sentinel values withDefaults rejects under
// AllowDevSecrets=false. Listed as package-level constants so test
// fixtures and cmd/server share the exact strings.
const (
	devSessionSecret     = "dev-session-secret-change-me"
	devDaemonSecret      = "dev-daemon-secret-change-me"
	devHumanCallerSecret = "dev-human-caller-secret-change-me"
)

var devWSAllowedOrigins = []string{
	"http://localhost:8832",
	"http://127.0.0.1:8832",
	"http://localhost:5173",
	"http://127.0.0.1:5173",
}

// ErrInsecureSecret is returned by withDefaults when a required secret
// is empty or equals one of the dev sentinels and AllowDevSecrets is
// false. Wraps the offending config field name so cmd/server can print
// an actionable error.
type ErrInsecureSecret struct {
	Field string
	Value string
}

// Error implements error.
func (e *ErrInsecureSecret) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("gateway: %s empty (set the env var or pass --allow-dev-secrets)", e.Field)
	}
	return fmt.Sprintf("gateway: %s equals dev sentinel %q (set the env var or pass --allow-dev-secrets)", e.Field, e.Value)
}

// ErrInsecureOrigin is returned by withDefaults when a browser-facing
// WebSocket Origin allowlist is missing or uses an allow-all sentinel in
// production mode.
type ErrInsecureOrigin struct {
	Field string
	Value string
}

func (e *ErrInsecureOrigin) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("gateway: %s empty (set exact origins or pass --allow-dev-secrets)", e.Field)
	}
	return fmt.Sprintf("gateway: %s contains insecure origin %q (use exact origins only)", e.Field, e.Value)
}

// ErrInsecureBcryptCost is returned when production config explicitly lowers
// bcrypt below the hardened minimum. Tests/dev may opt in via AllowDevSecrets.
type ErrInsecureBcryptCost struct {
	Cost int
}

func (e *ErrInsecureBcryptCost) Error() string {
	return fmt.Sprintf("gateway: BcryptCost=%d below production minimum %d (raise it or pass --allow-dev-secrets for dev/test)", e.Cost, identity.MinProductionBcryptCost)
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
	pushhub    *pushhub.Service
	daemonbus  *daemonbus.Service
	devicebus  *devicebus.Service

	rollbackRetryMu     sync.Mutex
	rollbackRetryActive map[string]struct{}
}

// New builds a composed App.
func New(ctx context.Context, cfg Config) (*App, error) {
	var err error
	cfg, err = withDefaults(cfg)
	if err != nil {
		return nil, err
	}

	var (
		db     *sql.DB
		closer func() error
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
		cfg:                 cfg,
		db:                  db,
		closer:              closer,
		rollbackRetryActive: map[string]struct{}{},
	}
	app.identity = identity.NewService(db, identity.Config{
		SessionSecret: cfg.SessionSecret,
		BcryptCost:    cfg.BcryptCost,
	})
	app.catalog = catalog.NewService(db)
	app.placements = placements.NewService(db, placements.Config{
		GracePeriod:      cfg.ReconcileGracePeriod,
		CreateTimeout:    cfg.ReconcileCreateTimeout,
		HeartbeatTimeout: cfg.ReconcileHeartbeatTimeout,
		Logger:           &pkgLogger,
	})
	app.viewcache = viewcache.NewService(db)
	app.pushhub = pushhub.NewService(pushhub.Config{
		AllowedOrigins: cfg.PushhubAllowedOrigins,
	})
	app.daemonbus = daemonbus.NewService(db, daemonbus.Config{
		SharedSecret:   cfg.DaemonSharedSecret,
		AllowedOrigins: cfg.DaemonbusAllowedOrigins,
	})
	app.devicebus = devicebus.NewService(db, devicebus.Config{
		AllowedOrigins:     cfg.DeviceAllowedOrigins,
		AllowMissingOrigin: cfg.DeviceAllowMissingOrigin,
	})

	// Wire viewcache → daemon resync via daemonbus.
	app.daemonbus.SetChannelDaemonResolver(app.placements)
	app.daemonbus.SetPlacementLoadReader(app.placements)
	app.daemonbus.SetRegisterHook(app.retryRollbackIntentsForRegisteredDaemon)
	app.daemonbus.SetUnregisterHook(app.handleDaemonDisconnect)
	app.placements.SetReclaimHandler(app.reclaimPlacement)
	app.viewcache.SetResyncer(&busResyncer{bus: app.daemonbus, viewcache: app.viewcache})
	app.viewcache.SetResyncCompletionNotifier(app)
	app.viewcache.SetAccessAuthorizer(app)
	app.pushhub.SetAccessAuthorizer(app)
	app.pushhub.SetReplayer(viewcacheReplayer{vc: app.viewcache})
	app.placements.SetAccessAuthorizer(app)
	app.catalog.SetSubscriptionRevoker(app.pushhub)
	app.catalog.SetPlacementHook(app)

	app.devicebus.SetAccessAuthorizer(app)
	app.devicebus.SetProxyDaemonNotifier(app)

	app.engine = buildEngine(app)
	return app, nil
}

// Close releases the database handle.
func (a *App) Close() error {
	if a.daemonbus != nil {
		if err := a.daemonbus.Close(); err != nil {
			return err
		}
	}
	if a.closer != nil {
		return a.closer()
	}
	return nil
}

// Handler returns the http.Handler for cmd/server.
func (a *App) Handler() http.Handler { return a.engine }

// Identity / Catalog / Placements / Viewcache / Daemonbus / Devicebus
// / Pushhub: test helpers.
func (a *App) Identity() *identity.Service     { return a.identity }
func (a *App) Catalog() *catalog.Service       { return a.catalog }
func (a *App) Placements() *placements.Service { return a.placements }
func (a *App) Viewcache() *viewcache.Service   { return a.viewcache }
func (a *App) Daemonbus() *daemonbus.Service   { return a.daemonbus }
func (a *App) Devicebus() *devicebus.Service   { return a.devicebus }
func (a *App) Pushhub() *pushhub.Service       { return a.pushhub }
func (a *App) DB() *sql.DB                     { return a.db }

func (a *App) handleDaemonDisconnect(conn *daemonbus.Connection) {
	if conn == nil || a.placements == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	changed, err := a.placements.MarkDaemonStale(ctx, conn.DaemonID, "daemon_disconnect")
	if err != nil {
		pkgLogger.Warn().Err(err).
			Str("event", "placement.daemon_disconnect_handoff_failed").
			Str("daemon_id", string(conn.DaemonID)).
			Msg("daemon disconnect handoff failed")
		return
	}
	if len(changed) > 0 {
		pkgLogger.Info().
			Str("event", "placement.daemon_disconnect_handoff").
			Str("daemon_id", string(conn.DaemonID)).
			Int("channels", len(changed)).
			Msg("daemon disconnect marked owned placements stale")
	}
}

// AuthorizeChannelAccess implements devicebus.AccessAuthorizer using the
// catalog membership table.
func (a *App) AuthorizeChannelAccess(ctx context.Context, channelID, userID string) error {
	_, err := a.catalog.GetChannelMember(ctx, channelID, userID)
	return err
}

// MemberActorID implements channelaccess.MemberActorResolver.
func (a *App) MemberActorID(ctx context.Context, channelID, userID string) (string, error) {
	member, err := a.catalog.GetChannelMember(ctx, channelID, userID)
	if err != nil {
		return "", err
	}
	return member.MemberActorID, nil
}

// RunReconcile blocks until ctx is cancelled, running the placements
// reconcile loop plus lightweight server-side recovery sweeps.
func (a *App) RunReconcile(ctx context.Context) {
	go a.placements.RunReconcile(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	runSweeps := func() {
		if err := a.viewcache.RecoverGaps(ctx); err != nil && !errors.Is(err, context.Canceled) {
			pkgLogger.Warn().Err(err).
				Str("event", "viewcache.gap_recover_failed").
				Msg("viewcache gap recovery sweep failed")
		}
		if err := a.sweepRollbackIntents(ctx); err != nil && !errors.Is(err, context.Canceled) {
			pkgLogger.Warn().Err(err).
				Str("event", "placement.rollback_sweep_failed").
				Msg("placement rollback intent sweep failed")
		}
		if _, err := a.catalog.ProcessDueMemberTransitions(ctx, 100); err != nil && !errors.Is(err, context.Canceled) {
			pkgLogger.Warn().Err(err).
				Str("event", "catalog.member_transition_failed").
				Msg("catalog member transition outbox processing failed")
		}
	}
	runSweeps()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runSweeps()
		}
	}
}

// withDefaults applies reconcile-loop default durations and runs the
// FIX-T8 secret fail-fast gate. Empty secrets and dev sentinel values
// are rejected unless AllowDevSecrets=true, in which case withDefaults
// installs the dev sentinel and emits a warning so operators can spot
// the misconfiguration in logs.
func withDefaults(cfg Config) (Config, error) {
	if cfg.ReconcileGracePeriod <= 0 {
		cfg.ReconcileGracePeriod = 60 * time.Second
	}
	if cfg.ReconcileCreateTimeout <= 0 {
		cfg.ReconcileCreateTimeout = 30 * time.Second
	}
	if cfg.ReconcileHeartbeatTimeout <= 0 {
		cfg.ReconcileHeartbeatTimeout = 90 * time.Second
	}
	if cfg.BcryptCost > 0 && cfg.BcryptCost < identity.MinProductionBcryptCost && !cfg.AllowDevSecrets {
		return cfg, &ErrInsecureBcryptCost{Cost: cfg.BcryptCost}
	}

	type secretSlot struct {
		field    string
		ptr      *string
		devValue string
	}
	slots := []secretSlot{
		{"SessionSecret", &cfg.SessionSecret, devSessionSecret},
		{"DaemonSharedSecret", &cfg.DaemonSharedSecret, devDaemonSecret},
		{"HumanCallerSecret", &cfg.HumanCallerSecret, devHumanCallerSecret},
	}
	for _, s := range slots {
		current := *s.ptr
		insecure := current == "" || current == s.devValue
		if !insecure {
			continue
		}
		if !cfg.AllowDevSecrets {
			return cfg, &ErrInsecureSecret{Field: s.field, Value: current}
		}
		pkgLogger.Warn().
			Str("event", "gateway.dev_sentinel_used").
			Str("field", s.field).
			Str("dev_value", s.devValue).
			Msg("dev sentinel installed for missing/insecure secret — DO NOT USE IN PRODUCTION")
		*s.ptr = s.devValue
	}
	type originSlot struct {
		field string
		ptr   *[]string
	}
	originSlots := []originSlot{
		{"DeviceAllowedOrigins", &cfg.DeviceAllowedOrigins},
		{"PushhubAllowedOrigins", &cfg.PushhubAllowedOrigins},
		{"DaemonbusAllowedOrigins", &cfg.DaemonbusAllowedOrigins},
	}
	for _, s := range originSlots {
		origins, bad := normalizeStartupOrigins(*s.ptr)
		if bad != "" {
			return cfg, &ErrInsecureOrigin{Field: s.field, Value: bad}
		}
		if len(origins) == 0 {
			if !cfg.AllowDevSecrets {
				return cfg, &ErrInsecureOrigin{Field: s.field}
			}
			origins = append([]string(nil), devWSAllowedOrigins...)
		}
		*s.ptr = origins
	}
	return cfg, nil
}

func normalizeStartupOrigins(origins []string) ([]string, string) {
	out := make([]string, 0, len(origins))
	seen := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if origin == "*" {
			return nil, origin
		}
		if _, ok := seen[origin]; ok {
			continue
		}
		seen[origin] = struct{}{}
		out = append(out, origin)
	}
	return out, ""
}
