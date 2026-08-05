// Command server runs the atoll application server.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"

	// Composition root wires the catalog: the BINARY pins which classes are
	// compiled in — not the app library (which stays impl-agnostic, so
	// `go test ./app` can register its own stub). Both assembly roots (server +
	// daemon) import the SAME catalog so placement can name any class the server
	// might host (G21): whether it actually runs is answered honestly by
	// Build/creds, not gated by binary contents. agent/all = the LLM engine
	// classes (go-kimi + claude); actors/all = tools + devices.
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

// shutdownTimeout bounds the in-flight drain so a wedged request cannot hold the
// process open forever.
const shutdownTimeout = 30 * time.Second

// appShutdowner is the graceful-teardown surface gracefulShutdown drives (App
// satisfies it). Close takes the same shutdown budget as the drain: every step
// spends from one purse, and a step that exhausts it abandons its stragglers
// with an account instead of holding the process for the supervisor's KILL.
type appShutdowner interface {
	Shutdown(context.Context) error
	Close(context.Context) error
}

// gracefulShutdown runs the ordered teardown — the order IS the semantics: ①
// drain the HTTP entry (stop accepting, finish in-flight), ② silence the gateway
// (关站全序: 停在场圈 → close every session → join read pumps → 等已获准递交归零 —
// gateway先静默 before ChannelHost, 连接模型勘误期 §3.2 / DoD-9), ③ close realm
// workers and ChannelHost (the substrate behind the entry), ④ close the app db.
// Each step logs before it runs so
// the order is assertable. All run even if an earlier one errors; errors are joined.
func gracefulShutdown(ctx context.Context, logger *slog.Logger, a appShutdowner, gw io.Closer, db io.Closer) error {
	logger.Info("server: shutdown step 1/4: draining http")
	e1 := a.Shutdown(ctx)
	logger.Info("server: shutdown step 2/4: silencing gateway")
	e2 := gw.Close()
	logger.Info("server: shutdown step 3/4: closing realm workers and channel host")
	e3 := a.Close(ctx)
	logger.Info("server: shutdown step 4/4: closing app db")
	e4 := db.Close()
	return errors.Join(e1, e2, e3, e4)
}

// gatewayResolver bridges the app's own entitlement DTO into the gateway's
// EntitlementResolver seam (连接模型勘误期 §3.2: app → drivers is fenced, so the
// assembly root maps DTO→DTO). The app resolves each principal's memberships;
// the bridge is a pure field-for-field carry.
func gatewayResolver(a *app.App) gateway.EntitlementResolver {
	return gateway.ResolverFunc(func(ctx context.Context, principal string) ([]gateway.Route, []channel.ID, error) {
		routes, failed, err := a.EntitlementSnapshot(ctx, principal)
		if err != nil {
			return nil, nil, err
		}
		gr := make([]gateway.Route, 0, len(routes))
		for _, r := range routes {
			gr = append(gr, gateway.Route{
				Channel:   r.Channel,
				Bundle:    r.Bundle,
				SubjectID: r.SubjectID,
			})
		}
		return gr, failed, nil
	})
}

func gatewayObserverResolver(a *app.App) gateway.ObserverResolver {
	return gateway.ObserverResolverFunc(func(ctx context.Context, principal string, chID channel.ID) (gateway.ObserverRoute, string, error) {
		route, reason, err := a.ResolveObservation(ctx, principal, chID)
		if err != nil || reason != "" {
			return gateway.ObserverRoute{}, reason, err
		}
		return gateway.ObserverRoute{Channel: route.Channel, Bundle: route.Bundle, Reader: route.Reader}, "", nil
	})
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "atoll.db", "app database path")
	channelDBDir := flag.String("channel-db-dir", "/tmp/atoll-dev/channels", "directory for channel databases")
	uiDist := flag.String("ui-dist", "", "path to the built web UI (atoll-web repo's dist/); empty = API-only")
	initDB := flag.Bool("init", false, "create a new app database; omit to open an existing database")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Seed config from .env (dev convenience) before anything reads the
	// environment. An already-exported variable wins; a missing file is a
	// no-op. The built-in agent's KIMI_* creds enter the process here.
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("server: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("server: loaded .env", "vars_set", n)
	}

	processDB, err := app.OpenProcessDB(*dbPath, *initDB)
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	defer processDB.Close()
	appDB := processDB.DB

	// Bootstrap the local automation principal (contract D3 / 交接包①): --init
	// mints the owner user + a bearer token and drops it next to the app db, so
	// shells/scripts read one file and have identity. Path is a release detail,
	// not contract.
	if *initDB {
		tokenPath := filepath.Join(filepath.Dir(*dbPath), "atoll-token")
		if _, err := app.BootstrapOwnerToken(context.Background(), appDB, tokenPath); err != nil {
			// --init refuses an existing database, so a half-initialized install
			// must not survive this failure: drop the fresh db to keep --init
			// retryable. (log.Fatalf skips defers — close explicitly first.)
			processDB.Close()
			_ = os.Remove(*dbPath)
			log.Fatalf("server: %v", err)
		}
		logger.Info("server: bootstrap owner token written", "path", tokenPath)
	}

	a, err := app.New(app.Config{
		DB:     appDB,
		Logger: logger,
		HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(*channelDBDir, deps)
		},
		UIDist: *uiDist,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	// Human-ingress gateway (gateway 期 S3, 连接模型勘误期): constructed AFTER the app so
	// it can hold the app's routing + entitlement面, then injected back (the construction
	// cycle is broken by the setters). ChannelHost wires membrane relation-change
	// emit points through HomeDeps.OnRelationChange to Gateway.Poke
	// directly; the entitlement resolver bridges the app's own DTO into gateway.Route
	// (app → drivers is fenced, so the assembly root does the DTO→DTO map here).
	gw, err := gateway.New(gateway.Config{
		Resolver: gatewayResolver(a),
		Observer: gatewayObserverResolver(a),
		Logger:   logger,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	gw.Start()
	a.SetGateway(web.New(gw, contract.Version))
	a.SetMembershipPoke(gw.Poke)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- a.Run(*addr) }()

	select {
	case err := <-serveErr:
		// Run returned before any signal — a listen/startup failure.
		if err != nil {
			log.Fatalf("server: %v", err)
		}
	case <-ctx.Done():
		stop() // restore default handling: a second signal hard-kills a stuck drain
		logger.Info("server: signal received, shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := gracefulShutdown(shutdownCtx, logger, a, gw, processDB); err != nil {
			logger.Error("server: shutdown", "err", err.Error())
		}
	}
}
