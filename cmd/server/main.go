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
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
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
// satisfies it).
type appShutdowner interface {
	Shutdown(context.Context) error
	Close() error
}

// gracefulShutdown runs the ordered teardown — the order IS the semantics: ①
// drain the HTTP entry (stop accepting, finish in-flight), ② silence the gateway
// (关站全序: 停在场圈 → close every session → join read pumps → 等已获准递交归零 —
// gateway先静默 before Home, 连接模型勘误期 §3.2 / DoD-9), ③ close channel homes (the
// substrate behind the entry), ④ close the app db. Each step logs before it runs so
// the order is assertable. All run even if an earlier one errors; errors are joined.
func gracefulShutdown(ctx context.Context, logger *slog.Logger, a appShutdowner, gw io.Closer, db io.Closer) error {
	logger.Info("server: shutdown step 1/4: draining http")
	e1 := a.Shutdown(ctx)
	logger.Info("server: shutdown step 2/4: silencing gateway")
	e2 := gw.Close()
	logger.Info("server: shutdown step 3/4: closing channel homes")
	e3 := a.Close()
	logger.Info("server: shutdown step 4/4: closing app db")
	e4 := db.Close()
	return errors.Join(e1, e2, e3, e4)
}

// gatewayResolver bridges the app's own entitlement DTO into the gateway's
// EntitlementResolver seam (连接模型勘误期 §3.2: app → drivers is fenced, so the
// assembly root maps DTO→DTO). The app resolves 户籍 ∪ 读资格 per principal; the bridge
// only translates the flat access class into gateway.AccessClass.
func gatewayResolver(a *app.App) gateway.EntitlementResolver {
	return gateway.ResolverFunc(func(ctx context.Context, principal string) ([]gateway.Route, []channel.ID, error) {
		routes, failed, err := a.EntitlementSnapshot(ctx, principal)
		if err != nil {
			return nil, nil, err
		}
		gr := make([]gateway.Route, 0, len(routes))
		for _, r := range routes {
			access := gateway.AccessObserver
			if r.Access == "member" {
				access = gateway.AccessMember
			}
			gr = append(gr, gateway.Route{
				Channel:   r.Channel,
				Home:      r.Home,
				Access:    access,
				SubjectID: r.SubjectID,
			})
		}
		return gr, failed, nil
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

	a, err := app.New(app.Config{
		DB:           appDB,
		Logger:       logger,
		ChannelDBDir: *channelDBDir,
		UIDist:       *uiDist,
		// drivers-side diagnostic obs vocabulary: the assembly root is the only
		// legal drivers/* importer (围栏 Fence B), so the agentbase kinds are
		// supplied here, not assembled inside app.
		ExtraDropKinds: agentbase.ObsDropKinds(),
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	// Human-ingress gateway (gateway 期 S3, 连接模型勘误期): constructed AFTER the app so
	// it can hold the app's routing + entitlement面, then injected back (the construction
	// cycle is broken by the setters). The platform membership-change emit points
	// (home.Home.Admit/Remove via home.Config.OnMembershipChange) call Gateway.Poke
	// directly; the entitlement resolver bridges the app's own DTO into gateway.Route
	// (app → drivers is fenced, so the assembly root does the DTO→DTO map here).
	gw, err := gateway.New(gateway.Config{
		Routing:  a.ResolveRoutingForGateway,
		Resolver: gatewayResolver(a),
		Logger:   logger,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	gw.Start()
	a.SetGateway(web.New(gw))
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
