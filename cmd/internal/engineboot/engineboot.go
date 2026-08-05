// Package engineboot is the engine assembly root shared by the two binaries
// that run a server (cmd/server, and cmd/atoll's `up`): open the process db,
// build the App, wire the human-ingress gateway, start the convergence arm,
// serve until signal, tear down in order. One assembly, two packagings — the
// wiring semantics (resolver bridges, phase order, shutdown order) live here
// exactly once, and ONLY here: Engine's internals are private, so every
// packaging drives the engine through the same four doors
// (ProvisionLocalNode / RotateOwnerToken / Serve+Ready / Close) and none can
// reach the db or gateway to invent its own boot or teardown.
//
// Phase order is the point ("先布置店面，再开门营业"): Boot wires AND starts
// the background convergence arm with the port still closed, so provisioning
// runs against a live engine that nobody outside can see yet; Serve binds the
// listener LAST and closes Ready() the moment it exists. A connectable node
// is therefore always a fully-provisioned node, and readiness is a fact the
// engine states — never something a caller infers by poking the port.
//
// NOTE: engineboot must never import drivers/devicehost — the server assembly
// is storage-free by 期11 §8.2's red line (storagehost's doc pins it); the
// device carrier is composed NEXT TO the engine by cmd/atoll, not inside it.
package engineboot

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// shutdownTimeout bounds the in-flight drain so a wedged request cannot hold
// the process open forever.
const shutdownTimeout = 30 * time.Second

// Config selects the engine's stores and listen address.
type Config struct {
	DBPath       string
	ChannelDBDir string
	Addr         string
	UIDist       string
	InitDB       bool
}

// Engine is a booted engine: everything wired, the convergence arm running,
// nothing bound to the listen address yet. Provision between Boot and Serve.
type Engine struct {
	app       *app.App
	processDB *app.ProcessDB
	gw        *gateway.Gateway
	cfg       Config
	logger    *slog.Logger
	ready     chan struct{}
	boundAddr string
}

// Boot opens stores, wires the full assembly (app + gateway + connector) and
// starts the background convergence arm — everything except the listener.
func Boot(cfg Config, logger *slog.Logger) (*Engine, error) {
	processDB, err := app.OpenProcessDB(cfg.DBPath, cfg.InitDB)
	if err != nil {
		return nil, err
	}
	a, err := app.New(app.Config{
		DB:     processDB.DB,
		Logger: logger,
		HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(cfg.ChannelDBDir, deps)
		},
		UIDist: cfg.UIDist,
	})
	if err != nil {
		processDB.Close()
		return nil, err
	}
	// Human-ingress gateway (gateway 期 S3, 连接模型勘误期): constructed AFTER the
	// app so it can hold the app's routing + entitlement面, then injected back
	// (the construction cycle is broken by the setters). ChannelHost wires
	// membrane relation-change emit points through HomeDeps.OnRelationChange to
	// Gateway.Poke directly; the entitlement resolver bridges the app's own DTO
	// into gateway.Route (app → drivers is fenced, so the assembly root does the
	// DTO→DTO map here).
	gw, err := gateway.New(gateway.Config{
		Resolver: gatewayResolver(a),
		Observer: gatewayObserverResolver(a),
		Logger:   logger,
	})
	if err != nil {
		processDB.Close()
		return nil, err
	}
	gw.Start()
	a.SetGateway(web.New(gw, contract.Version))
	a.SetMembershipPoke(gw.Poke)
	// Assembly complete — start the convergence arm now, with the port still
	// closed: reopened channels converge and provisioning verbs work while the
	// node is not yet visible to anyone.
	a.Start()
	return &Engine{app: a, processDB: processDB, gw: gw, cfg: cfg, logger: logger, ready: make(chan struct{})}, nil
}

// Ready is closed the moment the listener is bound (inside Serve) — the
// engine's own readiness statement, for whoever composes alongside it (the
// in-process device carrier dials only after this).
func (e *Engine) Ready() <-chan struct{} { return e.ready }

// BoundAddr is the listener's ACTUAL address (":0" resolved to a real port).
// Valid only after Ready is closed; callers must synchronize via Ready.
// Anything dialing the engine must use this, never the configured address
// string.
func (e *Engine) BoundAddr() string { return e.boundAddr }

// ProvisionLocalNode provisions the node against the booted (not yet
// listening) engine. The caller resolves the spec from the node home (token
// path, the device home's identity claim) and persists the resulting identity
// back — the engine never touches device storage.
func (e *Engine) ProvisionLocalNode(ctx context.Context, spec app.ProvisionSpec) (app.ProvisionResult, error) {
	return e.app.ProvisionLocalNode(ctx, spec)
}

// RotateOwnerToken rotates the local automation credential (`--init`'s minimal
// bootstrap; the same primitive ProvisionLocalNode runs first).
func (e *Engine) RotateOwnerToken(ctx context.Context, tokenPath string) error {
	_, err := app.BootstrapOwnerToken(ctx, e.processDB.DB, tokenPath)
	return err
}

// Serve binds the listen address (closing Ready), runs the HTTP entry until
// ctx is done or the entry fails, then runs the ordered graceful teardown.
// EVERY return path has torn the engine down — signal, runtime failure, even
// a bind failure (Ready stays open then) — so "Serve returned" always means
// "nothing is left running"; callers never clean up engine internals.
func (e *Engine) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", e.cfg.Addr)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return errors.Join(err, gracefulShutdown(shutdownCtx, e.logger, e.app, e.gw, e.processDB))
	}
	// Register the HTTP server BEFORE announcing ready or spawning the loop:
	// from here on, a teardown's Shutdown always reaches the server — no
	// window where a late-scheduled goroutine starts serving after teardown
	// already ran ("Serve returned ⇒ nothing left running" must hold on every
	// interleaving).
	run := e.app.PrepareServe(ln)
	e.boundAddr = ln.Addr().String()
	close(e.ready)
	serveErr := make(chan error, 1)
	go func() { serveErr <- run() }()

	select {
	case err := <-serveErr:
		// Serve returned before ctx ended — a startup/runtime failure. The
		// entry is dead but everything Boot started (gateway, channel host,
		// db) is not: run the SAME ordered teardown, so no caller ever
		// inherits half-open resources and "process death as cleanup".
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return errors.Join(err, gracefulShutdown(shutdownCtx, e.logger, e.app, e.gw, e.processDB))
	case <-ctx.Done():
		e.logger.Info("server: signal received, shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		shutdownErr := gracefulShutdown(shutdownCtx, e.logger, e.app, e.gw, e.processDB)
		// Join the serve goroutine before returning: Shutdown closed the
		// listener, so the loop's exit is imminent — but "imminent" is not
		// "returned", and the invariant is about EVERY goroutine this call
		// started, not just the resources it closed.
		return errors.Join(shutdownErr, <-serveErr)
	}
}

// Close tears down a booted engine that never served (a provision failure
// between Boot and Serve): the same ordered teardown — the HTTP drain no-ops
// with no listener — so the db is cleanly closed before any caller cleanup
// touches its files.
func (e *Engine) Close(ctx context.Context) error {
	return gracefulShutdown(ctx, e.logger, e.app, e.gw, e.processDB)
}

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
// Each step logs before it runs so the order is assertable. All run even if an
// earlier one errors; errors are joined.
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
