// Package engineboot is the sole running-process assembly root.
package engineboot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/portal"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/web"
)

const shutdownTimeout = 30 * time.Second
const contractVersion = "5"

type Config struct {
	ChannelDBDir     string
	Addr             string
	TokenPath        string
	RootPassword     string
	StewardClass     string // agent class carved as c0 steward on first install; empty = codex
	OpenRegistration bool   // node policy: expose system.principal.create to the lobby (default closed)
}

type Engine struct {
	cfg            Config
	logger         *slog.Logger
	registry       *lagoon.Registry
	host           *channelhost.ChannelHost
	daemonHost     *daemonhost.Host
	dataIssuer     dataplane.Issuer
	dataRedeemer   dataplane.Redeemer
	dataBinder     dataplane.Binder
	closeDataPlane func(context.Context) error
	gateway        *gateway.Gateway
	sessions       *gateway.SessionStore
	handler        http.Handler
	server         *http.Server
	ready          chan struct{}
	boundAddr      string
	closeOnce      sync.Once
	closeErr       error
}

func Boot(cfg Config, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelDBDir == "" {
		return nil, errors.New("engineboot: channel db dir required")
	}
	installed, err := boot.Ensure(context.Background(), boot.Config{ChannelDir: cfg.ChannelDBDir, RootPassword: cfg.RootPassword, StewardClass: cfg.StewardClass})
	if err != nil {
		return nil, err
	}
	if installed.Installed && installed.RootPassword != "" {
		logger.Info("atoll installed", "root_password", installed.RootPassword)
	}
	// Sessions live beside the install data: restart keeps every login,
	// reinstall (directory wipe) is what revokes them.
	e := &Engine{cfg: cfg, logger: logger, ready: make(chan struct{}), sessions: gateway.OpenSessionStore(filepath.Join(filepath.Dir(cfg.ChannelDBDir), "sessions.json"))}
	var host *channelhost.ChannelHost
	var gatewayEdge *gateway.Gateway
	e.registry, err = lagoon.OpenWith(installed.RegistryDBPath, func(change lagoon.Change) {
		if host != nil && (change.ChannelID != "" || change.AllChannels) {
			host.RegistryChanged(change)
		}
		if change.Principal != "" && gatewayEdge != nil {
			gatewayEdge.Poke(change.Principal)
		}
		if change.AllPrincipals && gatewayEdge != nil {
			gatewayEdge.PokeAll()
		}
	}, lagoon.Policy{OpenRegistration: cfg.OpenRegistration})
	if err != nil {
		return nil, err
	}
	resolver := &assemblyResolver{registry: e.registry, logger: logger}
	// The init host opens only the already-installed c0. No gateway, device host,
	// other channel, or convergence loop exists before registrar reconciliation.
	e.host, err = channelhost.New(cfg.ChannelDBDir, e.registry, channelhost.HomeDeps{CompositionResolver: resolver, IntroductionResolver: resolver, RegistryBindings: e.registry, Logger: logger})
	if err != nil {
		return nil, e.fail(err)
	}
	host = e.host
	resolver.host = e.host
	resolver.registrar = lagoon.NewRegistrar(e.registry, sourceFacts{host: e.host, genesis: installed.C0Genesis}, resolver)
	if err := e.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: channelspec.C0ChannelID, ChannelName: "c0", ExpectedType: "group"}); err != nil {
		return nil, e.fail(fmt.Errorf("open c0: %w", err))
	}
	if _, ok := e.host.Acquire(channelspec.C0ChannelID); !ok {
		return nil, e.fail(errors.New("c0 did not publish"))
	}
	if err := resolver.registrar.ReconcileSystem(context.Background()); err != nil {
		return nil, e.fail(fmt.Errorf("reconcile registry system rows: %w", err))
	}
	if cfg.TokenPath != "" {
		if _, err := gateway.MintAutomationToken(e.sessions, channelspec.RootPrincipalID, cfg.TokenPath); err != nil {
			return nil, e.fail(err)
		}
	}

	// Cross the init line: retire the c0-only host, start line-side mechanisms,
	// then reopen c0 through the same ordinary ChannelHost path with full deps.
	host = nil
	if err := e.host.Close(context.Background()); err != nil {
		return nil, e.fail(fmt.Errorf("close init c0: %w", err))
	}
	e.dataIssuer, e.dataRedeemer, e.dataBinder, e.closeDataPlane = dataplane.New()
	e.daemonHost = daemonhost.New(daemonhost.Config{Logger: logger, DataPlane: e.dataRedeemer, Present: func(ctx context.Context) ([]channel.ID, error) {
		rows, err := e.registry.ListPresentChannels(ctx)
		out := make([]channel.ID, len(rows))
		for i, row := range rows {
			out[i] = row.ID
		}
		return out, err
	}, DaemonFact: func(ctx context.Context, id string) daemonhost.DaemonFact {
		status, ok, err := e.registry.GetDeviceFact(ctx, id)
		if err != nil {
			return daemonhost.DaemonUnavailable
		}
		if !ok || status == regspec.DeviceRetired {
			return daemonhost.DaemonDeleted
		}
		return daemonhost.DaemonAlive
	}})
	if err := e.dataBinder.BindHostStreamOpener(e.daemonHost); err != nil {
		return nil, e.fail(fmt.Errorf("bind dataplane host streams: %w", err))
	}
	e.host, err = channelhost.New(cfg.ChannelDBDir, e.registry, channelhost.HomeDeps{
		CompositionResolver:  resolver,
		IntroductionResolver: resolver,
		RegistryBindings:     e.registry,
		Logger:               logger,
		DaemonRoutes:         e.daemonHost,
		DataPlaneIssuer:      e.dataIssuer,
		DataPlaneRedeemer:    e.dataRedeemer,
		DeviceDirectory:      e.registry,
		OnMembraneOpen: func(ch channel.ID, generation uint64, membrane platform.DaemonMembrane) {
			e.daemonHost.Register(ch, generation, membrane)
			row, ok, err := e.registry.GetChannelDesired(context.Background(), ch)
			if err != nil {
				logger.Warn("channel owner lookup after membrane open failed", "channel", ch, "err", err)
				return
			}
			if ok && gatewayEdge != nil {
				gatewayEdge.Poke(row.OwnerPrincipal)
			}
		},
		OnMembraneClose: e.daemonHost.Unregister,
	})
	if err != nil {
		return nil, e.fail(err)
	}
	host = e.host
	resolver.host = e.host
	resolver.registrar = lagoon.NewRegistrar(e.registry, sourceFacts{host: e.host, genesis: installed.C0Genesis}, resolver)
	if err := e.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: channelspec.C0ChannelID, ChannelName: "c0", ExpectedType: "group"}); err != nil {
		return nil, e.fail(fmt.Errorf("reopen c0 after init: %w", err))
	}
	if _, ok := e.host.Acquire(channelspec.C0ChannelID); !ok {
		return nil, e.fail(errors.New("reopened c0 did not publish"))
	}
	e.gateway, err = gateway.New(gateway.Config{Resolver: gateway.ResolverFunc(e.resolveEntitlements), Logger: logger})
	if err != nil {
		return nil, e.fail(err)
	}
	gatewayEdge = e.gateway
	observationPlane := obs.New(obs.Config{
		Registry: registryObsAdapter{registry: e.registry},
		Channels: channelObsAdapter{host: e.host},
		Daemons:  daemonObsAdapter{host: e.daemonHost},
		Now:      func() int64 { return time.Now().UnixMilli() },
	})
	p := portal.New(portal.Config{Registry: e.registry, Lobby: e.acquireLobby, Sessions: e.sessions, Gateway: e.gateway, DaemonHost: e.daemonHost, DataPlane: e.dataRedeemer, Obs: observationPlane, ContractVersion: contractVersion, Boot: fmt.Sprintf("%s@%d", installed.C0Genesis.ChannelID, installed.C0Genesis.CreatedAt), Web: web.Assets()})
	e.handler = p
	e.gateway.Start()
	if err := e.host.StartConvergence(); err != nil {
		return nil, e.fail(err)
	}
	return e, nil
}

func (e *Engine) acquireLobby(ctx context.Context) (channelhost.Bundle, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if bundle, ok := e.host.Acquire(channelspec.LobbyChannelID); ok {
			return bundle, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) LocalDeviceKey(ctx context.Context) (string, error) {
	return e.registry.LocalDeviceKey(ctx)
}

func (e *Engine) resolveEntitlements(ctx context.Context, principal string) ([]gateway.Route, []channel.ID, error) {
	status, ok, err := e.registry.GetPrincipalStatus(ctx, principal)
	if err != nil {
		return nil, nil, err
	}
	if !ok || status != regspec.PrincipalPresent {
		return nil, nil, nil
	}
	rows, err := e.registry.ListPresentChannels(ctx)
	if err != nil {
		return nil, nil, err
	}
	var routes []gateway.Route
	var failed []channel.ID
	for _, row := range rows {
		bundle, ok := e.host.Acquire(row.ID)
		if !ok {
			failed = append(failed, row.ID)
			continue
		}
		id, found, err := bundle.View().ResolvePrincipal(ctx, principal)
		if err != nil {
			failed = append(failed, row.ID)
			continue
		}
		if found {
			routes = append(routes, gateway.Route{Channel: row.ID, Bundle: bundle, SubjectID: id})
		}
	}
	return routes, failed, nil
}

func (e *Engine) Ready() <-chan struct{} { return e.ready }
func (e *Engine) BoundAddr() string      { return e.boundAddr }
func (e *Engine) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", e.cfg.Addr)
	if err != nil {
		_ = e.Close(context.Background())
		return err
	}
	e.server = &http.Server{Handler: e.handler}
	e.boundAddr = ln.Addr().String()
	close(e.ready)
	done := make(chan error, 1)
	go func() {
		err := e.server.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()
	select {
	case err := <-done:
		_ = e.Close(context.Background())
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		shutdownErr := e.server.Shutdown(shutdownCtx)
		serveErr := <-done
		return errors.Join(shutdownErr, serveErr, e.Close(shutdownCtx))
	}
}
func (e *Engine) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		if e.host != nil {
			e.closeErr = errors.Join(e.closeErr, e.host.StopConvergence(ctx))
		}
		if e.server != nil {
			e.closeErr = errors.Join(e.closeErr, e.server.Shutdown(ctx))
		}
		if e.gateway != nil {
			e.closeErr = errors.Join(e.closeErr, e.gateway.Close())
		}
		if e.host != nil {
			e.closeErr = errors.Join(e.closeErr, e.host.Close(ctx))
		}
		if e.dataBinder != nil {
			e.dataBinder.UnbindHostStreamOpener()
		}
		if e.daemonHost != nil {
			e.closeErr = errors.Join(e.closeErr, e.daemonHost.Close(ctx))
		}
		if e.closeDataPlane != nil {
			e.closeErr = errors.Join(e.closeErr, e.closeDataPlane(ctx))
		}
		if e.registry != nil {
			e.closeErr = errors.Join(e.closeErr, e.registry.Close())
		}
	})
	return e.closeErr
}
func (e *Engine) fail(err error) error { _ = e.Close(context.Background()); return err }

var _ io.Closer = (*gateway.Gateway)(nil)
