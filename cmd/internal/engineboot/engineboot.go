// Package engineboot is the sole running-process assembly root.
package engineboot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/portal"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const shutdownTimeout = 30 * time.Second
const provisionStepTimeout = 30 * time.Second
const contractVersion = "5"

type Config struct {
	ChannelDBDir string
	Addr         string
	TokenPath    string
	RootPassword string
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
	submitter      lagoon.Submitter
	handler        http.Handler
	server         *http.Server
	ready          chan struct{}
	boundAddr      string
	closeOnce      sync.Once
	closeErr       error
	// provisionTimeout is fixed in production and narrowed only by package tests.
	provisionTimeout time.Duration
}

func Boot(cfg Config, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelDBDir == "" {
		return nil, errors.New("engineboot: channel db dir required")
	}
	installed, err := boot.Ensure(context.Background(), boot.Config{ChannelDir: cfg.ChannelDBDir, RootPassword: cfg.RootPassword})
	if err != nil {
		return nil, err
	}
	if installed.Installed && installed.RootPassword != "" {
		logger.Info("atoll installed", "root_password", installed.RootPassword)
	}
	e := &Engine{cfg: cfg, logger: logger, ready: make(chan struct{}), sessions: gateway.NewSessionStore(), provisionTimeout: provisionStepTimeout}
	var host *channelhost.ChannelHost
	var gatewayEdge *gateway.Gateway
	e.registry, err = lagoon.Open(installed.RegistryDBPath, func(change lagoon.Change) {
		if host != nil && (change.ChannelID != "" || change.AllChannels) {
			host.RegistryChanged(change)
		}
		if change.Principal != "" && gatewayEdge != nil {
			gatewayEdge.Poke(change.Principal)
		}
		if change.AllPrincipals && gatewayEdge != nil {
			gatewayEdge.PokeAll()
		}
	})
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
	resolver.registrar = lagoon.NewRegistrar(e.registry, sourceFacts{host: e.host, genesis: installed.C0Genesis}, resolver)
	if err := e.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: protocol.C0ChannelID, ChannelName: "c0", ExpectedType: "group"}); err != nil {
		return nil, e.fail(fmt.Errorf("open c0: %w", err))
	}
	c0, ok := e.host.Acquire(protocol.C0ChannelID)
	if !ok {
		return nil, e.fail(errors.New("c0 did not publish"))
	}
	if err := resolver.registrar.ReconcileSystem(context.Background()); err != nil {
		return nil, e.fail(fmt.Errorf("reconcile registry system rows: %w", err))
	}
	if cfg.TokenPath != "" {
		if _, err := gateway.MintAutomationToken(e.sessions, protocol.RootPrincipalID, cfg.TokenPath); err != nil {
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
	resolver.registrar = lagoon.NewRegistrar(e.registry, sourceFacts{host: e.host, genesis: installed.C0Genesis}, resolver)
	if err := e.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: protocol.C0ChannelID, ChannelName: "c0", ExpectedType: "group"}); err != nil {
		return nil, e.fail(fmt.Errorf("reopen c0 after init: %w", err))
	}
	c0, ok = e.host.Acquire(protocol.C0ChannelID)
	if !ok {
		return nil, e.fail(errors.New("reopened c0 did not publish"))
	}
	registrarCaller := c0RegistrarCaller{bundle: c0}
	e.submitter = lagoon.NewSubmitter(registrarCaller)
	resolver.caller = registrarCaller
	e.gateway, err = gateway.New(gateway.Config{Resolver: gateway.ResolverFunc(e.resolveEntitlements), Logger: logger})
	if err != nil {
		return nil, e.fail(err)
	}
	gatewayEdge = e.gateway
	p := portal.New(portal.Config{Registry: e.registry, Submitter: e.submitter, Sessions: e.sessions, Gateway: e.gateway, DaemonHost: e.daemonHost, DataPlane: e.dataRedeemer, ContractVersion: contractVersion})
	e.handler = p
	e.gateway.Start()
	if err := e.host.StartConvergence(); err != nil {
		return nil, e.fail(err)
	}
	return e, nil
}

type c0RegistrarCaller struct{ bundle channelhost.Bundle }

func (c c0RegistrarCaller) CallRegistrar(ctx context.Context, word lagoon.Word, payload any) (json.RawMessage, error) {
	if c.bundle == nil {
		return nil, errors.New("lagoon: c0 bundle unavailable")
	}
	ids, err := c.bundle.View().DeclaredInstances(ctx, lagoon.RegistrarSeatDeclID)
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("lagoon: registrar seat unavailable")
	}
	raw, err := c.bundle.Call().Call(ctx, ids[0], string(word), payload)
	if err != nil {
		return nil, err
	}
	return decodeRegistrarTerminal(raw)
}

func decodeRegistrarTerminal(raw json.RawMessage) (json.RawMessage, error) {
	var terminal struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
		Detail    string `json:"detail"`
	}
	if err := json.Unmarshal(raw, &terminal); err != nil {
		return nil, fmt.Errorf("lagoon: decode registrar terminal: %w", err)
	}
	switch terminal.Status {
	case message.StatusCompleted:
		return append(json.RawMessage(nil), raw...), nil
	case message.StatusFailed:
		if terminal.ErrorCode != "" {
			return nil, &lagoon.Error{Code: lagoon.ErrorCode(terminal.ErrorCode), Detail: terminal.Detail}
		}
		return nil, errors.New("lagoon: registrar call failed")
	default:
		return nil, fmt.Errorf("lagoon: unknown registrar terminal status %q", terminal.Status)
	}
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
			routes = append(routes, gateway.Route{Channel: row.ID, ChannelName: row.QualifiedName, Bundle: bundle, SubjectID: id})
		}
	}
	return routes, failed, nil
}

type ProvisionResult struct {
	HomeChannelID channel.ID
	DeviceID      string
	DeviceKey     string
}

func (e *Engine) ProvisionLocalNode(ctx context.Context) (ProvisionResult, error) {
	rootActor, err := e.principalActor(ctx, protocol.C0ChannelID, protocol.RootPrincipalID)
	if err != nil {
		return ProvisionResult{}, err
	}
	call := func(word lagoon.Word, payload any) (lagoon.Reply, error) {
		return e.submitter.Submit(ctx, lagoon.SubmitIn{Source: protocol.C0ChannelID, Sender: rootActor, RequestID: uuid.NewString(), Word: word, Payload: payload})
	}
	if _, err := call(lagoon.WordDeviceAttach, lagoon.DeviceBinding{ChannelID: protocol.C0ChannelID, DeviceID: protocol.LocalDeviceID}); err != nil {
		return ProvisionResult{}, err
	}
	reply, err := call(lagoon.WordChannelCreate, lagoon.ChannelCreate{Name: protocol.RootPrincipalID, Parent: protocol.C0ChannelID})
	if err != nil {
		return ProvisionResult{}, err
	}
	var home regspec.ChannelRow
	if err := reply.DecodeValue(&home); err != nil {
		return ProvisionResult{}, fmt.Errorf("provision: invalid home reply: %w", err)
	}
	if err := e.waitChannel(ctx, home.ID); err != nil {
		return ProvisionResult{}, err
	}
	stewardDecl := lagoon.StableBootstrapDeclID(protocol.RootPrincipalID, "steward")
	if _, err := call(lagoon.WordDeclRegister, lagoon.DeclRegister{ID: stewardDecl, Name: "Steward", Class: "codex", Config: json.RawMessage(`{}`), Visibility: "private"}); err != nil {
		return ProvisionResult{}, err
	}
	if err := e.introduce(ctx, protocol.C0ChannelID, protocol.RootPrincipalID, stewardDecl, protocol.StewardPrincipalID); err != nil {
		return ProvisionResult{}, err
	}
	homeActor, err := e.principalActor(ctx, home.ID, protocol.RootPrincipalID)
	if err != nil {
		return ProvisionResult{}, err
	}
	homeCall := func(word lagoon.Word, payload any) (lagoon.Reply, error) {
		return e.submitter.Submit(ctx, lagoon.SubmitIn{Source: home.ID, Sender: homeActor, RequestID: uuid.NewString(), Word: word, Payload: payload})
	}
	codexID := lagoon.HomeCodexDeclID(protocol.RootPrincipalID)
	if _, err := homeCall(lagoon.WordDeclRegister, lagoon.DeclRegister{ID: codexID, Name: "home-codex", Class: "codex", Config: json.RawMessage(`{}`), Visibility: "private"}); err != nil {
		return ProvisionResult{}, err
	}
	if err := e.introduce(ctx, home.ID, protocol.RootPrincipalID, codexID, ""); err != nil {
		return ProvisionResult{}, err
	}
	device, ok, err := e.registry.GetDevice(ctx, protocol.LocalDeviceID)
	if err != nil || !ok {
		return ProvisionResult{}, errors.Join(err, errors.New("local device missing"))
	}
	return ProvisionResult{HomeChannelID: home.ID, DeviceID: device.ID, DeviceKey: device.Key}, nil
}

func (e *Engine) principalActor(ctx context.Context, ch channel.ID, principal string) (actor.ActorID, error) {
	waitCtx, cancel := context.WithTimeout(ctx, e.provisionWait())
	defer cancel()
	deadline := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	for {
		if bundle, ok := e.host.Acquire(ch); ok {
			id, found, err := bundle.View().ResolvePrincipal(waitCtx, principal)
			if err != nil {
				return "", err
			}
			if found {
				return id, nil
			}
		}
		select {
		case <-waitCtx.Done():
			return "", fmt.Errorf("provision: wait for principal %q in channel %q: %w", principal, ch, waitCtx.Err())
		case <-deadline.C:
		}
	}
}
func (e *Engine) waitChannel(ctx context.Context, ch channel.ID) error {
	waitCtx, cancel := context.WithTimeout(ctx, e.provisionWait())
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, ok := e.host.Acquire(ch); ok {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("provision: wait for channel %q: %w", ch, waitCtx.Err())
		case <-ticker.C:
		}
	}
}
func (e *Engine) introduce(ctx context.Context, ch channel.ID, principal, declID, ownPrincipal string) error {
	waitCtx, cancel := context.WithTimeout(ctx, e.provisionWait())
	defer cancel()
	bundle, ok := e.host.Acquire(ch)
	if !ok {
		return errors.New("channel unavailable")
	}
	sender, found, err := bundle.View().ResolvePrincipal(waitCtx, principal)
	if err != nil || !found {
		return errors.Join(err, errors.New("principal is not a member"))
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(sender)
	if !ok {
		return errors.New("subject slot unavailable")
	}
	payload := map[string]any{"kind": "agent", "decl_id": declID}
	if ownPrincipal != "" {
		payload["principal"] = ownPrincipal
	}
	requestID := message.ID(uuid.NewString())
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{ChannelID: string(ch), ID: string(requestID), MsgType: "channel.introduce_actor", Kind: "request", Audience: []string{string(actor.SystemActorID)}, Visibility: "public", Payload: mustJSON(payload)})
	if err != nil {
		return err
	}
	if _, err := slot.Deliver(waitCtx, frame); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	reader := channel.Reader{ActorID: sender, Mode: channel.ReaderMember}
	var cursor int64
	for {
		ids, err := bundle.View().DeclaredInstances(waitCtx, declID)
		if err != nil {
			return fmt.Errorf("provision: inspect introduced declaration %q: %w", declID, err)
		}
		if len(ids) == 1 {
			return nil
		}
		if len(ids) > 1 {
			return fmt.Errorf("provision: declaration %q has %d instances, want one", declID, len(ids))
		}
		rows, next, err := bundle.View().ReadVisibleAfterSeq(waitCtx, reader, cursor, 256)
		if err != nil {
			return fmt.Errorf("provision: read introduce terminal for %q: %w", declID, err)
		}
		cursor = next
		for _, row := range rows {
			if !row.IsTerminal || row.Envelope.Kind != message.KindResponse || row.Envelope.ParentID != requestID {
				continue
			}
			var terminal struct {
				Status    string `json:"status"`
				ErrorCode string `json:"error_code"`
				Detail    string `json:"detail"`
			}
			if err := json.Unmarshal(row.Envelope.Payload, &terminal); err != nil {
				return fmt.Errorf("provision: decode introduce terminal for %q: %w", declID, err)
			}
			switch terminal.Status {
			case message.StatusCompleted:
				// The terminal is only a wake; the value check above remains the
				// success condition and also handles idempotent replay.
			case message.StatusFailed:
				ids, err := bundle.View().DeclaredInstances(waitCtx, declID)
				if err == nil && len(ids) == 1 {
					return nil
				}
				return fmt.Errorf("provision: introduce %q rejected (%s): %s", declID, terminal.ErrorCode, terminal.Detail)
			default:
				return fmt.Errorf("provision: introduce %q returned unknown terminal status %q", declID, terminal.Status)
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("provision: wait for declaration %q instance: %w", declID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (e *Engine) provisionWait() time.Duration {
	if e.provisionTimeout > 0 {
		return e.provisionTimeout
	}
	return provisionStepTimeout
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
func mustJSON(v any) json.RawMessage   { raw, _ := json.Marshal(v); return raw }

var _ io.Closer = (*gateway.Gateway)(nil)
