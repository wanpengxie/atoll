package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ManagerConfig parameterises NewManager. ChannelID, ActorRegistry,
// TypeRegistry, HarnessChain, RequestLookup are required; the rest get
// safe defaults when nil.
type ManagerConfig struct {
	// ChannelID is the channel this Manager scopes every adapter to.
	ChannelID channel.ID

	// ActorRegistry is the channel-local actor registry. Install
	// queries it to verify each Declaration.ActorID exists with the
	// right binding.
	ActorRegistry actorreg.Registry

	// TypeRegistry receives one Upsert per Declaration.Type at Install
	// time. Framework ships InMemoryTypeRegistry for tests.
	TypeRegistry TypeRegistry

	// TypeInstaller is the runtime-owned install path. Production wires
	// this so adapter install goes through the shared install validator
	// and emits the system.type.installed mutation mirror. Tests may omit
	// it and use TypeRegistry directly.
	TypeInstaller TypeInstaller

	// HarnessChain is the kernel/harness write entry point — every
	// adapter response flows through here.
	HarnessChain harness.Chain

	// RequestLookup recovers the original request envelope (for F5
	// Respond). Production wires it to runtime/store; tests use
	// MemoryRequestLookup.
	RequestLookup RequestLookup

	// DeviceTransit is optional; required only when a module declares
	// Binding == BindingRuntimeInboundViaRelay. The framework refuses
	// Install for such modules when DeviceTransit is nil.
	DeviceTransit devicetransit.DeviceTransit

	// HTTPClient is optional; modules with Binding == BindingRuntimeOutbound
	// receive it through adapter-specific setup rather than ModuleContext.
	// When nil for an runtime_outbound module, Install logs a warning but
	// still proceeds (tests may provide their own client via Init).
	HTTPClient *HTTPClient

	// StateStore is the F4 persistent state seam. The framework wires a
	// NamespacedStateStore per adapter on top.
	StateStore StateStore

	// CredentialStore is the F8 credential backend the framework hands
	// to every adapter via ModuleContext extension.
	CredentialStore CredentialStore

	// Clock provides the current time. Tests inject a fake clock.
	Clock func() time.Time

	// Logger receives framework + adapter diagnostics.
	Logger Logger

	// Metrics receives framework counters / histograms.
	Metrics Metrics

	// Tracer receives Span objects for Handle / OnExternalCallback.
	Tracer Tracer

	// GCInterval is how often Manager.RunGC scans for stale pending
	// entries. Defaults to 30s.
	GCInterval time.Duration

	// HeartbeatInterval overrides the binding-specific readiness
	// heartbeat cadence. Zero uses embedded=60s, runtime_outbound=15s,
	// runtime_inbound_via_relay=30s.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout bounds one Heartbeater call. Defaults to 5s.
	HeartbeatTimeout time.Duration

	// StatusTimeout bounds optional StatusReporter enrichment for
	// actor.status. Defaults to 5s.
	StatusTimeout time.Duration

	// EmitInitialReadinessEvent controls whether the initial readiness
	// observation after Install emits actor.readiness.changed. Unit tests
	// leave this false to avoid install-time writes; production daemon
	// enables it so server-side projections can bootstrap from view-sync.
	EmitInitialReadinessEvent bool
}

// TypeInstaller is the install-time service seam used by production.
// It deliberately mirrors the registry Upsert shape so framework code
// keeps constructing TypeRow values while runtime owns install semantics.
type TypeInstaller interface {
	InstallType(ctx context.Context, row TypeRow) (TypeRow, error)
}

func (c *ManagerConfig) applyDefaults() {
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.Logger == nil {
		c.Logger = NoopLogger{}
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics{}
	}
	if c.Tracer == nil {
		c.Tracer = NoopTracer{}
	}
	if c.StateStore == nil {
		c.StateStore = NewMemoryStateStore()
	}
	if c.CredentialStore == nil {
		c.CredentialStore = NewMemoryCredentialStore()
	}
	if c.TypeRegistry == nil {
		c.TypeRegistry = NewInMemoryTypeRegistry()
	}
	if c.GCInterval <= 0 {
		c.GCInterval = 30 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 5 * time.Second
	}
	if c.StatusTimeout <= 0 {
		c.StatusTimeout = 5 * time.Second
	}
}

// Validate returns an error when a required field is missing.
func (c ManagerConfig) Validate() error {
	switch {
	case c.ChannelID == "":
		return errors.New("framework: ManagerConfig.ChannelID required")
	case c.ActorRegistry == nil:
		return errors.New("framework: ManagerConfig.ActorRegistry required")
	case c.HarnessChain == nil:
		return errors.New("framework: ManagerConfig.HarnessChain required")
	case c.RequestLookup == nil:
		return errors.New("framework: ManagerConfig.RequestLookup required")
	}
	return nil
}

// Manager implements kernel/adapter.Manager. One Manager instance per
// channel. Modules are bound during Install and dispatched by
// audience[0] match. Safe for concurrent Dispatch / OnExternalCallback /
// RunGC.
type Manager struct {
	cfg ManagerConfig

	mu      sync.RWMutex
	modules []*boundModule
	byActor map[actor.ActorID]*boundModule
	byName  map[string]*boundModule

	readinessMu sync.Mutex

	gcCancel context.CancelFunc

	// router is the Manager-level single center for the framework sync/async
	// mechanism: caller-side futures + receiver-side owner index + the single
	// terminal-lifecycle center (§3.1 / §3.3).
	router *responseRouter
}

// boundModule is the Manager-side bookkeeping for one installed Module.
type boundModule struct {
	module      adapter.Module
	declaration adapter.Declaration
	mctx        *adapter.ModuleContext
	correlation *memoryCorrelationTracker
	policy      *timerPolicy
	state       *NamespacedStateStore

	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}
}

type initialReadinessSuppressor interface {
	SuppressInitialReadiness() bool
}

// NewManager constructs a Manager. Caller MUST call Install before
// Dispatch / OnExternalCallback.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	return &Manager{
		cfg:     cfg,
		byActor: map[actor.ActorID]*boundModule{},
		byName:  map[string]*boundModule{},
		router:  newResponseRouter(cfg.Logger),
	}, nil
}

// Install runs the L2 §8.6 install path for each module. On failure
// the Manager is left in a partial state — caller should not retry
// Install on the same instance; rebuild a fresh Manager.
func (m *Manager) Install(ctx context.Context, modules []adapter.Module) error {
	if len(modules) == 0 {
		return errors.New("framework: Install requires at least one module")
	}
	for _, mod := range modules {
		if err := m.installOne(ctx, mod); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) installOne(ctx context.Context, mod adapter.Module) error {
	decl := mod.Declares()
	if err := validateDeclaration(decl); err != nil {
		return err
	}

	m.mu.Lock()
	if _, dup := m.byName[decl.Name]; dup {
		m.mu.Unlock()
		return fmt.Errorf("framework: adapter %q already installed", decl.Name)
	}
	if _, dup := m.byActor[decl.ActorID]; dup {
		m.mu.Unlock()
		return fmt.Errorf("framework: actor %q already bound by another adapter", decl.ActorID)
	}
	m.mu.Unlock()

	// Verify actor exists + binding matches.
	rec, ok, err := m.cfg.ActorRegistry.Lookup(ctx, decl.ActorID)
	if err != nil {
		return fmt.Errorf("framework: actor lookup %s: %w", decl.ActorID, err)
	}
	if !ok {
		return fmt.Errorf("%w: actor %s", asInstallError(message.InstallHandlerActorNotRegistered), decl.ActorID)
	}
	if decl.Binding != rec.Binding {
		return fmt.Errorf("%w: actor=%s actor_binding=%s declared=%s",
			asInstallError(message.InstallHandlerActorBindingMismatch),
			decl.ActorID, rec.Binding, decl.Binding)
	}

	// Binding-specific dependency check.
	if decl.Binding == actor.BindingRuntimeInboundViaRelay && m.cfg.DeviceTransit == nil {
		return fmt.Errorf("framework: adapter %q requires DeviceTransit (binding=runtime_inbound_via_relay) but ManagerConfig.DeviceTransit is nil",
			decl.Name)
	}
	if decl.Binding == actor.BindingRuntimeOutbound && m.cfg.HTTPClient == nil {
		m.cfg.Logger.Warn("framework.install.runtime_outbound.no_client",
			"adapter", decl.Name,
			"note", "ManagerConfig.HTTPClient nil — adapter must provide its own")
	}

	// Build type rows, but do not publish them until mod.Init succeeds.
	// Otherwise an Init failure leaves a callable type_registry row with
	// no live module behind it.
	//
	// TypeDeclarations opt-in policy:
	//   - decl.TypeDeclarations == nil  → adapter has NOT opted into
	//     strict mode; every Types entry gets permissive defaults
	//     (AllowedKinds = {event, request, response}). install logs a
	//     warning so the gap is observable.
	//   - decl.TypeDeclarations != nil  → adapter has opted in; EVERY
	//     Types entry MUST have a matching declaration row. Missing rows
	//     fail-closed with InstallTypeRegistryInvalid — no silent
	//     fall-back to permissive defaults. This prevents the failure
	//     mode where a partial map silently accepts spec-disallowed
	//     kinds on the un-declared types.
	strictTypeDeclarations := decl.TypeDeclarations != nil
	typeRows := make([]TypeRow, 0, len(decl.Types)+1)
	for _, t := range decl.Types {
		td, hasDecl := decl.TypeDeclarations[t]
		if !hasDecl {
			if strictTypeDeclarations {
				return fmt.Errorf("%w: adapter=%s type=%s: TypeDeclarations declared but row missing — strict mode rejects permissive-default fallback",
					asInstallError(message.InstallTypeRegistryInvalid), decl.Name, t)
			}
			td = adapter.TypeDeclaration{
				AllowedKinds: []message.Kind{
					message.KindEvent,
					message.KindRequest,
					message.KindResponse,
				},
			}
			m.cfg.Logger.Warn("framework.install.type_decl.permissive_default",
				"adapter", decl.Name,
				"type", t,
				"note", "decl.TypeDeclarations[type] missing — using permissive defaults")
		} else {
			if err := ValidateTypeDeclaration(t, td); err != nil {
				return fmt.Errorf("framework: validate type=%s: %w", t, err)
			}
		}

		row := TypeRow{
			Type:           t,
			HandlerActorID: decl.ActorID,
			HandlerBinding: decl.Binding,
			MaxPendingMs:   decl.MaxPendingMs,
			AllowedKinds:   append([]message.Kind(nil), td.AllowedKinds...),
		}
		if row.MaxPendingMs <= 0 {
			return fmt.Errorf("%w: adapter=%s type=%s",
				asInstallError(message.InstallAdapterTimeoutMissing), decl.Name, t)
		}
		typeRows = append(typeRows, row)
	}
	typeRows = append(typeRows, TypeRow{
		Type:           orphanCallbackType(decl.Name),
		HandlerActorID: decl.ActorID,
		HandlerBinding: decl.Binding,
		MaxPendingMs:   decl.MaxPendingMs,
		AllowedKinds:   []message.Kind{message.KindEvent},
	})

	// Build framework helpers.
	state := NewNamespacedStateStore(m.cfg.StateStore, "adapter:"+decl.Name)
	credentials := NewScopedCredentialStoreForDeclaration(m.cfg.CredentialStore, decl)
	if receiver, ok := mod.(CredentialStoreReceiver); ok {
		receiver.SetCredentialStore(credentials)
	} else if declarationNeeds(decl, "credentials") {
		return fmt.Errorf("framework: adapter %q declares credentials but does not accept a framework-scoped CredentialStore", decl.Name)
	}
	corr := newCorrelationTracker(decl.Name, state)
	policy := newTimerPolicy(
		decl.Name,
		corr,
		m.cfg.Logger,
		m.cfg.Metrics,
		m.cfg.Clock,
		m.cfg.ChannelID,
		m.cfg.HarnessChain,
	)
	observe := m.router.ObserveResponse
	respondCfg := respondConfig{
		adapterName:    decl.Name,
		adapterActorID: decl.ActorID,
		channelID:      m.cfg.ChannelID,
		declaration:    decl,
		lookup:         m.cfg.RequestLookup,
		chain:          m.cfg.HarnessChain,
		correlation:    corr,
		policy:         policy,
		clock:          m.cfg.Clock,
		logger:         m.cfg.Logger,
		metrics:        m.cfg.Metrics,
		observe:        observe,
	}
	respond, err := buildRespond(respondCfg)
	if err != nil {
		return fmt.Errorf("framework: build respond for %s: %w", decl.Name, err)
	}
	provisional, err := buildProvisional(respondCfg)
	if err != nil {
		return fmt.Errorf("framework: build provisional for %s: %w", decl.Name, err)
	}
	fallback, err := buildSynthesizedTerminalFallback(respondConfig{
		adapterName:    decl.Name,
		adapterActorID: decl.ActorID,
		channelID:      m.cfg.ChannelID,
		declaration:    decl,
		lookup:         m.cfg.RequestLookup,
		chain:          m.cfg.HarnessChain,
		correlation:    corr,
		policy:         policy,
		clock:          m.cfg.Clock,
		logger:         m.cfg.Logger,
		metrics:        m.cfg.Metrics,
		observe:        observe,
	})
	if err != nil {
		return fmt.Errorf("framework: build synthesized fallback for %s: %w", decl.Name, err)
	}
	policy.bindFallback(fallback)
	// F5: when the F3 fallback exhausts its write retries and force-expires a
	// request, drop the router's receiverOwner entry so the reqID does not leak
	// (the successful-write close path runs through router.ObserveResponse).
	policy.bindOnExpire(func(rid adapter.CorrelationKey) {
		m.router.forgetReceiver(message.ID(rid))
	})

	fail := buildFail(respond)
	mctx := &adapter.ModuleContext{
		AdapterName:            decl.Name,
		AdapterActorID:         decl.ActorID,
		AdapterActorKind:       actor.KindTool,
		ChannelID:              m.cfg.ChannelID,
		Respond:                respond,
		Provisional:            provisional,
		Fail:                   fail,
		Resolve:                buildResolve(respond, fail),
		EmitEvent:              buildEmitEvent(respondCfg, decl),
		ReportOrphanCallback:   buildReportOrphanCallback(respondCfg, decl),
		ForwardExternalRequest: m.buildForwardExternalRequest(decl, corr),
		UpdateReadiness: func(ctx context.Context, update actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			return m.updateReadinessForDeclaration(ctx, decl, update, true)
		},
		LookupPendingRequest: func(ctx context.Context, requestID adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
			return corr.Get(ctx, requestID)
		},
	}
	m.injectCallerContext(mctx, decl)

	if err := mod.Init(ctx, mctx); err != nil {
		return fmt.Errorf("framework: module %s init: %w", decl.Name, err)
	}

	for _, row := range typeRows {
		if _, err := m.installTypeRow(ctx, row); err != nil {
			_ = mod.Shutdown(ctx)
			reason := installReasonFromError(err)
			return fmt.Errorf("%w: adapter=%s type=%s: %v",
				asInstallError(reason), decl.Name, row.Type, err)
		}
	}
	m.persistDeclarationCatalog(ctx, state, decl)

	bm := &boundModule{
		module:      mod,
		declaration: decl,
		mctx:        mctx,
		correlation: corr,
		policy:      policy,
		state:       state,
	}
	m.mu.Lock()
	m.modules = append(m.modules, bm)
	m.byActor[decl.ActorID] = bm
	m.byName[decl.Name] = bm
	m.mu.Unlock()

	m.cfg.Metrics.IncCounter("adapter.install.ok",
		"adapter", decl.Name, "binding", string(decl.Binding))
	m.cfg.Logger.Info("framework.install.ok",
		"adapter", decl.Name,
		"binding", string(decl.Binding),
		"types", decl.Types,
		"channel_id", string(m.cfg.ChannelID))
	m.startReadiness(ctx, bm)
	return nil
}

func (m *Manager) persistDeclarationCatalog(ctx context.Context, state StateStore, decl adapter.Declaration) {
	if state == nil {
		return
	}
	raw, err := json.Marshal(adapter.DeclarationCatalogFromDeclaration(decl))
	if err != nil {
		m.cfg.Logger.Warn("framework.install.declaration_catalog.marshal",
			"adapter", decl.Name,
			"err", err.Error())
		return
	}
	if err := state.Put(ctx, adapter.DeclarationConventionStateKey, raw); err != nil {
		m.cfg.Logger.Warn("framework.install.declaration_catalog.persist",
			"adapter", decl.Name,
			"err", err.Error())
	}
}

func (m *Manager) startReadiness(ctx context.Context, bm *boundModule) {
	if bm == nil {
		return
	}
	if suppressor, ok := bm.module.(initialReadinessSuppressor); ok && suppressor.SuppressInitialReadiness() {
		return
	}
	if _, ok := bm.module.(adapter.Heartbeater); !ok {
		m.applyReadiness(ctx, bm, actorreg.ReadinessUpdate{
			State:     actorreg.ReadinessReady,
			Reason:    "ok",
			Detail:    readinessDetail(map[string]any{"binding": string(bm.declaration.Binding)}),
			CheckedAt: m.cfg.Clock().UnixMilli(),
		}, m.cfg.EmitInitialReadinessEvent)
		return
	}

	m.runHeartbeatOnce(ctx, bm, m.cfg.EmitInitialReadinessEvent)

	hbCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	bm.heartbeatCancel = cancel
	bm.heartbeatDone = done
	interval := m.heartbeatInterval(bm.declaration.Binding)
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				m.runHeartbeatOnce(hbCtx, bm, true)
			}
		}
	}()
}

func (m *Manager) heartbeatInterval(binding actor.Binding) time.Duration {
	if m.cfg.HeartbeatInterval > 0 {
		return m.cfg.HeartbeatInterval
	}
	switch binding {
	case actor.BindingEmbedded:
		return 60 * time.Second
	case actor.BindingRuntimeOutbound:
		return 15 * time.Second
	case actor.BindingRuntimeInboundViaRelay:
		return 30 * time.Second
	default:
		return 30 * time.Second
	}
}

func (m *Manager) runHeartbeatOnce(ctx context.Context, bm *boundModule, emitEvent bool) {
	hb, ok := bm.module.(adapter.Heartbeater)
	if !ok {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, m.cfg.HeartbeatTimeout)
	report, err := hb.Heartbeat(probeCtx)
	cancel()
	now := m.cfg.Clock()
	if err != nil {
		report = adapter.HeartbeatReport{
			Available: false,
			Reason:    "unknown",
			Detail:    map[string]any{"error": err.Error()},
			CheckedAt: now,
		}
	}
	if report.CheckedAt.IsZero() {
		report.CheckedAt = now
	}
	state := actorreg.ReadinessNotReady
	if report.Available {
		state = actorreg.ReadinessReady
	}
	reason := report.Reason
	if reason == "" {
		if report.Available {
			reason = "ok"
		} else {
			reason = "unknown"
		}
	}
	m.applyReadiness(ctx, bm, actorreg.ReadinessUpdate{
		State:     state,
		Reason:    reason,
		Detail:    readinessDetail(report.Detail),
		CheckedAt: report.CheckedAt.UnixMilli(),
	}, emitEvent)
}

func (m *Manager) applyReadiness(ctx context.Context, bm *boundModule, update actorreg.ReadinessUpdate, emitEvent bool) {
	_, err := m.updateReadinessForDeclaration(ctx, bm.declaration, update, emitEvent)
	if err != nil {
		m.cfg.Logger.Warn("framework.readiness.update_failed",
			"adapter", bm.declaration.Name,
			"actor_id", string(bm.declaration.ActorID),
			"err", err.Error())
	}
}

func (m *Manager) updateReadinessForDeclaration(
	ctx context.Context,
	decl adapter.Declaration,
	update actorreg.ReadinessUpdate,
	emitEvent bool,
) (actorreg.ReadinessTransition, error) {
	updater, ok := m.cfg.ActorRegistry.(actorreg.ReadinessUpdater)
	if !ok {
		return actorreg.ReadinessTransition{}, fmt.Errorf("framework: readiness updater missing")
	}
	m.readinessMu.Lock()
	defer m.readinessMu.Unlock()
	tr, err := m.computeReadinessTransition(ctx, decl.ActorID, update)
	if err != nil {
		return actorreg.ReadinessTransition{}, err
	}
	if emitEvent && tr.Changed {
		if err := m.emitReadinessChangedForDeclaration(ctx, decl, tr, update.CheckedAt); err != nil {
			return tr, err
		}
	}
	tr, err = updater.UpdateReadiness(ctx, decl.ActorID, update)
	if err != nil {
		return actorreg.ReadinessTransition{}, err
	}
	return tr, nil
}

func (m *Manager) computeReadinessTransition(
	ctx context.Context,
	actorID actor.ActorID,
	update actorreg.ReadinessUpdate,
) (actorreg.ReadinessTransition, error) {
	rec, ok, err := m.cfg.ActorRegistry.Lookup(ctx, actorID)
	if err != nil {
		return actorreg.ReadinessTransition{}, err
	}
	if !ok {
		return actorreg.ReadinessTransition{}, fmt.Errorf("framework: readiness actor %s not found", actorID)
	}
	prev := rec.Readiness.Normalize()
	next := actorreg.Readiness{
		State:  update.State,
		Reason: update.Reason,
		Detail: append(json.RawMessage(nil), update.Detail...),
	}.Normalize()
	switch next.State {
	case actorreg.ReadinessReady, actorreg.ReadinessNotReady, actorreg.ReadinessUnknown:
	default:
		return actorreg.ReadinessTransition{}, fmt.Errorf("framework: readiness update %s invalid state %q", actorID, next.State)
	}
	if !json.Valid(next.Detail) {
		return actorreg.ReadinessTransition{}, fmt.Errorf("framework: readiness update %s invalid detail JSON", actorID)
	}
	stateReasonChanged := prev.State != next.State || prev.Reason != next.Reason
	next.LastReadyAt = prev.LastReadyAt
	if next.State == actorreg.ReadinessReady {
		next.LastReadyAt = update.CheckedAt
	}
	next.LastStateChangeAt = prev.LastStateChangeAt
	if stateReasonChanged {
		next.LastStateChangeAt = update.CheckedAt
	}
	changed := readinessProjectionChanged(prev, next)
	return actorreg.ReadinessTransition{Previous: prev, Current: next, Changed: changed}, nil
}

func readinessProjectionChanged(prev, next actorreg.Readiness) bool {
	prev = prev.Normalize()
	next = next.Normalize()
	return prev.State != next.State ||
		prev.Reason != next.Reason ||
		string(prev.Detail) != string(next.Detail) ||
		prev.LastReadyAt != next.LastReadyAt ||
		prev.LastStateChangeAt != next.LastStateChangeAt
}

func (m *Manager) emitReadinessChangedForDeclaration(ctx context.Context, decl adapter.Declaration, tr actorreg.ReadinessTransition, checkedAt int64) error {
	changedAt := checkedAt
	if changedAt == 0 {
		changedAt = tr.Current.LastStateChangeAt
	}
	if changedAt == 0 {
		changedAt = tr.Current.LastReadyAt
	}
	if changedAt == 0 {
		changedAt = m.cfg.Clock().UnixMilli()
	}
	_, err := writeEvent(ctx, m.cfg.HarnessChain, eventEnvelope{
		Type:       "actor.readiness.changed",
		ChannelID:  m.cfg.ChannelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Now:        changedAt,
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
		Payload: map[string]any{
			"actor_id":   string(decl.ActorID),
			"previous":   readinessPayload(tr.Previous),
			"current":    readinessPayload(tr.Current),
			"changed_at": changedAt,
		},
	})
	return err
}

func readinessDetail(detail map[string]any) json.RawMessage {
	if len(detail) == 0 {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return json.RawMessage(`{"error":"detail_marshal_failed"}`)
	}
	return raw
}

func readinessPayload(r actorreg.Readiness) map[string]any {
	r = r.Normalize()
	return map[string]any{
		"ready":                r.State == actorreg.ReadinessReady,
		"state":                string(r.State),
		"reason":               r.Reason,
		"detail":               rawJSONObject(r.Detail),
		"last_ready_at":        r.LastReadyAt,
		"last_state_change_at": r.LastStateChangeAt,
	}
}

func rawJSONObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func (m *Manager) installTypeRow(ctx context.Context, row TypeRow) (TypeRow, error) {
	if m.cfg.TypeInstaller != nil {
		return m.cfg.TypeInstaller.InstallType(ctx, row)
	}
	return m.cfg.TypeRegistry.Upsert(ctx, row)
}

type externalRequestTransitBody struct {
	Direction     string                         `json:"direction"`
	RequestID     message.ID                     `json:"request_id"`
	ParentID      message.ID                     `json:"parent_id"`
	CorrelationID message.ID                     `json:"correlation_id"`
	ExpiresAt     int64                          `json:"expires_at"`
	Payload       adapter.ExternalRequestPayload `json:"payload"`
}

func (m *Manager) buildForwardExternalRequest(decl adapter.Declaration, corr *memoryCorrelationTracker) adapter.ExternalRequestFunc {
	return func(ctx context.Context, env *message.Envelope, payload adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
		if env == nil {
			return adapter.ExternalRequestResult{}, errors.New("framework: ForwardExternalRequest envelope nil")
		}
		if m.cfg.DeviceTransit == nil {
			return adapter.ExternalRequestResult{}, errors.New("framework: ForwardExternalRequest DeviceTransit not wired")
		}
		if env.Kind != message.KindRequest {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest envelope kind=%s (must be request)", env.Kind)
		}
		if env.ID == "" {
			return adapter.ExternalRequestResult{}, errors.New("framework: ForwardExternalRequest envelope id required")
		}
		if env.ChannelID != m.cfg.ChannelID {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest channel mismatch: envelope=%s manager=%s",
				env.ChannelID, m.cfg.ChannelID)
		}
		if len(env.Audience) == 0 || env.Audience[0] != decl.ActorID {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest audience %v does not target %s",
				env.Audience, decl.ActorID)
		}
		if !declAllowsKind(decl, env.Type, message.KindRequest) {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest type %s is not request-capable for adapter %s",
				env.Type, decl.Name)
		}
		found, ok, err := m.cfg.RequestLookup.FindByID(ctx, env.ID)
		if err != nil {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest lookup %s: %w", env.ID, err)
		}
		if !ok || found == nil {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest request %s not found", env.ID)
		}
		if found.Kind != message.KindRequest || found.ChannelID != env.ChannelID || found.Type != env.Type {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest lookup mismatch for %s", env.ID)
		}
		if len(found.Audience) == 0 || found.Audience[0] != decl.ActorID {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest lookup audience %v does not target %s",
				found.Audience, decl.ActorID)
		}
		if len(env.Audience) != len(found.Audience) {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest audience mismatch: envelope=%v lookup=%v",
				env.Audience, found.Audience)
		}
		for i := range env.Audience {
			if env.Audience[i] != found.Audience[i] {
				return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest audience mismatch: envelope=%v lookup=%v",
					env.Audience, found.Audience)
			}
		}
		if env.ExpiresAt == nil {
			return adapter.ExternalRequestResult{}, errors.New("framework: ForwardExternalRequest envelope expires_at required")
		}
		if found.ExpiresAt == nil || *found.ExpiresAt != *env.ExpiresAt {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest expires_at mismatch: envelope=%d lookup=%v",
				*env.ExpiresAt, found.ExpiresAt)
		}
		entry, ok, err := corr.Get(ctx, adapter.CorrelationKey(env.ID))
		if err != nil {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest correlation lookup %s: %w", env.ID, err)
		}
		if !ok {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest no pending correlation for %s", env.ID)
		}
		if entry.State != adapter.CorrelationPending {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest correlation %s state=%s (must be pending)",
				env.ID, entry.State)
		}
		if entry.ChannelID != m.cfg.ChannelID || entry.AudienceActor != decl.ActorID || entry.ParentID != env.ID {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest correlation owner mismatch for %s", env.ID)
		}
		if entry.ExpiresAt != *env.ExpiresAt {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest correlation expires_at mismatch: entry=%d envelope=%d",
				entry.ExpiresAt, *env.ExpiresAt)
		}
		correlationID := found.CorrelationID
		if correlationID == "" {
			correlationID = found.ID
		}
		body, err := json.Marshal(externalRequestTransitBody{
			Direction:     "to_device",
			RequestID:     found.ID,
			ParentID:      found.ParentID,
			CorrelationID: correlationID,
			ExpiresAt:     *found.ExpiresAt,
			Payload:       append(adapter.ExternalRequestPayload(nil), payload...),
		})
		if err != nil {
			return adapter.ExternalRequestResult{}, fmt.Errorf("framework: ForwardExternalRequest marshal transit body: %w", err)
		}
		frameID, err := m.cfg.DeviceTransit.Send(ctx, devicetransit.SendFrame{
			AdapterActorID: decl.ActorID,
			ChannelID:      m.cfg.ChannelID,
			Body:           body,
		})
		if err != nil {
			return adapter.ExternalRequestResult{}, err
		}
		return adapter.ExternalRequestResult{FrameID: frameID.String()}, nil
	}
}

type installReasoner interface {
	InstallReason() message.InstallReason
}

func installReasonFromError(err error) message.InstallReason {
	var ie *InstallError
	if errors.As(err, &ie) && ie.Reason != "" {
		return ie.Reason
	}
	var r installReasoner
	if errors.As(err, &r) && r.InstallReason() != "" {
		return r.InstallReason()
	}
	return message.InstallTypeRegistryInvalid
}

// BootRecoverTimers re-arms every pending request's F3 timer per L2
// §8.6 step 3. Called once after Install.
func (m *Manager) BootRecoverTimers(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, bm := range m.modules {
		if err := m.recoverTimersForBoundModule(ctx, bm); err != nil {
			return err
		}
	}
	return nil
}

// RecoverTimersForActor rehydrates and re-arms pending requests for one
// dynamically installed actor. Composition roots call this immediately
// after proxy facade Install, before exposing the actor as callable.
func (m *Manager) RecoverTimersForActor(ctx context.Context, actorID actor.ActorID) error {
	m.mu.RLock()
	bm := m.byActor[actorID]
	m.mu.RUnlock()
	if bm == nil {
		return fmt.Errorf("framework: recover actor %s: not installed", actorID)
	}
	return m.recoverTimersForBoundModule(ctx, bm)
}

// DeclarationForActor returns the installed declaration for an actor.
// Composition roots use it to resume dynamic proxy facade wiring after a
// partial install without calling Install a second time.
func (m *Manager) DeclarationForActor(actorID actor.ActorID) (adapter.Declaration, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bm := m.byActor[actorID]
	if bm == nil {
		return adapter.Declaration{}, false
	}
	return bm.declaration, true
}

func (m *Manager) recoverTimersForBoundModule(ctx context.Context, bm *boundModule) error {
	if err := bm.correlation.recoverFromStore(ctx); err != nil {
		return fmt.Errorf("framework: recover %s: %w", bm.declaration.Name, err)
	}
	pending, err := bm.correlation.ListPending(ctx)
	if err != nil {
		return fmt.Errorf("framework: list pending %s: %w", bm.declaration.Name, err)
	}
	for _, e := range pending {
		// temporal R1: recover against the LIVE deadline. RearmedExpiresAt holds
		// the latest provisional-heartbeat-extended deadline (0 = never re-armed);
		// when set it supersedes the stale original ExpiresAt so a still-alive
		// long-running receiver is not force-failed at 1µs after restart. A
		// request that never heart-beat (RearmedExpiresAt==0) recovers against its
		// original ExpiresAt and still F3-fails promptly if it elapsed during
		// downtime.
		deadlineMs := e.ExpiresAt
		if e.RearmedExpiresAt > deadlineMs {
			deadlineMs = e.RearmedExpiresAt
		}
		deadline := time.UnixMilli(deadlineMs)
		// F4: rebuild the router receiver-owner index BEFORE re-arming the F3
		// timer. A recovered entry whose deadline is already past makes
		// RecoverTimer fire (near-)immediately; the fallback final must find the
		// receiverOwner to close lifecycle through the single center (§3.1) — so
		// owner-first, timer-second, matching reservePendingRequest.
		m.router.trackReceiver(message.ID(e.RequestID), bm)
		// Seed the ScheduleToClose anchor from EnqueuedAt (original creation), not
		// recovery time, so the hard ceiling stays anchored at creation across
		// restarts and heartbeats after recovery can't escape it.
		if err := bm.policy.RecoverTimer(ctx, e.RequestID, deadline, e.EnqueuedAt); err != nil {
			m.cfg.Logger.Warn("framework.recover.register_timer",
				"adapter", bm.declaration.Name,
				"request_id", e.RequestID,
				"err", err.Error())
		}
	}
	return nil
}

// Dispatch routes a request envelope to the correct module.
func (m *Manager) Dispatch(ctx context.Context, env *message.Envelope) error {
	if env == nil {
		return errors.New("framework: Dispatch envelope nil")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("framework: Dispatch envelope kind=%s (must be request)", env.Kind)
	}
	if env.ChannelID != m.cfg.ChannelID {
		return fmt.Errorf("framework: Dispatch channel mismatch envelope=%s manager=%s",
			env.ChannelID, m.cfg.ChannelID)
	}
	if len(env.Audience) == 0 {
		return errors.New("framework: Dispatch envelope audience empty")
	}
	target := env.Audience[0]
	m.mu.RLock()
	bm, ok := m.byActor[target]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("framework: Dispatch no module bound to actor %s", target)
	}

	if env.Type == "actor.status" {
		if err := m.reservePendingRequest(ctx, bm, env); err != nil {
			return err
		}
		return m.respondActorStatus(ctx, bm, env)
	}
	if env.Type == "actor.describe" {
		if err := m.reservePendingRequest(ctx, bm, env); err != nil {
			return err
		}
		return m.respondActorDescribe(ctx, bm, env)
	}

	// Verify the type is one the adapter declared.
	if !declAllowsKind(bm.declaration, env.Type, message.KindRequest) {
		return fmt.Errorf("framework: Dispatch type %s is not request-capable for adapter %s",
			env.Type, bm.declaration.Name)
	}

	if err := m.reservePendingRequest(ctx, bm, env); err != nil {
		return err
	}

	if unavailable, err := m.respondIfNotReady(ctx, bm, env); err != nil {
		return err
	} else if unavailable {
		return nil
	}

	span := m.cfg.Tracer.StartSpan("adapter.dispatch",
		"adapter", bm.declaration.Name, "type", env.Type)
	defer span.End()

	m.cfg.Metrics.IncCounter("adapter.dispatch",
		"adapter", bm.declaration.Name, "type", env.Type)

	if err := m.runHandle(ctx, bm, env); err != nil {
		// Deferred: the adapter will produce the terminal later via Resolve.
		// Keep pending + F3 alive; do NOT finalize. Must be the exact sentinel
		// (no %w-wrapped business error per §2.1 hardening).
		if errors.Is(err, adapter.ErrHandleDeferred) {
			m.cfg.Metrics.IncCounter("adapter.dispatch.handle_deferred",
				"adapter", bm.declaration.Name, "type", env.Type)
			return nil
		}
		m.cfg.Logger.Warn("framework.dispatch.handle.error",
			"adapter", bm.declaration.Name,
			"type", env.Type,
			"request_id", env.ID,
			"err", err.Error())
		m.cfg.Metrics.IncCounter("adapter.dispatch.handle_error",
			"adapter", bm.declaration.Name, "type", env.Type)
		if isTerminalWriteFailure(err) {
			return err
		}
		if entry, ok, getErr := bm.correlation.Get(ctx, adapter.CorrelationKey(env.ID)); getErr != nil {
			return fmt.Errorf("framework: dispatch handle error correlation lookup failed: %w (handle error: %v)", getErr, err)
		} else if ok && entry.State != adapter.CorrelationPending {
			return fmt.Errorf("framework: dispatch handle error after terminal response: %w", err)
		}
		if perr := bm.policy.OnExternalError(ctx,
			adapter.CorrelationKey(env.ID),
			message.TerminalReceiverInternalError,
			err.Error(),
		); perr != nil {
			return fmt.Errorf("framework: dispatch handle error terminal failed: %w (handle error: %v)", perr, err)
		}
		return fmt.Errorf("framework: dispatch handle error terminal emitted: %w", err)
	}
	return nil
}

func (m *Manager) reservePendingRequest(ctx context.Context, bm *boundModule, env *message.Envelope) error {
	now := m.cfg.Clock()
	deadline := now.Add(time.Duration(bm.declaration.MaxPendingMs) * time.Millisecond)
	deadlineMs := deadline.UnixMilli()
	if env.ExpiresAt != nil && *env.ExpiresAt > 0 {
		deadlineMs = *env.ExpiresAt
		deadline = time.UnixMilli(deadlineMs)
	} else {
		exp := deadlineMs
		env.ExpiresAt = &exp
	}
	entry := adapter.CorrelationEntry{
		RequestID:     adapter.CorrelationKey(env.ID),
		CorrelationID: env.CorrelationID,
		ChannelID:     env.ChannelID,
		AudienceActor: bm.declaration.ActorID,
		ParentID:      env.ID,
		EnqueuedAt:    now.UnixMilli(),
		ExpiresAt:     deadlineMs,
		State:         adapter.CorrelationPending,
	}
	if _, err := bm.correlation.Reserve(ctx, entry); err != nil {
		return fmt.Errorf("framework: dispatch reserve: %w", err)
	}
	// F4: record the receiver-owner BEFORE arming the F3 timer. With a very
	// short deadline the timer can fire (or fire-immediately) before
	// RegisterTimer returns, and the F3 fallback writes a final that the
	// router observes — if the receiverOwner is not yet recorded the router
	// cannot locate this module's correlation to close it, leaking corr + F3.
	// Same point/same lock as correlation reserve (§3.1).
	m.router.trackReceiver(env.ID, bm)
	if err := bm.policy.RegisterTimer(ctx, adapter.CorrelationKey(env.ID), deadline); err != nil {
		m.router.forgetReceiver(env.ID)
		return fmt.Errorf("framework: dispatch register timer: %w", err)
	}
	return nil
}

func (m *Manager) respondActorStatus(ctx context.Context, bm *boundModule, env *message.Envelope) error {
	rec, ok, err := m.cfg.ActorRegistry.Lookup(ctx, bm.declaration.ActorID)
	if err != nil {
		return fmt.Errorf("framework: actor.status lookup %s: %w", bm.declaration.ActorID, err)
	}
	if !ok {
		rec = actorreg.Record{
			ID:      bm.declaration.ActorID,
			Kind:    actor.KindTool,
			Binding: bm.declaration.Binding,
		}
	}
	readiness := rec.Readiness.Normalize()
	detail := rawJSONObject(readiness.Detail)
	checkedAt := m.cfg.Clock().UnixMilli()

	// channel-lifecycle-reconcile §5 护栏 3 — actor.status.available is
	// realtime liveness/probe, NEVER the persisted readiness projection. The
	// registry readiness (ready_state/last_ready_at/…) is only baseline
	// detail/diagnostics, not the answer to "is this actor callable right
	// now". The realtime answer comes from the module's StatusReporter
	// (connection liveness / probe). A module with no StatusReporter is an
	// in-process embedded module: it is live exactly because it is installed
	// and dispatchable here, so it reports available=true.
	available := true
	reason := readiness.Reason
	if reporter, ok := bm.module.(adapter.StatusReporter); ok {
		statusCtx, cancel := context.WithTimeout(ctx, m.cfg.StatusTimeout)
		report, err := reporter.Status(statusCtx)
		cancel()
		if err != nil {
			// Probe failed under deadline — treat as not-callable rather than
			// falling back to the sticky readiness column (which the spec
			// forbids as the available authority).
			available = false
			reason = "status_probe_error"
			m.cfg.Logger.Warn("framework.actor_status.reporter_error",
				"adapter", bm.declaration.Name,
				"actor_id", string(bm.declaration.ActorID),
				"err", err.Error())
		} else {
			available = report.Available
			if report.Reason != "" {
				reason = report.Reason
			}
			for k, v := range report.Detail {
				detail[k] = v
			}
			if !report.CheckedAt.IsZero() {
				checkedAt = report.CheckedAt.UnixMilli()
			}
		}
	}

	payload, err := json.Marshal(map[string]any{
		"available":            available,
		"reason":               reason,
		"kind":                 string(rec.Kind),
		"binding":              string(rec.Binding),
		"last_ready_at":        readiness.LastReadyAt,
		"last_state_change_at": readiness.LastStateChangeAt,
		"detail":               detail,
		"checked_at":           checkedAt,
	})
	if err != nil {
		return fmt.Errorf("framework: actor.status marshal: %w", err)
	}
	_, err = m.respondReservedWithAdapterActor(ctx, bm, env, payload, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

// respondActorDescribe answers the actor.describe reserved type with the
// adapter's static Declaration projection. The data is sourced directly
// from Module.Declares() — daemon is the single source of truth, server
// is just transport. No server-side mirror, no event emission.
//
// Optional payload `{"type": "<name>"}` filters to a single type's full
// metadata; omitted payload returns the full actor projection.
func (m *Manager) respondActorDescribe(ctx context.Context, bm *boundModule, env *message.Envelope) error {
	decl := bm.declaration
	catalog := adapter.DeclarationCatalogFromDeclaration(decl)

	var filter struct {
		Type string `json:"type,omitempty"`
	}
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &filter)
	}

	if filter.Type != "" {
		typeDoc, ok := catalog.Types[filter.Type]
		typeDecl, declOK := decl.TypeDeclarations[filter.Type]
		if !ok && !declOK {
			payload, _ := json.Marshal(map[string]any{
				"status":     "failed",
				"reason":     string(message.TerminalReceiverInternalError),
				"error_code": "unknown_type",
				"detail":     fmt.Sprintf("actor %s does not declare type %q", decl.ActorID, filter.Type),
			})
			_, err := m.respondReservedWithAdapterActor(ctx, bm, env, payload, adapter.RespondOptions{
				Status: "failed",
				Reason: string(message.TerminalReceiverInternalError),
			})
			return err
		}
		allowedKinds := make([]string, 0, len(typeDecl.AllowedKinds))
		for _, k := range typeDecl.AllowedKinds {
			allowedKinds = append(allowedKinds, string(k))
		}
		body, err := json.Marshal(map[string]any{
			"actor_id":        string(decl.ActorID),
			"type":            filter.Type,
			"description":     typeDoc.Description,
			"payload_example": typeDoc.PayloadExample,
			"payload_fields":  typeDoc.PayloadFields,
			"error_codes":     typeDoc.ErrorCodes,
			"notes":           typeDoc.Notes,
			"allowed_kinds":   allowedKinds,
			"max_pending_ms":  decl.MaxPendingMs,
			"handler_binding": string(decl.Binding),
		})
		if err != nil {
			return fmt.Errorf("framework: actor.describe type marshal: %w", err)
		}
		_, err = m.respondReservedWithAdapterActor(ctx, bm, env, body, adapter.RespondOptions{
			Status: "completed",
		})
		return err
	}

	body, err := json.Marshal(map[string]any{
		"actor_id":    string(decl.ActorID),
		"name":        decl.Name,
		"binding":     string(decl.Binding),
		"description": catalog.Description,
		"skill_doc":   catalog.SkillDoc,
		"types":       catalog.Types,
	})
	if err != nil {
		return fmt.Errorf("framework: actor.describe marshal: %w", err)
	}
	_, err = m.respondReservedWithAdapterActor(ctx, bm, env, body, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

func (m *Manager) respondReservedWithAdapterActor(
	ctx context.Context,
	bm *boundModule,
	env *message.Envelope,
	payload json.RawMessage,
	opts adapter.RespondOptions,
) (adapter.RespondResult, error) {
	return runRespondWithSender(ctx, respondConfig{
		adapterName:    bm.declaration.Name,
		adapterActorID: bm.declaration.ActorID,
		channelID:      m.cfg.ChannelID,
		declaration:    bm.declaration,
		lookup:         m.cfg.RequestLookup,
		chain:          m.cfg.HarnessChain,
		correlation:    bm.correlation,
		policy:         bm.policy,
		clock:          m.cfg.Clock,
		logger:         m.cfg.Logger,
		metrics:        m.cfg.Metrics,
	}, adapter.CorrelationKey(env.ID), payload, opts, message.Sender{
		Kind: actor.KindTool,
		ID:   bm.declaration.ActorID,
	}, terminalValidationMode{
		allowReservedActorTypes: true,
	})
}

func (m *Manager) respondIfNotReady(ctx context.Context, bm *boundModule, env *message.Envelope) (bool, error) {
	rec, ok, err := m.cfg.ActorRegistry.Lookup(ctx, bm.declaration.ActorID)
	if err != nil {
		return false, fmt.Errorf("framework: readiness pre-check lookup %s: %w", bm.declaration.ActorID, err)
	}
	if !ok {
		return false, nil
	}
	readiness := rec.Readiness.Normalize()
	if readiness.State != actorreg.ReadinessNotReady {
		return false, nil
	}
	detail := fmt.Sprintf("actor not ready (last state change at %d): %s",
		readiness.LastStateChangeAt, string(readiness.Detail))
	payload, err := json.Marshal(map[string]any{
		"error_code":    readiness.Reason,
		"detail":        detail,
		"recovery_hint": recoveryHintForReadiness(bm.declaration.Name, readiness.Reason),
	})
	if err != nil {
		return false, fmt.Errorf("framework: readiness pre-check marshal: %w", err)
	}
	_, err = bm.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), payload, adapter.RespondOptions{
		Status: "failed",
		Reason: string(message.TerminalReceiverUnavailable),
	})
	if err != nil {
		return false, err
	}
	m.cfg.Metrics.IncCounter("adapter.dispatch.precheck_unavailable",
		"adapter", bm.declaration.Name, "reason", readiness.Reason)
	return true, nil
}

func recoveryHintForReadiness(adapterName, reason string) string {
	switch reason {
	case "device_offline":
		return "Call list_actors to see current readiness; wait for xhs.device.online or actor.readiness.changed before retrying"
	case "token_expired", "device_token_expired":
		return "Re-bind the device, then wait for actor.readiness.changed before retrying"
	case "extension_disconnected":
		return "Reload the browser extension or page, then call list_actors before retrying"
	case "daemon_unreachable", "upstream_unreachable":
		return "Start the upstream daemon, then wait for actor.readiness.changed before retrying"
	default:
		return fmt.Sprintf("Call list_actors to see current readiness for %s before retrying", adapterName)
	}
}

// runHandle wraps Module.Handle with a panic recover that emits a
// failed terminal through Dispatch's common handle-error path per L2 §8
// F3 panic safety. The terminal carries reason=receiver_internal_error
// + detail containing the panic message and stack trace.
func (m *Manager) runHandle(ctx context.Context, bm *boundModule, env *message.Envelope) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		stack := string(debug.Stack())
		detail := fmt.Sprintf("adapter %s panic: %v\n%s", bm.declaration.Name, r, stack)
		m.cfg.Logger.Error("framework.dispatch.handle.panic",
			"adapter", bm.declaration.Name,
			"type", env.Type,
			"request_id", env.ID,
			"panic", fmt.Sprint(r))
		m.cfg.Metrics.IncCounter("adapter.dispatch.handle_panic",
			"adapter", bm.declaration.Name, "type", env.Type)
		err = fmt.Errorf("%s", detail)
	}()
	return bm.module.Handle(ctx, env)
}

// OnExternalCallback routes an inbound external callback to the named
// module. The framework de-dupes via correlation state when the
// adapter has already moved the request to a terminal state.
func (m *Manager) OnExternalCallback(ctx context.Context, adapterName string, payload []byte) error {
	m.mu.RLock()
	bm, ok := m.byName[adapterName]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("framework: OnExternalCallback unknown adapter %q", adapterName)
	}
	span := m.cfg.Tracer.StartSpan("adapter.callback", "adapter", adapterName)
	defer span.End()
	m.cfg.Metrics.IncCounter("adapter.callback", "adapter", adapterName)
	correlationID := callbackCorrelationID(payload)
	if correlationID != "" {
		if _, ok, err := bm.correlation.Get(ctx, adapter.CorrelationKey(message.ID(correlationID))); err != nil {
			return fmt.Errorf("framework: callback correlation lookup %s: %w", correlationID, err)
		} else if !ok {
			m.cfg.Metrics.IncCounter("adapter.callback.orphan", "adapter", adapterName)
			ev := orphanCallbackEvent{
				AdapterName:    adapterName,
				AdapterActorID: bm.declaration.ActorID,
				ChannelID:      m.cfg.ChannelID,
				Chain:          m.cfg.HarnessChain,
				Clock:          m.cfg.Clock,
				CorrelationID:  correlationID,
				Detail:         "correlation lookup miss",
				Payload:        payload,
			}
			if err := emitOrphanCallbackEvents(ctx, ev); err != nil {
				return err
			}
			return emitCorrelationLostSystemEvent(ctx, m.cfg.HarnessChain, ev)
		}
	}
	return m.runExternalCallback(ctx, bm, adapterName, payload, correlationID)
}

// OnExternalCallbackFrame routes an inbound callback while preserving the
// framework-owned transport identity that wrapped the adapter-domain payload.
func (m *Manager) OnExternalCallbackFrame(ctx context.Context, adapterName string, frame adapter.ExternalCallbackFrame) error {
	m.mu.RLock()
	bm, ok := m.byName[adapterName]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("framework: OnExternalCallbackFrame unknown adapter %q", adapterName)
	}
	if frame.ChannelID == "" {
		return errors.New("framework: OnExternalCallbackFrame channel_id required")
	}
	if frame.ChannelID != m.cfg.ChannelID {
		return fmt.Errorf("framework: OnExternalCallbackFrame channel mismatch: frame=%s manager=%s",
			frame.ChannelID, m.cfg.ChannelID)
	}
	if frame.AdapterActorID != "" && frame.AdapterActorID != bm.declaration.ActorID {
		return fmt.Errorf("framework: OnExternalCallbackFrame actor mismatch: frame=%s adapter=%s",
			frame.AdapterActorID, bm.declaration.ActorID)
	}
	if len(frame.Payload) == 0 {
		return errors.New("framework: OnExternalCallbackFrame payload required")
	}
	payload, isReadiness, err := normalizeExternalCallbackPayload(frame)
	if err != nil {
		return err
	}
	frame.Payload = payload

	span := m.cfg.Tracer.StartSpan("adapter.callback", "adapter", adapterName)
	defer span.End()
	m.cfg.Metrics.IncCounter("adapter.callback", "adapter", adapterName)

	if isReadiness {
		return m.runExternalCallbackFrame(ctx, bm, adapterName, frame, "")
	}
	requestID := frame.RequestID
	if requestID == "" {
		err := errors.New("framework: OnExternalCallbackFrame request_id required")
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "request_id_required", "", err)
	}
	payloadCorrelationID := callbackCorrelationID(payload)
	if payloadCorrelationID != "" && payloadCorrelationID != requestID.String() {
		err := fmt.Errorf("framework: OnExternalCallbackFrame payload correlation %s does not match outer request_id %s",
			payloadCorrelationID, requestID)
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "correlation_mismatch", "", err)
	}
	entry, ok, err := bm.correlation.Get(ctx, adapter.CorrelationKey(requestID))
	if err != nil {
		return fmt.Errorf("framework: callback correlation lookup %s: %w", requestID, err)
	}
	if !ok {
		correlationID := requestID.String()
		m.cfg.Metrics.IncCounter("adapter.callback.orphan", "adapter", adapterName)
		ev := orphanCallbackEvent{
			AdapterName:    adapterName,
			AdapterActorID: bm.declaration.ActorID,
			ChannelID:      m.cfg.ChannelID,
			Chain:          m.cfg.HarnessChain,
			Clock:          m.cfg.Clock,
			CorrelationID:  correlationID,
			Detail:         "correlation lookup miss",
			Payload:        payload,
		}
		if err := emitOrphanCallbackEvents(ctx, ev); err != nil {
			return err
		}
		return emitCorrelationLostSystemEvent(ctx, m.cfg.HarnessChain, ev)
	}
	if entry.ChannelID != m.cfg.ChannelID || entry.AudienceActor != bm.declaration.ActorID || entry.ParentID != requestID {
		err := fmt.Errorf("framework: OnExternalCallbackFrame correlation owner mismatch for %s", requestID)
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "correlation_owner_mismatch", "", err)
	}
	if entry.State != adapter.CorrelationPending {
		err := fmt.Errorf("framework: OnExternalCallbackFrame correlation %s state=%s (must be pending)",
			requestID, entry.State)
		switch entry.State {
		case adapter.CorrelationDone:
			return devicetransit.NewAckError(devicetransit.AckAccepted, "duplicate_terminal", "", err)
		case adapter.CorrelationExpired:
			return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "callback_expired", "", err)
		default:
			return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "callback_closed", "", err)
		}
	}
	expectedCorrelationID := entry.CorrelationID
	if expectedCorrelationID == "" {
		expectedCorrelationID = entry.ParentID
	}
	if frame.CorrelationID == "" {
		err := errors.New("framework: OnExternalCallbackFrame correlation_id required")
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "correlation_id_required", "", err)
	}
	if frame.CorrelationID != expectedCorrelationID {
		err := fmt.Errorf("framework: OnExternalCallbackFrame correlation_id mismatch: frame=%s expected=%s",
			frame.CorrelationID, expectedCorrelationID)
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "correlation_id_mismatch", "", err)
	}
	if frame.ExpiresAt == 0 {
		err := errors.New("framework: OnExternalCallbackFrame expires_at required")
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "expires_at_required", "", err)
	}
	if entry.ExpiresAt != frame.ExpiresAt {
		err := fmt.Errorf("framework: OnExternalCallbackFrame expires_at mismatch: frame=%d entry=%d",
			frame.ExpiresAt, entry.ExpiresAt)
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "expires_at_mismatch", "", err)
	}
	if m.cfg.Clock().UnixMilli() > frame.ExpiresAt {
		m.cfg.Metrics.IncCounter("adapter.callback.expired", "adapter", adapterName)
		ev := orphanCallbackEvent{
			AdapterName:    adapterName,
			AdapterActorID: bm.declaration.ActorID,
			ChannelID:      m.cfg.ChannelID,
			Chain:          m.cfg.HarnessChain,
			Clock:          m.cfg.Clock,
			CorrelationID:  requestID.String(),
			Detail:         "callback arrived after expires_at",
			Payload:        payload,
		}
		if err := emitOrphanCallbackEvents(ctx, ev); err != nil {
			return err
		}
		err := fmt.Errorf("framework: OnExternalCallbackFrame callback expired: now=%d expires_at=%d",
			m.cfg.Clock().UnixMilli(), frame.ExpiresAt)
		return devicetransit.NewAckError(devicetransit.AckRejectedPermanent, "callback_expired", "", err)
	}
	return m.runExternalCallbackFrame(ctx, bm, adapterName, frame, requestID.String())
}

func (m *Manager) runExternalCallback(ctx context.Context, bm *boundModule, adapterName string, payload []byte, correlationID string) (err error) {
	return m.runExternalCallbackInvoke(ctx, bm, adapterName, correlationID, func() error {
		return bm.module.OnExternalCallback(ctx, payload)
	})
}

func (m *Manager) runExternalCallbackFrame(ctx context.Context, bm *boundModule, adapterName string, frame adapter.ExternalCallbackFrame, correlationID string) error {
	if aware, ok := bm.module.(adapter.ExternalCallbackFrameAware); ok {
		return m.runExternalCallbackInvoke(ctx, bm, adapterName, correlationID, func() error {
			return aware.OnExternalCallbackFrame(ctx, frame)
		})
	}
	return m.runExternalCallback(ctx, bm, adapterName, frame.Payload, correlationID)
}

func (m *Manager) runExternalCallbackInvoke(ctx context.Context, bm *boundModule, adapterName string, correlationID string, invoke func() error) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		stack := string(debug.Stack())
		detail := fmt.Sprintf("adapter %s callback panic: %v\n%s", adapterName, r, stack)
		m.cfg.Logger.Error("framework.callback.panic",
			"adapter", adapterName,
			"correlation_id", correlationID,
			"panic", fmt.Sprint(r))
		m.cfg.Metrics.IncCounter("adapter.callback.panic", "adapter", adapterName)
		if correlationID != "" {
			if perr := bm.policy.OnExternalError(ctx,
				adapter.CorrelationKey(message.ID(correlationID)),
				message.TerminalReceiverInternalError,
				detail,
			); perr != nil {
				m.cfg.Logger.Error("framework.callback.panic.emit_failed",
					"adapter", adapterName,
					"correlation_id", correlationID,
					"err", perr.Error())
				err = fmt.Errorf("adapter %s callback panicked and failed-terminal emit failed: %v / %w",
					adapterName, r, perr)
				return
			}
			err = fmt.Errorf("adapter %s callback panicked (failed terminal emitted): %v", adapterName, r)
			return
		}
		err = fmt.Errorf("adapter %s callback panicked: %v", adapterName, r)
	}()
	return invoke()
}

// OnRuntimeEvent fans a runtime lifecycle signal out to every bound
// Module that implements adapter.RuntimeEventAware and matches the
// event's (ChannelID, AdapterActorID) routing key. Spec ref: proto-
// layer1 §3.6 O6 Lifecycle Awareness — the adapter framework owns the
// runtime-event delivery contract; this is the device-binding hook on
// top of channel-level boot/fence/shutdown wired by the composition
// root.
//
// Routing rules:
//   - If evt.AdapterActorID is non-empty, only the Module whose
//     Declaration.ActorID matches receives the event.
//   - If evt.AdapterActorID is empty, every Module on this channel
//     whose binding accepts lifecycle events (currently only
//     runtime_inbound_via_relay) receives the event.
//
// Errors are collected; the first non-nil error is returned so that
// callers can observe failures, but remaining Modules still see the
// event. Panics are recovered and logged so one bad Module cannot
// take down the whole dispatcher.
func (m *Manager) OnRuntimeEvent(ctx context.Context, evt adapter.RuntimeEvent) error {
	if evt.ChannelID != "" && m.cfg.ChannelID != "" && evt.ChannelID != m.cfg.ChannelID {
		// Wrong channel — drop silently. The composition root routes
		// events by channel id; a mismatch here is a wiring bug, not
		// per-module concern.
		return nil
	}

	m.mu.RLock()
	bound := make([]*boundModule, 0, len(m.byName))
	for _, bm := range m.byName {
		bound = append(bound, bm)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, bm := range bound {
		if evt.AdapterActorID != "" && bm.declaration.ActorID != evt.AdapterActorID {
			continue
		}
		aware, ok := bm.module.(adapter.RuntimeEventAware)
		if !ok {
			continue
		}
		if err := m.dispatchRuntimeEvent(ctx, bm, aware, evt); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *Manager) dispatchRuntimeEvent(
	ctx context.Context,
	bm *boundModule,
	aware adapter.RuntimeEventAware,
	evt adapter.RuntimeEvent,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			m.cfg.Logger.Error("framework.runtime_event.panic",
				"adapter", bm.declaration.Name,
				"event_kind", string(evt.Kind),
				"panic", fmt.Sprint(r),
				"stack", stack)
			m.cfg.Metrics.IncCounter("adapter.runtime_event.panic", "adapter", bm.declaration.Name)
			err = fmt.Errorf("adapter %s OnRuntimeEvent panicked: %v", bm.declaration.Name, r)
		}
	}()
	m.cfg.Metrics.IncCounter("adapter.runtime_event", "adapter", bm.declaration.Name, "event_kind", string(evt.Kind))
	if err := aware.OnRuntimeEvent(ctx, evt); err != nil {
		return err
	}
	if _, ok := bm.module.(adapter.Heartbeater); ok {
		m.runHeartbeatOnce(ctx, bm, true)
	}
	return nil
}

func normalizeExternalCallbackPayload(frame adapter.ExternalCallbackFrame) ([]byte, bool, error) {
	payload := append([]byte(nil), frame.Payload...)
	var env message.Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return payload, false, nil
	}
	isEnvelope := env.Kind != "" || env.ID != "" || env.ParentID != "" || env.Sender.ID != ""
	if !isEnvelope {
		return payload, false, nil
	}
	if env.ChannelID == "" {
		env.ChannelID = frame.ChannelID
	} else if env.ChannelID != frame.ChannelID {
		return nil, false, fmt.Errorf("framework: callback envelope channel mismatch: envelope=%s frame=%s",
			env.ChannelID, frame.ChannelID)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil, false, fmt.Errorf("framework: marshal callback envelope: %w", err)
	}
	return raw, env.Kind == message.KindEvent && env.Type == "actor.readiness.changed", nil
}

func callbackCorrelationID(payload []byte) string {
	var body struct {
		Kind          message.Kind `json:"kind"`
		ParentID      string       `json:"parent_id"`
		CorrelationID string       `json:"correlation_id"`
		RequestID     string       `json:"request_id"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return ""
	}
	if body.Kind == message.KindResponse && body.ParentID != "" {
		return body.ParentID
	}
	if body.CorrelationID != "" {
		return body.CorrelationID
	}
	return body.RequestID
}

// RunGC starts the periodic GC ticker. Stops on ctx cancellation.
// Calling RunGC more than once aborts the previous ticker.
func (m *Manager) RunGC(ctx context.Context) {
	m.mu.Lock()
	if m.gcCancel != nil {
		m.gcCancel()
	}
	gcCtx, cancel := context.WithCancel(ctx)
	m.gcCancel = cancel
	m.mu.Unlock()

	ticker := time.NewTicker(m.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-gcCtx.Done():
			return
		case <-ticker.C:
			m.runOneGCPass(gcCtx)
		}
	}
}

func (m *Manager) runOneGCPass(ctx context.Context) {
	m.mu.RLock()
	mods := make([]*boundModule, len(m.modules))
	copy(mods, m.modules)
	m.mu.RUnlock()
	now := m.cfg.Clock().UnixMilli()
	for _, bm := range mods {
		pending, err := bm.correlation.ListPending(ctx)
		if err != nil {
			m.cfg.Logger.Warn("framework.gc.list_pending",
				"adapter", bm.declaration.Name, "err", err.Error())
			continue
		}
		stale := 0
		for _, e := range pending {
			if e.ExpiresAt+int64((30*time.Second)/time.Millisecond) < now {
				stale++
				m.cfg.Logger.Warn("framework.gc.stale_pending",
					"adapter", bm.declaration.Name,
					"request_id", e.RequestID,
					"expired_ago_ms", now-e.ExpiresAt)
			}
		}
		if stale > 0 {
			m.cfg.Metrics.IncCounter("adapter.gc.stale_pending",
				"adapter", bm.declaration.Name)
		}
	}
}

// Shutdown stops timers + the GC ticker and calls Module.Shutdown on
// each installed module.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.gcCancel != nil {
		m.gcCancel()
		m.gcCancel = nil
	}
	mods := make([]*boundModule, len(m.modules))
	copy(mods, m.modules)
	m.mu.Unlock()

	var firstErr error
	for _, bm := range mods {
		if bm.heartbeatCancel != nil {
			bm.heartbeatCancel()
			if bm.heartbeatDone != nil {
				select {
				case <-bm.heartbeatDone:
				case <-ctx.Done():
					if firstErr == nil {
						firstErr = fmt.Errorf("framework: shutdown %s heartbeat: %w", bm.declaration.Name, ctx.Err())
					}
				}
			}
		}
		bm.policy.shutdown()
		if err := bm.module.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("framework: shutdown %s: %w", bm.declaration.Name, err)
		}
	}
	return firstErr
}

// InstalledAdapters returns the sorted list of installed adapter names.
// Convenience for ops / debugging.
func (m *Manager) InstalledAdapters() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.modules))
	for _, bm := range m.modules {
		out = append(out, bm.declaration.Name)
	}
	sort.Strings(out)
	return out
}

// AdaptersByBinding returns the sorted list of installed adapter names
// whose Declaration.Binding equals the supplied binding. Composition
// roots use this to discover which adapters need binding-specific wiring
// — notably the runtime_inbound_via_relay binding's inbound callback hook (the
// daemon's per-channel SetDeviceCallback dispatches `device_transit.send`
// frames — impl-layer2 §5.3.1 inbound — to the Manager.OnExternalCallback
// of these adapters).
//
// Returns an empty slice (not nil) when no adapter matches so callers
// can range over it without a nil check.
func (m *Manager) AdaptersByBinding(b actor.Binding) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.modules))
	for _, bm := range m.modules {
		if bm.declaration.Binding == b {
			out = append(out, bm.declaration.Name)
		}
	}
	sort.Strings(out)
	return out
}

func validateDeclaration(d adapter.Declaration) error {
	if d.Name == "" {
		return errors.New("framework: Declaration.Name required")
	}
	if d.ActorID == "" {
		return fmt.Errorf("framework: Declaration[%s].ActorID required", d.Name)
	}
	if len(d.Types) == 0 {
		return fmt.Errorf("framework: Declaration[%s].Types must be non-empty", d.Name)
	}
	if _, ok := actor.ParseBinding(string(d.Binding)); !ok {
		return fmt.Errorf("framework: Declaration[%s].Binding %q invalid", d.Name, d.Binding)
	}
	if d.MaxPendingMs <= 0 {
		return fmt.Errorf("%w: adapter=%s",
			asInstallError(message.InstallAdapterTimeoutMissing), d.Name)
	}
	// duplicate type detection
	seen := map[string]struct{}{}
	for _, t := range d.Types {
		if t == "" {
			return fmt.Errorf("framework: Declaration[%s].Types contains empty entry", d.Name)
		}
		if _, dup := seen[t]; dup {
			return fmt.Errorf("framework: Declaration[%s].Types contains duplicate %q", d.Name, t)
		}
		seen[t] = struct{}{}
	}
	return nil
}

func declHasType(d adapter.Declaration, t string) bool {
	for _, candidate := range d.Types {
		if candidate == t {
			return true
		}
	}
	return false
}

func declAllowsKind(d adapter.Declaration, t string, k message.Kind) bool {
	if !declHasType(d, t) {
		return false
	}
	td, ok := d.TypeDeclarations[t]
	if !ok {
		return d.TypeDeclarations == nil
	}
	for _, allowed := range td.AllowedKinds {
		if allowed == k {
			return true
		}
	}
	return false
}

func declarationNeeds(d adapter.Declaration, need string) bool {
	for _, candidate := range d.Needs {
		if candidate == need {
			return true
		}
	}
	return false
}

// InstallError wraps a kernel InstallReason so callers can errors.As to
// extract the reason while keeping a friendly message.
type InstallError struct {
	Reason message.InstallReason
}

// Error returns the reason's wire form.
func (e *InstallError) Error() string { return string(e.Reason) }

// Unwrap returns nil (terminal in the chain). The wrapping fmt.Errorf
// keeps the friendly suffix.
func (e *InstallError) Unwrap() error { return nil }

func asInstallError(r message.InstallReason) error { return &InstallError{Reason: r} }
