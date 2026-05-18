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
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func cloneSchemaMap(in map[message.Kind]json.RawMessage) map[message.Kind]json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make(map[message.Kind]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = append(json.RawMessage(nil), v...)
	}
	return out
}

// ManagerConfig parameterises NewManager. ChannelID, ActorRegistry,
// TypeRegistry, HarnessChain, RequestLookup are required; the rest get
// safe defaults when nil.
type ManagerConfig struct {
	// ChannelID is the channel this Manager scopes every adapter to.
	ChannelID channel.ID

	// ActorRegistry is the channel-local actor registry. Install
	// queries it to verify each Declaration.ActorID exists with the
	// right binding.
	ActorRegistry actor.Registry

	// TypeRegistry receives one Upsert per Declaration.Type at Install
	// time. Framework ships InMemoryTypeRegistry for tests.
	TypeRegistry TypeRegistry

	// HarnessChain is the kernel/harness write entry point — every
	// adapter response flows through here.
	HarnessChain harness.Chain

	// RequestLookup recovers the original request envelope (for F5
	// Respond). Production wires it to runtime/store; tests use
	// MemoryRequestLookup.
	RequestLookup RequestLookup

	// DeviceTransit is optional; required only when a module declares
	// Binding == BindingViaServerTransit. The framework refuses
	// Install for such modules when DeviceTransit is nil.
	DeviceTransit adapter.DeviceTransit

	// HTTPClient is optional; modules with Binding == BindingOutboundHTTP
	// receive it via ModuleContext (extension surface — kernel/adapter
	// does not currently expose the field; the framework attaches it
	// behind a typed assertion through ModuleContext.HarnessChain not
	// applicable here, so adapters call a per-module accessor —
	// see ModuleContext below). When nil for an outbound_http module,
	// Install logs a warning but still proceeds (tests may provide
	// their own client via the Init phase).
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
	byActor map[string]*boundModule
	byName  map[string]*boundModule

	gcCancel context.CancelFunc
}

// boundModule is the Manager-side bookkeeping for one installed Module.
type boundModule struct {
	module      adapter.Module
	declaration adapter.Declaration
	mctx        *adapter.ModuleContext
	correlation *memoryCorrelationTracker
	policy      *timerPolicy
	state       *NamespacedStateStore
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
		byActor: map[string]*boundModule{},
		byName:  map[string]*boundModule{},
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
	if _, dup := m.byActor[string(decl.ActorID)]; dup {
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
	if !decl.Binding.MatchesActorBinding(rec.Binding) {
		return fmt.Errorf("%w: actor=%s actor_binding=%s declared=%s",
			asInstallError(message.InstallHandlerActorBindingMismatch),
			decl.ActorID, rec.Binding, decl.Binding)
	}

	// Binding-specific dependency check.
	if decl.Binding == adapter.BindingViaServerTransit && m.cfg.DeviceTransit == nil {
		return fmt.Errorf("framework: adapter %q requires DeviceTransit (binding=via_server_transit) but ManagerConfig.DeviceTransit is nil",
			decl.Name)
	}
	if decl.Binding == adapter.BindingOutboundHTTP && m.cfg.HTTPClient == nil {
		m.cfg.Logger.Warn("framework.install.outbound_http.no_client",
			"adapter", decl.Name,
			"note", "ManagerConfig.HTTPClient nil — adapter must provide its own")
	}

	// Upsert types into the type_registry. Per L2 §1.4.2 install rules,
	// each row carries allowed_kinds + per-kind payload schemas +
	// fallback_response_schema (when request) + terminal_convention.
	// Adapters omitting decl.TypeSchemas[type] fall back to permissive
	// defaults (allowed_kinds={event,request,response}, payload_status
	// terminal); install logs a warning so the gap stays observable.
	for _, t := range decl.Types {
		schema, hasSchema := decl.TypeSchemas[t]
		if !hasSchema {
			schema = adapter.TypeSchema{
				AllowedKinds: []message.Kind{
					message.KindEvent,
					message.KindRequest,
					message.KindResponse,
				},
			}
			m.cfg.Logger.Warn("framework.install.type_schema.permissive_default",
				"adapter", decl.Name,
				"type", t,
				"note", "decl.TypeSchemas[type] missing — using permissive defaults; harness will not enforce per-payload schema")
		} else {
			if err := ValidateTypeSchema(t, schema); err != nil {
				return fmt.Errorf("framework: validate type=%s: %w", t, err)
			}
		}

		conv := schema.TerminalConvention
		if conv == "" {
			conv = string(TerminalPayloadStatus)
		}

		row := TypeRow{
			Type:                   t,
			HandlerActorID:         decl.ActorID,
			HandlerBinding:         decl.Binding,
			MaxPendingMs:           decl.MaxPendingMs,
			AllowedKinds:           append([]message.Kind(nil), schema.AllowedKinds...),
			SchemasByKind:          cloneSchemaMap(schema.SchemasByKind),
			FallbackResponseSchema: append(json.RawMessage(nil), schema.FallbackResponseSchema...),
			TerminalConvention:     TerminalConvention(conv),
		}
		if row.MaxPendingMs <= 0 {
			return fmt.Errorf("%w: adapter=%s type=%s",
				asInstallError(message.InstallAdapterTimeoutMissing), decl.Name, t)
		}
		if _, err := m.cfg.TypeRegistry.Upsert(ctx, row); err != nil {
			return fmt.Errorf("%w: adapter=%s type=%s: %v",
				asInstallError(message.InstallTypeRegistryInvalid), decl.Name, t, err)
		}
	}

	// Build framework helpers.
	state := NewNamespacedStateStore(m.cfg.StateStore, "adapter:"+decl.Name)
	corr := newCorrelationTracker(decl.Name, state)
	policy := newTimerPolicy(decl.Name, corr, m.cfg.Logger, m.cfg.Metrics, m.cfg.Clock)
	respond, err := buildRespond(respondConfig{
		adapterName:    decl.Name,
		adapterActorID: decl.ActorID,
		channelID:      m.cfg.ChannelID,
		lookup:         m.cfg.RequestLookup,
		chain:          m.cfg.HarnessChain,
		correlation:    corr,
		policy:         policy,
		clock:          m.cfg.Clock,
		logger:         m.cfg.Logger,
		metrics:        m.cfg.Metrics,
	})
	if err != nil {
		return fmt.Errorf("framework: build respond for %s: %w", decl.Name, err)
	}
	policy.bindRespond(respond)

	mctx := &adapter.ModuleContext{
		AdapterName:    decl.Name,
		AdapterActorID: decl.ActorID,
		ChannelID:      m.cfg.ChannelID,
		Correlation:    corr,
		ErrorPolicy:    policy,
		Respond:        respond,
		HarnessChain:   m.cfg.HarnessChain,
	}
	if decl.Binding == adapter.BindingViaServerTransit {
		mctx.DeviceTransit = m.cfg.DeviceTransit
	}

	if err := mod.Init(ctx, mctx); err != nil {
		return fmt.Errorf("framework: module %s init: %w", decl.Name, err)
	}

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
	m.byActor[string(decl.ActorID)] = bm
	m.byName[decl.Name] = bm
	m.mu.Unlock()

	m.cfg.Metrics.IncCounter("adapter.install.ok",
		"adapter", decl.Name, "binding", string(decl.Binding))
	m.cfg.Logger.Info("framework.install.ok",
		"adapter", decl.Name,
		"binding", string(decl.Binding),
		"types", decl.Types,
		"channel_id", string(m.cfg.ChannelID))
	return nil
}

// BootRecoverTimers re-arms every pending request's F3 timer per L2
// §8.6 step 3. Called once after Install.
func (m *Manager) BootRecoverTimers(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, bm := range m.modules {
		if err := bm.correlation.recoverFromStore(ctx); err != nil {
			return fmt.Errorf("framework: recover %s: %w", bm.declaration.Name, err)
		}
		pending, err := bm.correlation.ListPending(ctx)
		if err != nil {
			return fmt.Errorf("framework: list pending %s: %w", bm.declaration.Name, err)
		}
		for _, e := range pending {
			deadline := time.UnixMilli(e.ExpiresAt)
			if err := bm.policy.RegisterTimer(ctx, e.RequestID, deadline); err != nil {
				m.cfg.Logger.Warn("framework.recover.register_timer",
					"adapter", bm.declaration.Name,
					"request_id", e.RequestID,
					"err", err.Error())
			}
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
	if env.ChannelID != string(m.cfg.ChannelID) {
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

	// Verify the type is one the adapter declared.
	if !declHasType(bm.declaration, env.Type) {
		return fmt.Errorf("framework: Dispatch type %s not declared by adapter %s",
			env.Type, bm.declaration.Name)
	}

	span := m.cfg.Tracer.StartSpan("adapter.dispatch",
		"adapter", bm.declaration.Name, "type", env.Type)
	defer span.End()

	// Reserve correlation entry + register timer.
	now := m.cfg.Clock()
	deadline := now.Add(time.Duration(bm.declaration.MaxPendingMs) * time.Millisecond)
	entry := adapter.CorrelationEntry{
		RequestID:     env.ID,
		CorrelationID: env.CorrelationID,
		ChannelID:     env.ChannelID,
		AudienceActor: string(bm.declaration.ActorID),
		ParentID:      env.ID,
		EnqueuedAt:    now.UnixMilli(),
		ExpiresAt:     deadline.UnixMilli(),
		State:         adapter.CorrelationPending,
	}
	if _, err := bm.correlation.Reserve(ctx, entry); err != nil {
		return fmt.Errorf("framework: dispatch reserve: %w", err)
	}
	if err := bm.policy.RegisterTimer(ctx, env.ID, deadline); err != nil {
		return fmt.Errorf("framework: dispatch register timer: %w", err)
	}
	m.cfg.Metrics.IncCounter("adapter.dispatch",
		"adapter", bm.declaration.Name, "type", env.Type)

	if err := m.runHandle(ctx, bm, env); err != nil {
		m.cfg.Logger.Warn("framework.dispatch.handle.error",
			"adapter", bm.declaration.Name,
			"type", env.Type,
			"request_id", env.ID,
			"err", err.Error())
		m.cfg.Metrics.IncCounter("adapter.dispatch.handle_error",
			"adapter", bm.declaration.Name, "type", env.Type)
		return err
	}
	return nil
}

// runHandle wraps Module.Handle with a panic recover that emits a
// failed terminal via ErrorPolicy.OnExternalError per L2 §8 F3 panic
// safety. The terminal carries reason=receiver_unavailable + detail
// containing the panic message and stack trace (closed-set reason from
// L1 §10.3.3; "handler crashed" maps semantically to "receiver
// unavailable" — no separate handler_panic reason exists in the spec).
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
		if perr := bm.policy.OnExternalError(ctx,
			adapter.CorrelationKey(env.ID),
			message.TerminalReceiverUnavailable,
			detail,
		); perr != nil {
			m.cfg.Logger.Error("framework.dispatch.handle.panic.emit_failed",
				"adapter", bm.declaration.Name,
				"request_id", env.ID,
				"err", perr.Error())
			err = fmt.Errorf("adapter %s panicked and failed-terminal emit failed: %v / %w",
				bm.declaration.Name, r, perr)
			return
		}
		err = fmt.Errorf("adapter %s panicked (failed terminal emitted): %v", bm.declaration.Name, r)
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
	return bm.module.OnExternalCallback(ctx, payload)
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
// — notably the via_server_transit binding's inbound callback hook (the
// daemon's per-channel SetDeviceCallback dispatches device_transit.recv
// frames to the Manager.OnExternalCallback of these adapters).
//
// Returns an empty slice (not nil) when no adapter matches so callers
// can range over it without a nil check.
func (m *Manager) AdaptersByBinding(b adapter.BindingKind) []string {
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
	if _, ok := adapter.NormalizeBinding(string(d.Binding)); !ok {
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
