package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/adapters/device/framework"
	adapterframework "github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// DeviceState re-exports framework.DeviceState so xhs callers don't
// need to import the device framework package for the closed set.
type DeviceState = framework.DeviceState

const (
	DeviceStateUnknown      = framework.DeviceStateUnknown
	DeviceStateOnline       = framework.DeviceStateOnline
	DeviceStateOffline      = framework.DeviceStateOffline
	DeviceStateTokenExpired = framework.DeviceStateTokenExpired
)

// Config tunes a Module instance. Everything carries a defensible default.
type Config struct {
	// AdapterActorID overrides the actor_registry id this Module owns.
	// Empty defaults to DefaultAdapterActorID (`tool:xhs-adapter`).
	AdapterActorID actor.ActorID

	// MaxPendingMs overrides the per-type pending budget. Zero defaults
	// to DefaultMaxPendingMs (30s).
	MaxPendingMs int64

	// Now is a clock injection point for tests. Defaults to time.Now.
	Now func() time.Time

	// NewExternalCorrelationID mints the id placed in Command.CorrelationID
	// when the adapter wants to hide envelope.id from the extension.
	// Empty defaults to envelope.id directly (launch baseline:
	// outbound `device_transit.recv.request_id` (impl-layer2 §5.3.2)
	// carries the canonical id; no need for a daemon-internal external
	// id like M1.3 had — extension echoes request_id back via inbound
	// `device_transit.send.request_id` (§5.3.1)).
	NewExternalCorrelationID func(envelopeID string) string
}

// Module implements kernel/adapter.Module for the xhs device adapter.
// One instance per channel per daemon process.
//
// Cross-binding helpers in use:
//   - adapters/framework.FailNow: synchronous failure path (Handle gate
//   - payload decode + push error)
//   - adapters/device/framework.LifecycleTracker: device state machine
//   - lifecycle event emission (delegated from OnRuntimeEvent)
type Module struct {
	cfg        Config
	mctx       *adapter.ModuleContext
	proxy      *framework.DeviceProxy
	lifecycle  *framework.LifecycleTracker
	now        func() time.Time
	externalID func(string) string

	// pendingMu guards pendingType. kernel/adapter.CorrelationEntry does
	// not carry envelope.type — the per-type allow-list (R4-FIX-A) is
	// adapter-domain knowledge, so we mirror the request type alongside
	// the framework correlation entry. Cleared on success / cancel.
	pendingMu   sync.Mutex
	pendingType map[string]string
}

// New constructs a Module from cfg. Validates the required fields up
// front so a misconfigured wire-up fails at composition root rather
// than at first request dispatch.
func New(cfg Config) (*Module, error) {
	if cfg.AdapterActorID == "" {
		cfg.AdapterActorID = DefaultAdapterActorID
	}
	if cfg.MaxPendingMs <= 0 {
		cfg.MaxPendingMs = DefaultMaxPendingMs
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewExternalCorrelationID == nil {
		cfg.NewExternalCorrelationID = func(envelopeID string) string { return envelopeID }
	}
	m := &Module{
		cfg:         cfg,
		now:         cfg.Now,
		externalID:  cfg.NewExternalCorrelationID,
		pendingType: map[string]string{},
	}
	return m, nil
}

// DeviceState returns the adapter's current view of its device runtime
// state via the embedded LifecycleTracker. Exposed for tests +
// observability; production callers should subscribe to the channel
// events (TypeDeviceOnline / TypeDeviceOffline) rather than poll.
func (m *Module) DeviceState() DeviceState {
	if m.lifecycle == nil {
		return DeviceStateUnknown
	}
	return m.lifecycle.State()
}

// Declares returns the static adapter metadata. Called exactly once
// per Install per channel by the framework (L2 §8.1).
//
// TypeDeclarations — every xhs type in domain-xhs-spec §1.1–§1.6
// ships a per-type allowed_kinds + terminal_convention row. The
// framework fails install closed when an adapter declares
// TypeDeclarations but a Types entry has no matching row (no silent
// permissive-default fallback when the adapter has explicitly opted
// into strict mode).
//
// Level A: payload schemas are NOT declared at the protocol layer
// (proto-layer0 §1.4.1).
func (m *Module) Declares() adapter.Declaration {
	return adapter.Declaration{
		Description:      actorDescription,
		SkillDoc:         actorSkillDoc,
		Name:             AdapterName,
		ActorID:          m.cfg.AdapterActorID,
		Types:            append([]string{}, AllTypes...),
		TypeDeclarations: DeclarationTypeDeclarations(),
		Binding:          Binding,
		MaxPendingMs:     m.cfg.MaxPendingMs,
	}
}

// Init captures the framework-provided ModuleContext + constructs the
// DeviceProxy that wraps DeviceTransit + Correlation + ErrorPolicy.
// Returns an error if DeviceTransit is absent (the framework MUST
// inject it for runtime_inbound_via_relay bindings — covers codex warning
// #15).
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("xhs.Init: ModuleContext is nil")
	}
	if mctx.DeviceTransit == nil {
		return errors.New("xhs.Init: ModuleContext.DeviceTransit is nil; runtime_inbound_via_relay binding requires runtime/transit to wire it (T3)")
	}
	if mctx.Correlation == nil {
		return errors.New("xhs.Init: ModuleContext.Correlation is nil")
	}
	if mctx.ErrorPolicy == nil {
		return errors.New("xhs.Init: ModuleContext.ErrorPolicy is nil")
	}
	if mctx.Respond == nil {
		return errors.New("xhs.Init: ModuleContext.Respond is nil")
	}
	if mctx.AdapterActorID == "" {
		mctx.AdapterActorID = m.cfg.AdapterActorID
	}
	proxy, err := framework.NewDeviceProxy(
		AdapterName,
		mctx.AdapterActorID,
		mctx.ChannelID,
		framework.DeviceProxyDeps{
			Transit:     mctx.DeviceTransit,
			Correlation: mctx.Correlation,
			Policy:      mctx.ErrorPolicy,
		},
	)
	if err != nil {
		return fmt.Errorf("xhs.Init: build DeviceProxy: %w", err)
	}
	proxy.SetClock(m.now)
	m.mctx = mctx
	m.proxy = proxy

	lifecycle, err := framework.NewLifecycleTracker(framework.LifecycleTrackerConfig{
		EventTypes: map[framework.DeviceState]string{
			framework.DeviceStateOnline:       TypeDeviceOnline,
			framework.DeviceStateOffline:      TypeDeviceOffline,
			framework.DeviceStateTokenExpired: TypeDeviceOffline,
		},
		AdapterActorID: mctx.AdapterActorID,
		ChannelID:      mctx.ChannelID,
		HarnessChain:   mctx.HarnessChain,
		Clock:          m.now,
	})
	if err != nil {
		return fmt.Errorf("xhs.Init: build LifecycleTracker: %w", err)
	}
	m.lifecycle = lifecycle
	return nil
}

// Shutdown is a no-op. Pending requests resolve via the F3 timer on
// the next daemon boot (Manager.BootRecoverTimers re-arms them per
// L2 §8.6 step 3).
func (m *Module) Shutdown(_ context.Context) error { return nil }

// Heartbeat reports the current device lifecycle state to the adapter
// framework. It does not probe the extension; devicebus lifecycle
// events are the source of truth and the heartbeat is only a fallback
// projection into actor_registry readiness.
func (m *Module) Heartbeat(_ context.Context) (adapter.HeartbeatReport, error) {
	state := m.DeviceState()
	report := adapter.HeartbeatReport{
		CheckedAt: m.now(),
		Detail: map[string]any{
			"device_state": string(state),
		},
	}
	switch state {
	case DeviceStateOnline:
		report.Available = true
		report.Reason = "ok"
	case DeviceStateTokenExpired:
		report.Available = false
		report.Reason = "token_expired"
	default:
		report.Available = false
		report.Reason = "device_offline"
	}
	return report, nil
}

// Status enriches actor.status with xhs lifecycle detail.
func (m *Module) Status(ctx context.Context) (adapter.StatusReport, error) {
	hb, err := m.Heartbeat(ctx)
	return adapter.StatusReport(hb), err
}

// Handle translates one inbound kind=request envelope into an outbound
// `device_transit.recv` frame (impl-layer2 §5.3.2 outbound — adapter →
// device). Synchronous failure paths emit a failed terminal immediately
// so the agent observes a closed-set terminal reason without waiting on
// the F3 fallback timer.
func (m *Module) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil || m.proxy == nil {
		return errors.New("xhs.Handle: Init was not called")
	}
	if env == nil {
		return errors.New("xhs.Handle: envelope is nil")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("xhs.Handle: envelope kind must be %q, got %q", message.KindRequest, env.Kind)
	}

	// Gate on device state — fail fast when the extension isn't
	// reachable so the LLM gets a clean terminal instead of waiting on
	// the F3 default-timeout timer (5 min).
	switch m.DeviceState() {
	case DeviceStateOnline:
		// fall through to dispatch path
	case DeviceStateTokenExpired:
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverUnavailable,
			ErrorCode:      "device_token_expired",
			Detail:         "extension pairing token expired; re-bind via UI",
		})
	default:
		// DeviceStateOffline / DeviceStateUnknown — adapter knows the
		// device isn't reachable; emit failed terminal without trying
		// the wire.
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverUnavailable,
			ErrorCode:      "device_offline",
			Detail:         "extension is not connected for this channel",
		})
	}

	cmd, err := buildCommand(env)
	if err != nil {
		return m.failNow(ctx, env.ID.String(), "", "payload_decode_failed", err.Error())
	}

	if newID := m.externalID(env.ID.String()); newID != "" {
		cmd.CorrelationID = newID
	}

	wirePayload, err := json.Marshal(cmd)
	if err != nil {
		return m.failNow(ctx, env.ID.String(), "", "command_marshal_failed", err.Error())
	}

	// Stash the requestType BEFORE Send so a callback that races the
	// proxy's bookkeeping still finds it. The stash is dropped on
	// terminal (success or cancel) inside completePending.
	m.setPendingType(env.ID.String(), env.Type)

	if _, err := m.proxy.SendRequest(ctx, env, wirePayload); err != nil {
		// SendRequest already rolled back correlation + timer; clear our
		// local stash too.
		m.clearPendingType(env.ID.String())
		return m.failNow(ctx, env.ID.String(), "", "device_push_failed", err.Error())
	}
	return nil
}

// OnExternalCallback decodes a `device_transit.send` payload
// (impl-layer2 §5.3.1 inbound — device → adapter) + emits the
// terminal response via ctx.Respond.
//
// The framework de-dupes orphan callbacks before invoking — when this
// runs, the request_id is guaranteed unresolved. Even so, we re-check
// correlation state so a duplicate inbound (e.g. extension retransmit
// after a network hiccup) does not double-Respond.
func (m *Module) OnExternalCallback(ctx context.Context, raw []byte) error {
	if m.mctx == nil || m.proxy == nil {
		return errors.New("xhs.OnExternalCallback: Init was not called")
	}
	cb, err := parseCallback(raw)
	if err != nil {
		if emitErr := adapterframework.EmitOrphanCallbackEvents(ctx, adapterframework.OrphanCallbackEvent{
			AdapterName:    AdapterName,
			AdapterActorID: m.mctx.AdapterActorID,
			ChannelID:      m.mctx.ChannelID,
			Chain:          m.mctx.HarnessChain,
			Clock:          m.now,
			Detail:         err.Error(),
			Payload:        raw,
		}); emitErr != nil {
			return fmt.Errorf("xhs.OnExternalCallback: emit parse failure event: %w", emitErr)
		}
		return err
	}

	entry, ok, err := m.proxy.LookupInFlight(ctx, cb.CorrelationID)
	if err != nil {
		return fmt.Errorf("xhs.OnExternalCallback: lookup correlation: %w", err)
	}
	if !ok {
		// Orphan callback — drop. Manager emits its observability event
		// (L1 §6.5); the adapter stays silent.
		return nil
	}
	if entry.State != adapter.CorrelationPending {
		// Duplicate / already-terminal. Drop silently — Respond's harness
		// dedupe would also catch this, but skipping the round trip is
		// cheaper.
		return nil
	}

	requestType := m.getPendingType(cb.CorrelationID)

	body, status, reason, err := buildRespondPayload(cb, requestType)
	if err != nil {
		return err
	}

	// Mark the correlation done before Respond so a race between F3 and
	// the success path does not double-emit.
	if err := m.proxy.CompleteInFlight(ctx, cb.CorrelationID); err != nil {
		return err
	}
	m.clearPendingType(cb.CorrelationID)

	_, err = m.mctx.Respond(ctx, adapter.CorrelationKey(message.ID(cb.CorrelationID)), body, adapter.RespondOptions{
		Status: status,
		Reason: reason,
	})
	return err
}

// failNow is the xhs-specific shim around adapterframework.FailNow.
// Wraps the cross-binding helper with the adapter's domain bookkeeping
// (per-request pendingType stash + DeviceProxy.CancelInFlight which
// owns both the F3 timer cancel + correlation mark on the device
// transit path).
func (m *Module) failNow(ctx context.Context, requestID string, terminalReason message.TerminalFailureReason, errorCode, detail string) error {
	if m.mctx == nil {
		return fmt.Errorf("xhs.failNow: Init was not called (error_code=%s)", errorCode)
	}
	if m.proxy != nil {
		m.proxy.CancelInFlight(ctx, requestID)
	}
	m.clearPendingType(requestID)
	return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
		RequestID:      message.ID(requestID),
		TerminalReason: terminalReason,
		ErrorCode:      errorCode,
		Detail:         detail,
	})
}

// setPendingType / getPendingType / clearPendingType keep the per-
// request envelope.type stash that the per-type allow-list relies on
// (kernel/adapter.CorrelationEntry intentionally omits Type — see
// kernel/adapter/correlation.go; adapter-domain metadata stays in the
// adapter).
func (m *Module) setPendingType(requestID, requestType string) {
	if requestID == "" {
		return
	}
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	m.pendingType[requestID] = requestType
}

func (m *Module) getPendingType(requestID string) string {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return m.pendingType[requestID]
}

func (m *Module) clearPendingType(requestID string) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	delete(m.pendingType, requestID)
}

// OnRuntimeEvent implements adapter.RuntimeEventAware. Delegates to the
// shared LifecycleTracker which owns the device state machine + channel
// event emission boilerplate. Per impl-layer2 §5 + proto-layer1 §3.6
// O6 the adapter remains the source of truth for "device alive"; this
// hook is just the wire path that signals state mutations.
func (m *Module) OnRuntimeEvent(ctx context.Context, evt adapter.RuntimeEvent) error {
	if evt.Kind != adapter.RuntimeEventDeviceLifecycle || evt.DeviceLifecycle == nil {
		return nil
	}
	if m.lifecycle == nil {
		return errors.New("xhs.OnRuntimeEvent: Init was not called (lifecycle tracker nil)")
	}
	return m.lifecycle.Apply(ctx, *evt.DeviceLifecycle)
}
