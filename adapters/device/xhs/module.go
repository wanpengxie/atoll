package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/adapters/device/framework"
	adapterframework "github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// DeviceState is the closed set of device runtime states the xhs adapter
// projects from its devicebus connection lifecycle. Spec ref:
// proto-layer1 §3.6 O6 (adapter owns its black-box runtime state).
type DeviceState string

const (
	// DeviceStateUnknown is the initial state before any lifecycle event
	// arrives. Handle treats unknown as offline — fail-fast so the LLM
	// gets a clean terminal instead of waiting on F3 default timeout.
	DeviceStateUnknown DeviceState = "unknown"
	// DeviceStateOnline — extension WS is registered + reachable.
	DeviceStateOnline DeviceState = "online"
	// DeviceStateOffline — extension WS closed cleanly or unexpectedly.
	DeviceStateOffline DeviceState = "offline"
	// DeviceStateTokenExpired — actor token past expires_at; re-bind
	// required from the UI. Distinct from offline because the recovery
	// path is different (re-issue token vs reload extension).
	DeviceStateTokenExpired DeviceState = "token_expired"
)

// Config tunes a Module instance. Everything carries a defensible default.
type Config struct {
	// AdapterActorID overrides the actor_registry id this Module owns.
	// Empty defaults to DefaultAdapterActorID (`tool:xhs-adapter`).
	AdapterActorID actor.ActorID

	// MaxPendingMs overrides the per-type pending budget. Zero defaults
	// to DefaultMaxPendingMs (5 min).
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
type Module struct {
	cfg        Config
	mctx       *adapter.ModuleContext
	proxy      *framework.DeviceProxy
	now        func() time.Time
	externalID func(string) string

	// pendingMu guards pendingType. kernel/adapter.CorrelationEntry does
	// not carry envelope.type — the per-type allow-list (R4-FIX-A) is
	// adapter-domain knowledge, so we mirror the request type alongside
	// the framework correlation entry. Cleared on success / cancel.
	pendingMu   sync.Mutex
	pendingType map[string]string

	// deviceState tracks the adapter-owned runtime state derived from
	// devicebus lifecycle signals (RuntimeEventDeviceLifecycle). atomic
	// store so Handle / OnExternalCallback / OnRuntimeEvent paths read
	// without locking. Stores a DeviceState string.
	deviceState atomic.Value // DeviceState

	// deviceID is the last-known device identifier the actor token was
	// issued against. Informative — included in the emitted lifecycle
	// channel events so UI / agent can tell two browsers apart when the
	// channel is rebound across hardware.
	deviceIDMu sync.RWMutex
	deviceID   string

	// stateChangeMu serialises state transitions so a parallel
	// connected/disconnected race produces a single channel event per
	// distinct state, not two. Only OnRuntimeEvent takes this lock.
	stateChangeMu sync.Mutex
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
	m.deviceState.Store(DeviceStateUnknown)
	return m, nil
}

// DeviceState returns the adapter's current view of its device runtime
// state. Exposed for tests + observability; production callers should
// subscribe to the channel events (TypeDeviceOnline / TypeDeviceOffline)
// rather than poll.
func (m *Module) DeviceState() DeviceState {
	v, _ := m.deviceState.Load().(DeviceState)
	if v == "" {
		return DeviceStateUnknown
	}
	return v
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
	return nil
}

// Shutdown is a no-op. Pending requests resolve via the F3 timer on
// the next daemon boot (Manager.BootRecoverTimers re-arms them per
// L2 §8.6 step 3).
func (m *Module) Shutdown(_ context.Context) error { return nil }

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
		return m.failNow(ctx, env.ID.String(), message.TerminalReceiverUnavailable, "device_token_expired", "extension pairing token expired; re-bind via UI")
	default:
		// DeviceStateOffline / DeviceStateUnknown — adapter knows the
		// device isn't reachable; emit failed terminal without trying
		// the wire.
		return m.failNow(ctx, env.ID.String(), message.TerminalReceiverUnavailable, "device_offline", "extension is not connected for this channel")
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

// failNow emits a failed terminal via the framework Respond closure
// (sender = adapter actor, kind = response). Used for synchronous
// Handle failures (bad payload, missing session, push error). Errors
// from the underlying Respond bubble up to the caller (Manager.Dispatch
// surfaces them in the observability stream).
//
// The `terminalReason` parameter is the message.TerminalFailureReason
// value to feed ErrorPolicy.OnExternalError when the failure path
// wants the policy hook (some failures — payload decode — are pre-
// transit and don't need it; pass empty string to skip).
func (m *Module) failNow(ctx context.Context, requestID string, terminalReason message.TerminalFailureReason, errorCode, detail string) error {
	if m.mctx == nil {
		return fmt.Errorf("xhs.failNow: Init was not called (error_code=%s)", errorCode)
	}
	fields := map[string]any{"error_code": errorCode}
	if detail != "" {
		fields["detail"] = detail
	}
	payload, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		payload = []byte(`{}`)
	}
	respondReason := message.TerminalReceiverInternalError
	if terminalReason != "" {
		respondReason = terminalReason
	}
	opts := adapter.RespondOptions{Status: "failed", Reason: string(respondReason)}

	if terminalReason != "" {
		_ = m.mctx.ErrorPolicy.OnExternalError(ctx, adapter.CorrelationKey(message.ID(requestID)), terminalReason, detail)
	}

	// Walk back the framework bookkeeping so the F3 default-timeout
	// timer does not also fire later.
	m.proxy.CancelInFlight(ctx, requestID)
	m.clearPendingType(requestID)

	_, err := m.mctx.Respond(ctx, adapter.CorrelationKey(message.ID(requestID)), payload, opts)
	return err
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

// OnRuntimeEvent implements adapter.RuntimeEventAware. The framework
// dispatches devicebus connect / disconnect / token-expiry signals
// through this hook so the adapter can project them into its
// device-state machine without reaching into transport plumbing.
//
// Per impl-layer2 §5 + proto-layer1 §3.6 O6:
//   - signal source: server-side devicebus connection register /
//     unregister / token check;
//   - propagation: server → daemonbus → daemon ControlHandlers →
//     framework.Manager → here;
//   - effect: adapter mutates its private device-state field and
//     publishes a `xhs.device.online` / `xhs.device.offline` event to
//     the channel so UI / agent can observe.
func (m *Module) OnRuntimeEvent(ctx context.Context, evt adapter.RuntimeEvent) error {
	if evt.Kind != adapter.RuntimeEventDeviceLifecycle || evt.DeviceLifecycle == nil {
		return nil
	}
	lf := evt.DeviceLifecycle
	next := DeviceStateUnknown
	eventType := ""
	switch lf.Event {
	case devicetransit.LifecycleConnected:
		next = DeviceStateOnline
		eventType = TypeDeviceOnline
	case devicetransit.LifecycleDisconnected:
		next = DeviceStateOffline
		eventType = TypeDeviceOffline
	case devicetransit.LifecycleTokenExpired:
		next = DeviceStateTokenExpired
		eventType = TypeDeviceOffline
	default:
		// Unknown lifecycle event — log and drop. Future-proofing for
		// kernel devicetransit closed-set additions.
		return nil
	}

	m.stateChangeMu.Lock()
	prev := m.DeviceState()
	if prev == next {
		m.stateChangeMu.Unlock()
		return nil
	}
	m.deviceState.Store(next)
	if lf.DeviceID != "" {
		m.deviceIDMu.Lock()
		m.deviceID = lf.DeviceID
		m.deviceIDMu.Unlock()
	}
	m.stateChangeMu.Unlock()

	return m.emitDeviceLifecycle(ctx, eventType, lf, prev, next)
}

// emitDeviceLifecycle writes a `xhs.device.online` / `xhs.device.offline`
// event envelope into the channel so UI and other actors can observe
// the adapter's device state. Failures are best-effort and logged via
// the harness chain return; state transition has already committed.
func (m *Module) emitDeviceLifecycle(
	ctx context.Context,
	eventType string,
	lf *devicetransit.LifecycleFrame,
	prev, next DeviceState,
) error {
	if m.mctx == nil || m.mctx.HarnessChain == nil {
		return nil
	}
	if eventType == "" {
		return nil
	}
	payload := map[string]any{
		"device_state":    string(next),
		"previous_state":  string(prev),
		"lifecycle_event": string(lf.Event),
	}
	if lf.DeviceID != "" {
		payload["device_id"] = lf.DeviceID
	}
	if lf.Detail != "" {
		payload["detail"] = lf.Detail
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("xhs.OnRuntimeEvent: marshal payload: %w", err)
	}
	now := m.now().UnixMilli()
	env := &message.Envelope{
		ID:        envelopeID(m.mctx.AdapterActorID, lf.Event, now),
		ChannelID: m.mctx.ChannelID,
		Type:      eventType,
		Kind:      message.KindEvent,
		Sender: message.Sender{
			Kind: actor.KindTool,
			ID:   m.mctx.AdapterActorID,
		},
		// Visibility=system: UI default-folds these in the chat view but
		// still receives them on the /ws push, so the device-status
		// indicator can update without polling.
		// Audience=[SystemActorID]: this is an observability event; no
		// other actor needs to be triggered as a handler. The visibility
		// channel is what drives view fanout (proto-layer1 §4.1.3).
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
		Payload:    body,
		TS:         now,
		TSReceived: now,
	}
	if _, err := m.mctx.HarnessChain.Write(ctx, env); err != nil {
		return fmt.Errorf("xhs.OnRuntimeEvent: harness write %s: %w", eventType, err)
	}
	return nil
}

// envelopeID derives a deterministic-shape id for adapter lifecycle
// events. Format: `event:<actor_id>:<lifecycle_kind>:<ts_ms>`. Not
// cryptographically unique; harness step 8 dedupes if a same-ts event
// somehow recurs (shouldn't, lifecycle changes are state-machine
// driven).
func envelopeID(actorID actor.ActorID, lifecycleKind devicetransit.LifecycleEvent, tsMs int64) message.ID {
	return message.ID(fmt.Sprintf("event:%s:%s:%d", actorID, lifecycleKind, tsMs))
}
