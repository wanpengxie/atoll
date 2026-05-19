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
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Config tunes a Module instance. Only SessionStore is mandatory at
// construction time; everything else carries a defensible default.
type Config struct {
	// AdapterActorID overrides the actor_registry id this Module owns.
	// Empty defaults to DefaultAdapterActorID (`tool:xhs-adapter`).
	AdapterActorID actor.ActorID

	// MaxPendingMs overrides the per-type pending budget. Zero defaults
	// to DefaultMaxPendingMs (5 min).
	MaxPendingMs int64

	// DefaultSession is the device_session_id used when an inbound
	// request payload omits one. Optional — if empty AND the payload
	// also omits device_session_id, Module.Handle emits a synchronous
	// failed terminal with reason="device_session_missing" rather than
	// guessing.
	DefaultSession devicetransit.DeviceSessionID

	// SessionStore is the daemon-side mirror table for device sessions.
	// Required: Module.Handle consults it to confirm the target session
	// is in a state that can route traffic before pushing a frame.
	SessionStore framework.SessionStore

	// Now is a clock injection point for tests. Defaults to time.Now.
	Now func() time.Time

	// NewExternalCorrelationID mints the id placed in Command.CorrelationID
	// when the adapter wants to hide envelope.id from the extension.
	// Empty defaults to envelope.id directly (M1.5 baseline:
	// device_transit.send.request_id carries the canonical id; no need
	// for a daemon-internal external id like M1.3 had — extension
	// echoes request_id back via device_transit.recv.request_id).
	NewExternalCorrelationID func(envelopeID string) string
}

// Module implements kernel/adapter.Module for the xhs device adapter.
// One instance per channel per daemon process.
type Module struct {
	cfg        Config
	mctx       *adapter.ModuleContext
	proxy      *framework.DeviceProxy
	sessions   framework.SessionStore
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
	if cfg.SessionStore == nil {
		return nil, errors.New("xhs.New: Config.SessionStore is required")
	}
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
	return &Module{
		cfg:         cfg,
		sessions:    cfg.SessionStore,
		now:         cfg.Now,
		externalID:  cfg.NewExternalCorrelationID,
		pendingType: map[string]string{},
	}, nil
}

// Declares returns the static adapter metadata. Called exactly once
// per Install per channel by the framework (L2 §8.1).
func (m *Module) Declares() adapter.Declaration {
	return adapter.Declaration{
		Name:         AdapterName,
		ActorID:      m.cfg.AdapterActorID,
		Types:        append([]string{}, AllTypes...),
		Binding:      Binding,
		MaxPendingMs: m.cfg.MaxPendingMs,
	}
}

// Init captures the framework-provided ModuleContext + constructs the
// DeviceProxy that wraps DeviceTransit + Correlation + ErrorPolicy.
// Returns an error if DeviceTransit is absent (the framework MUST
// inject it for via_server_transit bindings — covers codex warning
// #15).
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("xhs.Init: ModuleContext is nil")
	}
	if mctx.DeviceTransit == nil {
		return errors.New("xhs.Init: ModuleContext.DeviceTransit is nil; via_server_transit binding requires runtime/transit to wire it (T3)")
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

// Handle translates one inbound kind=request envelope into a
// device_transit.send frame. Synchronous failure paths emit a failed
// terminal immediately so the agent observes a clean reason without
// waiting on the F3 fallback timer.
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

	cmd, sid, _, err := buildCommand(env)
	if err != nil {
		return m.failNow(ctx, env.ID.String(), "", "payload_decode_failed", err.Error())
	}

	if newID := m.externalID(env.ID.String()); newID != "" {
		cmd.CorrelationID = newID
	}

	if sid == "" {
		sid = m.cfg.DefaultSession
	}
	if sid == "" {
		return m.failNow(ctx, env.ID.String(), "", "device_session_missing", "")
	}

	// Confirm the session is registered + reachable. The framework
	// state-machine guarantees `active` is the only routable state;
	// other states surface as device_offline / receiver_unavailable.
	sess, ok, err := m.sessions.Get(ctx, sid)
	if err != nil {
		return m.failNow(ctx, env.ID.String(), "", "session_store_unavailable", err.Error())
	}
	if !ok {
		return m.failNow(ctx, env.ID.String(), "", "device_session_unknown", string(sid))
	}
	if !sess.State.IsReachable() {
		return m.failNow(ctx, env.ID.String(), "", "device_offline", fmt.Sprintf("session state=%s", sess.State))
	}

	wirePayload, err := json.Marshal(cmd)
	if err != nil {
		return m.failNow(ctx, env.ID.String(), "", "command_marshal_failed", err.Error())
	}

	// Stash the requestType BEFORE Send so a callback that races the
	// proxy's bookkeeping still finds it. The stash is dropped on
	// terminal (success or cancel) inside completePending.
	m.setPendingType(env.ID.String(), env.Type)

	if _, err := m.proxy.SendRequest(ctx, env, sid, wirePayload); err != nil {
		// SendRequest already rolled back correlation + timer; clear our
		// local stash too.
		m.clearPendingType(env.ID.String())
		return m.failNow(ctx, env.ID.String(), "", "device_push_failed", err.Error())
	}
	return nil
}

// OnExternalCallback decodes a device_transit.recv payload + emits the
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
func (m *Module) failNow(ctx context.Context, requestID string, terminalReason message.TerminalFailureReason, reason, detail string) error {
	if m.mctx == nil {
		return fmt.Errorf("xhs.failNow: Init was not called (reason=%s)", reason)
	}
	payload, marshalErr := json.Marshal(map[string]any{"reason": reason})
	if marshalErr != nil {
		payload = []byte(`{}`)
	}
	opts := adapter.RespondOptions{Status: "failed", Reason: reason}

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
