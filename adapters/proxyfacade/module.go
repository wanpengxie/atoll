// Package proxy_facade provides the cloud-daemon-side facade for actors
// hosted behind the proxy daemon v2 transport.
package proxyfacade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const DefaultMaxPendingMs int64 = 30_000

// liveState is the proxy facade's volatile, in-memory view of whether the
// user-machine device behind this relay is currently reachable. It is the
// "实时态/③" of channel-lifecycle-reconcile-architecture.md §2 — a strong
// over-expiring external signal, NEVER persisted, NEVER sticky. It is fed by
// the inbound devicebus lifecycle frames (connected / disconnected /
// token_expired) the framework routes through OnRuntimeEvent, and read by
// the StatusReporter so actor.status.available is realtime liveness — not a
// persisted readiness projection (§5 护栏 3).
type liveState string

const (
	liveUnknown      liveState = "unknown"
	liveOnline       liveState = "online"
	liveOffline      liveState = "offline"
	liveTokenExpired liveState = "token_expired"
)

type ProxyFacadeModule struct {
	decl behavior.Declaration
	mctx *behavior.ModuleContext

	// live holds the current liveState. A PLAIN field, not atomic: every
	// access — OnRuntimeEvent (write, folded onto the cell via Runtime.Post),
	// Handle and Status (read, dispatched on the same cell) — runs on the
	// adapter actor's single cell goroutine, so the mailbox IS the
	// serialization (construction-spec §3 state-home). Volatile by
	// construction: reconstructed from live lifecycle frames after a restart,
	// never written to actor_registry.
	live liveState

	// liveCheckedAt is the unix-ms timestamp of the last lifecycle frame that
	// moved `live`. Surfaced as actor.status `checked_at`. Plain field, same
	// cell-serial rationale as `live`.
	liveCheckedAt int64

	now func() time.Time
}

type CapabilitySet struct {
	Name             string                             `json:"name,omitempty"`
	Description      string                             `json:"description,omitempty"`
	SkillDoc         string                             `json:"skill_doc,omitempty"`
	Types            []string                           `json:"types,omitempty"`
	TypeDeclarations map[string]behavior.TypeDeclaration `json:"type_declarations,omitempty"`
	MaxPendingMs     int64                              `json:"max_pending_ms,omitempty"`
}

type readinessChangedPayload struct {
	ActorID   actor.ActorID `json:"actor_id"`
	ChangedAt int64         `json:"changed_at"`
	Current   struct {
		Ready             bool            `json:"ready"`
		Reason            string          `json:"reason"`
		Detail            json.RawMessage `json:"detail"`
		LastReadyAt       int64           `json:"last_ready_at"`
		LastStateChangeAt int64           `json:"last_state_change_at"`
	} `json:"current"`
}

func New(decl behavior.Declaration) (*ProxyFacadeModule, error) {
	decl = normalizeDeclaration(decl)
	if err := validateDeclaration(decl); err != nil {
		return nil, err
	}
	m := &ProxyFacadeModule{decl: decl, now: time.Now, live: liveUnknown}
	return m, nil
}

func DeclarationFromCapability(actorID actor.ActorID, capability json.RawMessage) (behavior.Declaration, error) {
	var cap CapabilitySet
	if len(capability) > 0 {
		if err := json.Unmarshal(capability, &cap); err != nil {
			return behavior.Declaration{}, fmt.Errorf("proxy_facade: decode capability_set: %w", err)
		}
	}
	name := cap.Name
	if name == "" {
		name = strings.TrimPrefix(string(actorID), "tool:")
	}
	return normalizeDeclaration(behavior.Declaration{
		Name:             name,
		ActorID:          actorID,
		Types:            cap.Types,
		TypeDeclarations: cap.TypeDeclarations,
		Binding:          actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs:     cap.MaxPendingMs,
		Description:      cap.Description,
		SkillDoc:         cap.SkillDoc,
	}), nil
}

func (m *ProxyFacadeModule) Declares() behavior.Declaration {
	return m.decl
}

func (m *ProxyFacadeModule) SuppressInitialReadiness() bool { return true }

func (m *ProxyFacadeModule) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	if mctx == nil {
		return errors.New("proxy_facade: ModuleContext required")
	}
	if mctx.ForwardExternalRequest == nil {
		return errors.New("proxy_facade: ForwardExternalRequest required")
	}
	if mctx.Resolve == nil {
		return errors.New("proxy_facade: Resolve required; the framework sync/async refactor routes final callbacks through the receiver-side Resolve path")
	}
	if mctx.Provisional == nil {
		return errors.New("proxy_facade: Provisional required; provisional callbacks route through the framework provisional emit helper")
	}
	if mctx.UpdateReadiness == nil {
		return errors.New("proxy_facade: UpdateReadiness required")
	}
	m.mctx = mctx
	return nil
}

func (m *ProxyFacadeModule) Shutdown(context.Context) error { return nil }

// liveStateNow reads the current volatile liveness. Cell-serial: only the
// adapter's own goroutine touches m.live.
func (m *ProxyFacadeModule) liveStateNow() liveState {
	if m.live == "" {
		return liveUnknown
	}
	return m.live
}

// OnRuntimeEvent implements behavior.RuntimeEventAware. The framework routes
// devicebus connection lifecycle frames (connected / disconnected /
// token_expired) here for the (channel, adapter_actor_id) this facade owns.
// We fold them into the volatile `live` signal so StatusReporter reports
// realtime reachability. This is the ③实时态 input of the reconcile model —
// it is intentionally NOT persisted and NOT pushed into actor_registry
// readiness (that path remains driven by the relayed actor.readiness.changed
// event so the daemon-side projection still reflects the upstream actor's
// own readiness; liveness is a separate over-expiring transport signal).
func (m *ProxyFacadeModule) OnRuntimeEvent(_ context.Context, evt behavior.RuntimeEvent) error {
	if evt.Kind != devicetransit.RuntimeEventKindDeviceLifecycle {
		return nil
	}
	lifecycle, err := devicetransit.DecodeLifecycleRuntimeEventPayload(evt.Payload)
	if err != nil {
		return err
	}
	next, ok := mapLifecycleToLive(lifecycle.Event)
	if !ok {
		return nil
	}
	m.live = next
	ts := lifecycle.Ts
	if ts == 0 {
		ts = m.nowMs()
	}
	m.liveCheckedAt = ts
	return nil
}

func mapLifecycleToLive(e devicetransit.LifecycleEvent) (liveState, bool) {
	switch e {
	case devicetransit.LifecycleConnected:
		return liveOnline, true
	case devicetransit.LifecycleDisconnected:
		return liveOffline, true
	case devicetransit.LifecycleTokenExpired:
		return liveTokenExpired, true
	}
	return "", false
}

// Status implements behavior.StatusReporter. actor.status.available is the
// realtime liveness of the relay transport (§5 护栏 3) — derived purely from
// the volatile lifecycle signal, never from a persisted readiness column.
// online ⇒ available; offline / token_expired / unknown ⇒ not available, with
// a reason the UI can act on (re-bind vs reconnect).
func (m *ProxyFacadeModule) Status(_ context.Context) (behavior.StatusReport, error) {
	state := m.liveStateNow()
	checkedAt := m.liveCheckedAt
	if checkedAt == 0 {
		checkedAt = m.nowMs()
	}
	report := behavior.StatusReport{
		Available: state == liveOnline,
		CheckedAt: time.UnixMilli(checkedAt),
		Detail: map[string]any{
			"live_state": string(state),
		},
	}
	switch state {
	case liveOnline:
		report.Reason = "ok"
	case liveTokenExpired:
		report.Reason = "token_expired"
	case liveOffline:
		report.Reason = "device_offline"
	default:
		report.Reason = "device_unreachable"
	}
	return report, nil
}

func (m *ProxyFacadeModule) nowMs() int64 {
	if m.now != nil {
		return m.now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (m *ProxyFacadeModule) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil || m.mctx.ForwardExternalRequest == nil {
		return errors.New("proxy_facade: not initialized")
	}
	if env == nil {
		return errors.New("proxy_facade: nil envelope")
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("proxy_facade: marshal envelope: %w", err)
	}
	_, err = m.mctx.ForwardExternalRequest(ctx, env, behavior.ExternalRequestPayload(raw))
	if err != nil {
		return fmt.Errorf("proxy_facade: send device transit: %w", err)
	}
	return nil
}

func (m *ProxyFacadeModule) OnExternalCallback(ctx context.Context, payload []byte) error {
	if m.mctx == nil {
		return errors.New("proxy_facade: not initialized")
	}
	var env message.Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return fmt.Errorf("proxy_facade: decode callback envelope: %w", err)
	}
	if env.ID == "" {
		return errors.New("proxy_facade: callback envelope id required")
	}
	if env.Type == "actor.readiness.changed" {
		return m.updateReadinessFromEvent(ctx, &env)
	}
	// Response envelopes are polymorphic per proto-foundation §1.6.3:
	// payload.status ∈ {completed, failed} are final and close the
	// correlation; any other status is provisional and must NOT touch
	// the pending registry / F3 timer. The framework sync/async refactor
	// (§5.3) makes the closed-loop decision a single router center: this
	// adapter only classifies the inbound callback and routes it —
	//   final       → ctx.Resolve (receiver-side terminal),
	//   provisional  → ctx.Provisional (interim, no closure).
	// It no longer judges/closes the correlation itself.
	if env.Kind != message.KindResponse {
		return fmt.Errorf("proxy_facade: callback kind=%s (must be response or actor.readiness.changed event)", env.Kind)
	}
	status, statusErr := extractPayloadStatus(env.Payload)
	if statusErr != nil {
		return fmt.Errorf("proxy_facade: decode callback payload.status: %w", statusErr)
	}
	if !message.IsFinalStatus(status) {
		return m.handleProvisionalCallback(ctx, &env, status)
	}
	return m.handleFinalCallback(ctx, &env, status)
}

// handleFinalCallback routes a final response envelope from the proxy
// daemon transport through ModuleContext.Resolve (framework sync/async
// spec §5.3). The request envelope id (= callback parent_id) is the wait
// anchor; ctx.Resolve constructs the adapter-signed final, writes it
// through the harness, and closes the pending correlation + F3 timer via
// the router's single lifecycle center. The adapter no longer constructs
// the terminal envelope or judges closure — it supplies only the
// receiver-side status / payload / reason.
func (m *ProxyFacadeModule) handleFinalCallback(ctx context.Context, env *message.Envelope, status string) error {
	if m.mctx.Resolve == nil {
		return errors.New("proxy_facade: Resolve helper unavailable; final response callback cannot be routed")
	}
	if env.ParentID == "" {
		return errors.New("proxy_facade: final callback parent_id required")
	}
	// F6: restore the lightweight consistency checks the old
	// CompleteExternalResponse enforced. An un-owned reqID is already rejected
	// downstream (Resolve fails with no pending entry), but a callback that
	// CLAIMS a channel / sender / correlation inconsistent with the pending
	// request must be refused here — otherwise a misrouted or spoofed callback
	// could resolve the wrong request. These are the few original checks, not a
	// broader policy: channel, sender, and correlation against the pending entry.
	if env.ChannelID == "" {
		return errors.New("proxy_facade: final callback channel_id required")
	}
	if env.ChannelID != m.mctx.ChannelID {
		return fmt.Errorf("proxy_facade: final channel mismatch: callback=%s manager=%s", env.ChannelID, m.mctx.ChannelID)
	}
	if err := m.validateCallbackSender("final", env); err != nil {
		return err
	}
	if env.CorrelationID == "" {
		return errors.New("proxy_facade: final correlation_id required")
	}
	if m.mctx.LookupPendingRequest != nil {
		entry, ok, lookErr := m.mctx.LookupPendingRequest(ctx, behavior.CorrelationKey(env.ParentID))
		if lookErr != nil {
			return fmt.Errorf("proxy_facade: final callback pending lookup: %w", lookErr)
		}
		if ok {
			if entry.ChannelID != "" && env.ChannelID != entry.ChannelID {
				return fmt.Errorf("proxy_facade: final channel mismatch vs pending: callback=%s pending=%s", env.ChannelID, entry.ChannelID)
			}
			expectedCorr := entry.CorrelationID
			if expectedCorr == "" {
				expectedCorr = entry.ParentID
			}
			if env.CorrelationID != expectedCorr {
				return fmt.Errorf("proxy_facade: final correlation mismatch: callback=%s pending=%s", env.CorrelationID, expectedCorr)
			}
		}
	}
	// Strip the protocol status/reason out of the inbound payload — the
	// framework Resolve path re-injects them onto the canonical final
	// envelope it constructs (mergeResponsePayload). Passing them through
	// ResolveRequest keeps the typed status/reason as the single source.
	body, reason, err := payloadWithoutStatusAndReason(env.Payload)
	if err != nil {
		return fmt.Errorf("proxy_facade: prepare final payload: %w", err)
	}
	if err := m.mctx.Resolve(ctx, env.ParentID, behavior.ResolveRequest{
		Status:  status,
		Payload: body,
		Reason:  reason,
	}); err != nil {
		return fmt.Errorf("proxy_facade: resolve final callback: %w", err)
	}
	return nil
}

// handleProvisionalCallback routes a provisional response envelope from
// the proxy daemon transport through ModuleContext.Provisional so the
// framework builds a fresh response envelope without touching the
// pending correlation entry / F3 timer. The original envelope's
// sender / audience / type are reused via the request lookup inside
// Provisional; we only forward the parent_id (= request id), the
// status, and the user-supplied payload fields.
func (m *ProxyFacadeModule) handleProvisionalCallback(ctx context.Context, env *message.Envelope, status string) error {
	if m.mctx.Provisional == nil {
		return errors.New("proxy_facade: Provisional helper unavailable; provisional response callback cannot be routed")
	}
	if env.ParentID == "" {
		return errors.New("proxy_facade: provisional callback parent_id required")
	}
	if env.ChannelID == "" {
		return errors.New("proxy_facade: provisional callback channel_id required")
	}
	if env.ChannelID != m.mctx.ChannelID {
		return fmt.Errorf("proxy_facade: provisional channel mismatch: callback=%s manager=%s", env.ChannelID, m.mctx.ChannelID)
	}
	if err := m.validateCallbackSender("provisional", env); err != nil {
		return err
	}
	// The framework Provisional helper re-merges status onto the payload;
	// strip it from the inbound copy so we don't pass duplicate fields.
	userFields, err := payloadWithoutStatus(env.Payload)
	if err != nil {
		return fmt.Errorf("proxy_facade: prepare provisional payload: %w", err)
	}
	_, err = m.mctx.Provisional(
		ctx,
		behavior.CorrelationKey(env.ParentID),
		status,
		userFields,
		behavior.ProvisionalOptions{
			Visibility: env.Visibility,
			Audience:   env.Audience,
		},
	)
	if err != nil {
		return fmt.Errorf("proxy_facade: emit provisional: %w", err)
	}
	return nil
}

func (m *ProxyFacadeModule) validateCallbackSender(label string, env *message.Envelope) error {
	expectedKind := m.mctx.AdapterActorKind
	if expectedKind == "" {
		expectedKind = actor.KindTool
	}
	if env.Sender.ID != m.mctx.AdapterActorID || env.Sender.Kind != expectedKind {
		return fmt.Errorf("proxy_facade: %s sender mismatch: callback=(%s,%s) adapter=(%s,%s)",
			label, env.Sender.Kind, env.Sender.ID, expectedKind, m.mctx.AdapterActorID)
	}
	return nil
}

// extractPayloadStatus pulls payload.status out of a response envelope
// payload without imposing a typed schema on the rest of the payload.
// Empty payload / missing status both return "" which IsFinalStatus
// reports as non-final — for the proxy_facade callback path that means
// we treat a missing status as provisional. The harness will still
// reject the chain write if the status is malformed.
func extractPayloadStatus(payload json.RawMessage) (string, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return "", nil
	}
	var fields struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload, &fields); err != nil {
		return "", err
	}
	return fields.Status, nil
}

// payloadWithoutStatus returns the payload object with the top-level
// "status" key removed. Used before handing the payload to
// ModuleContext.Provisional which re-injects the status itself.
func payloadWithoutStatus(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return json.RawMessage(`{}`), nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if fields == nil {
		return json.RawMessage(`{}`), nil
	}
	delete(fields, "status")
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// payloadWithoutStatusAndReason returns the payload object with the
// top-level "status" and "reason" keys removed, plus the extracted
// reason string. Used before handing the payload to ModuleContext.Resolve
// which carries status/reason as typed ResolveRequest fields and
// re-injects them onto the canonical final envelope it constructs.
func payloadWithoutStatusAndReason(payload json.RawMessage) (json.RawMessage, string, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return json.RawMessage(`{}`), "", nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, "", fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if fields == nil {
		return json.RawMessage(`{}`), "", nil
	}
	var reason string
	if raw, ok := fields["reason"]; ok {
		// reason is optional; tolerate a non-string value by leaving it empty.
		_ = json.Unmarshal(raw, &reason)
	}
	delete(fields, "status")
	delete(fields, "reason")
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, "", err
	}
	return out, reason, nil
}

func (m *ProxyFacadeModule) updateReadinessFromEvent(ctx context.Context, env *message.Envelope) error {
	if env.Type != "actor.readiness.changed" {
		return fmt.Errorf("proxy_facade: readiness callback type=%s (must be actor.readiness.changed)", env.Type)
	}
	if env.Kind != message.KindEvent {
		return fmt.Errorf("proxy_facade: readiness callback kind=%s (must be event)", env.Kind)
	}
	if env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID {
		return fmt.Errorf("proxy_facade: readiness sender mismatch: sender=(%s,%s)", env.Sender.Kind, env.Sender.ID)
	}
	if env.ChannelID == "" {
		return errors.New("proxy_facade: readiness channel_id required")
	}
	if env.ChannelID != m.mctx.ChannelID {
		return fmt.Errorf("proxy_facade: readiness channel mismatch: event=%s manager=%s", env.ChannelID, m.mctx.ChannelID)
	}
	var payload readinessChangedPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("proxy_facade: decode readiness event: %w", err)
	}
	actorID := payload.ActorID
	if actorID == "" {
		return errors.New("proxy_facade: readiness payload actor_id required")
	}
	if actorID != m.decl.ActorID {
		return fmt.Errorf("proxy_facade: readiness actor mismatch: payload=%s facade=%s", actorID, m.decl.ActorID)
	}
	state := actor.ReadinessNotReady
	if payload.Current.Ready {
		state = actor.ReadinessReady
	}
	checkedAt := payload.ChangedAt
	if checkedAt == 0 {
		checkedAt = payload.Current.LastStateChangeAt
	}
	if checkedAt == 0 && state == actor.ReadinessReady {
		checkedAt = payload.Current.LastReadyAt
	}
	if checkedAt == 0 {
		return errors.New("proxy_facade: readiness event missing changed_at")
	}
	if len(payload.Current.Detail) == 0 {
		payload.Current.Detail = json.RawMessage(`{}`)
	}
	if _, err := m.mctx.UpdateReadiness(ctx, actor.ReadinessUpdate{
		State:     state,
		Reason:    payload.Current.Reason,
		Detail:    payload.Current.Detail,
		CheckedAt: checkedAt,
	}); err != nil {
		return fmt.Errorf("proxy_facade: update readiness: %w", err)
	}
	return nil
}

func normalizeDeclaration(decl behavior.Declaration) behavior.Declaration {
	decl.Name = strings.TrimSpace(decl.Name)
	if decl.Name == "" && decl.ActorID != "" {
		decl.Name = strings.TrimPrefix(string(decl.ActorID), "tool:")
	}
	if decl.Binding == "" {
		decl.Binding = actor.BindingRuntimeInboundViaRelay
	}
	if decl.MaxPendingMs <= 0 {
		decl.MaxPendingMs = DefaultMaxPendingMs
	}
	return decl
}

func validateDeclaration(decl behavior.Declaration) error {
	if decl.Name == "" {
		return errors.New("proxy_facade: declaration name required")
	}
	if decl.ActorID == "" {
		return errors.New("proxy_facade: actor_id required")
	}
	if decl.Binding != actor.BindingRuntimeInboundViaRelay {
		return fmt.Errorf("proxy_facade: binding must be %s", actor.BindingRuntimeInboundViaRelay)
	}
	if len(decl.Types) == 0 {
		return errors.New("proxy_facade: at least one type required")
	}
	return nil
}

var (
	_ behavior.Module            = (*ProxyFacadeModule)(nil)
	_ behavior.RuntimeEventAware = (*ProxyFacadeModule)(nil)
	_ behavior.StatusReporter    = (*ProxyFacadeModule)(nil)
)
