// Package proxy_facade provides the cloud-daemon-side facade for actors
// hosted behind the proxy daemon v2 transport.
package proxy_facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const DefaultMaxPendingMs int64 = 30_000

type ProxyFacadeModule struct {
	decl adapter.Declaration
	mctx *adapter.ModuleContext
}

type CapabilitySet struct {
	Name             string                             `json:"name,omitempty"`
	Description      string                             `json:"description,omitempty"`
	SkillDoc         string                             `json:"skill_doc,omitempty"`
	Types            []string                           `json:"types,omitempty"`
	TypeDeclarations map[string]adapter.TypeDeclaration `json:"type_declarations,omitempty"`
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

func New(decl adapter.Declaration) (*ProxyFacadeModule, error) {
	decl = normalizeDeclaration(decl)
	if err := validateDeclaration(decl); err != nil {
		return nil, err
	}
	return &ProxyFacadeModule{decl: decl}, nil
}

func DeclarationFromCapability(actorID actor.ActorID, capability json.RawMessage) (adapter.Declaration, error) {
	var cap CapabilitySet
	if len(capability) > 0 {
		if err := json.Unmarshal(capability, &cap); err != nil {
			return adapter.Declaration{}, fmt.Errorf("proxy_facade: decode capability_set: %w", err)
		}
	}
	name := cap.Name
	if name == "" {
		name = strings.TrimPrefix(string(actorID), "tool:")
	}
	return normalizeDeclaration(adapter.Declaration{
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

func (m *ProxyFacadeModule) Declares() adapter.Declaration {
	return m.decl
}

func (m *ProxyFacadeModule) SuppressInitialReadiness() bool { return true }

func (m *ProxyFacadeModule) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("proxy_facade: ModuleContext required")
	}
	if mctx.ForwardExternalRequest == nil {
		return errors.New("proxy_facade: ForwardExternalRequest required")
	}
	if mctx.CompleteExternalResponse == nil {
		return errors.New("proxy_facade: CompleteExternalResponse required")
	}
	if mctx.UpdateReadiness == nil {
		return errors.New("proxy_facade: UpdateReadiness required")
	}
	m.mctx = mctx
	return nil
}

func (m *ProxyFacadeModule) Shutdown(context.Context) error { return nil }

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
	_, err = m.mctx.ForwardExternalRequest(ctx, env, adapter.ExternalRequestPayload(raw))
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
	// the pending registry / F3 timer. Routing every response through
	// CompleteExternalResponse would prematurely fire CorrelationDone on
	// the first provisional and reject the trailing final as a
	// terminal_duplicate — breaking provisional streams from proxy-hosted
	// actors.
	if env.Kind == message.KindResponse {
		status, statusErr := extractPayloadStatus(env.Payload)
		if statusErr != nil {
			return fmt.Errorf("proxy_facade: decode callback payload.status: %w", statusErr)
		}
		if !message.IsFinalStatus(status) {
			return m.handleProvisionalCallback(ctx, &env, status)
		}
	}
	_, err := m.mctx.CompleteExternalResponse(ctx, &env)
	return err
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
	if env.Sender.ID != m.mctx.AdapterActorID {
		return fmt.Errorf("proxy_facade: provisional sender mismatch: callback=%s adapter=%s", env.Sender.ID, m.mctx.AdapterActorID)
	}
	// The framework Provisional helper re-merges status onto the payload;
	// strip it from the inbound copy so we don't pass duplicate fields.
	userFields, err := payloadWithoutStatus(env.Payload)
	if err != nil {
		return fmt.Errorf("proxy_facade: prepare provisional payload: %w", err)
	}
	_, err = m.mctx.Provisional(
		ctx,
		adapter.CorrelationKey(env.ParentID),
		status,
		userFields,
		adapter.ProvisionalOptions{
			Visibility: env.Visibility,
			Audience:   env.Audience,
		},
	)
	if err != nil {
		return fmt.Errorf("proxy_facade: emit provisional: %w", err)
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
	state := actorreg.ReadinessNotReady
	if payload.Current.Ready {
		state = actorreg.ReadinessReady
	}
	checkedAt := payload.ChangedAt
	if checkedAt == 0 {
		checkedAt = payload.Current.LastStateChangeAt
	}
	if checkedAt == 0 && state == actorreg.ReadinessReady {
		checkedAt = payload.Current.LastReadyAt
	}
	if checkedAt == 0 {
		return errors.New("proxy_facade: readiness event missing changed_at")
	}
	if len(payload.Current.Detail) == 0 {
		payload.Current.Detail = json.RawMessage(`{}`)
	}
	if _, err := m.mctx.UpdateReadiness(ctx, actorreg.ReadinessUpdate{
		State:     state,
		Reason:    payload.Current.Reason,
		Detail:    payload.Current.Detail,
		CheckedAt: checkedAt,
	}); err != nil {
		return fmt.Errorf("proxy_facade: update readiness: %w", err)
	}
	return nil
}

func normalizeDeclaration(decl adapter.Declaration) adapter.Declaration {
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

func validateDeclaration(decl adapter.Declaration) error {
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

var _ adapter.Module = (*ProxyFacadeModule)(nil)
