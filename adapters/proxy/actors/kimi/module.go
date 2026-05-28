package kimi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type Config struct {
	ActorID        actor.ActorID `json:"actor_id,omitempty"`
	TimeoutMs      int64         `json:"timeout_ms,omitempty"`
	DefaultSession string        `json:"default_session,omitempty"`
}

type Module struct {
	cfg    Config
	server *Server

	serverStartPort       int
	serverFallbackPort    int
	serverPingInterval    time.Duration
	serverMissedPongLimit int
}

func New() *Module {
	return &Module{}
}

func (m *Module) ActorID() actor.ActorID {
	if m.cfg.ActorID != "" {
		return m.cfg.ActorID
	}
	return DefaultAdapterActorID
}

func (m *Module) Declaration() adapter.Declaration {
	return Declaration(m.ActorID(), m.maxPendingMs())
}

func (m *Module) Init(ctx context.Context, cfg actorapi.ModuleConfig) error {
	parsed := Config{}
	if len(cfg.Raw) > 0 {
		if err := json.Unmarshal(cfg.Raw, &parsed); err != nil {
			return fmt.Errorf("kimi proxy module config: %w", err)
		}
	}
	if parsed.ActorID == "" {
		parsed.ActorID = DefaultAdapterActorID
	}
	if parsed.TimeoutMs <= 0 {
		parsed.TimeoutMs = DefaultMaxPendingMs
	}
	if m.server != nil {
		_ = m.server.Shutdown(context.Background())
		m.server = nil
	}
	server, err := StartServer(ctx, serverOptions{
		Port:            m.listenStartPort(),
		FallbackPort:    m.listenFallbackPort(),
		PingInterval:    m.serverPingInterval,
		MissedPongLimit: m.serverMissedPongLimit,
	})
	if err != nil {
		return fmt.Errorf("kimi proxy module ws server: %w", err)
	}
	m.cfg = parsed
	m.server = server
	log.Printf("kimi proxy module ws server listening: %s", server.Addr())
	return nil
}

func (m *Module) Shutdown(ctx context.Context) error {
	if m.server == nil {
		return nil
	}
	err := m.server.Shutdown(ctx)
	m.server = nil
	return err
}

func (m *Module) Readiness(context.Context) (bool, string, error) {
	if m.server == nil {
		return false, "initializing", nil
	}
	if !m.server.HasConnectedExtension() {
		return false, "extension_disconnected", nil
	}
	return true, "ok", nil
}

func (m *Module) Handle(ctx context.Context, env message.Envelope) (message.Envelope, error) {
	switch env.Type {
	case "actor.describe":
		return m.completedResponse(env, mustJSON(map[string]any{
			"actor_id":    string(m.ActorID()),
			"name":        AdapterName,
			"description": actorDescription,
			"skill_doc":   actorSkillDoc,
			"types":       adapter.DeclarationCatalogFromDeclaration(m.Declaration()).Types,
			"binding":     string(actor.BindingRuntimeInboundViaRelay),
		})), nil
	case "actor.status":
		ready, reason, err := m.Readiness(ctx)
		if err != nil {
			return message.Envelope{}, err
		}
		return m.completedResponse(env, mustJSON(map[string]any{
			"available":  ready,
			"reason":     reason,
			"kind":       string(actor.KindTool),
			"binding":    string(actor.BindingRuntimeInboundViaRelay),
			"detail":     m.statusDetail(),
			"checked_at": time.Now().UnixMilli(),
		})), nil
	case TypeCommand:
	default:
		return m.failedResponse(env, message.TerminalReceiverInternalError, "unknown_type",
			fmt.Sprintf("kimi proxy module does not handle type %q", env.Type)), nil
	}
	if m.server == nil {
		return m.failedResponse(env, message.TerminalReceiverUnavailable, "daemon_unreachable", "kimi embedded ws server is not initialized"), nil
	}
	name, args, err := normalizeCommandPayload(env.Payload, m.cfg.DefaultSession)
	if err != nil {
		return m.failedResponse(env, message.TerminalReceiverInternalError, "payload_decode_failed", err.Error()), nil
	}
	callCtx, cancel := m.callContext(ctx)
	defer cancel()
	raw, err := m.server.CallTool(callCtx, name, args)
	if err != nil {
		return m.failedResponseForCallError(env, err), nil
	}
	return m.completedResponse(env, ensureJSONObject(raw)), nil
}

func (m *Module) completedResponse(req message.Envelope, payload json.RawMessage) message.Envelope {
	return responseEnvelope(time.Now, req, m.ActorID(), payloadWithStatus(payload, "completed", ""))
}

func (m *Module) failedResponse(req message.Envelope, reason message.TerminalFailureReason, code, detail string) message.Envelope {
	payload, _ := json.Marshal(map[string]any{
		"error_code": code,
		"detail":     detail,
	})
	return responseEnvelope(time.Now, req, m.ActorID(), payloadWithStatus(payload, "failed", string(reason)))
}

func (m *Module) failedResponseForCallError(req message.Envelope, err error) message.Envelope {
	var toolErr *ToolError
	switch {
	case errors.Is(err, ErrExtensionDisconnected):
		return m.failedResponse(req, message.TerminalReceiverUnavailable, "extension_disconnected", err.Error())
	case errors.Is(err, ErrServerClosed):
		return m.failedResponse(req, message.TerminalReceiverUnavailable, "daemon_unreachable", err.Error())
	case errors.Is(err, ErrUnknownTool):
		return m.failedResponse(req, message.TerminalReceiverInternalError, "payload_decode_failed", err.Error())
	case errors.As(err, &toolErr):
		return m.failedResponse(req, message.TerminalReceiverInternalError, "tool_failed", toolErr.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return m.failedResponse(req, message.TerminalReceiverUnavailable, "daemon_call_failed", err.Error())
	default:
		return m.failedResponse(req, message.TerminalReceiverUnavailable, "daemon_call_failed", err.Error())
	}
}

func responseEnvelope(clock func() time.Time, req message.Envelope, sender actor.ActorID, payload json.RawMessage) message.Envelope {
	hash, err := message.CanonicalHashPayload(payload)
	if err != nil {
		hash = fmt.Sprintf("%d", clock().UnixNano())
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}
	now := clock().UnixMilli()
	visibility := req.Visibility
	if visibility == "" {
		visibility = message.VisibilityPublic
	}
	resp := message.Envelope{
		ID:            message.ID("response:" + req.ID.String() + ":" + hash),
		TS:            now,
		ChannelID:     req.ChannelID,
		Sender:        message.Sender{Kind: actor.KindTool, ID: sender},
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    visibility,
		Audience:      message.Audience{req.Sender.ID},
	}
	if req.ExpiresAt != nil {
		exp := *req.ExpiresAt
		resp.ExpiresAt = &exp
	}
	return resp
}

func normalizeCommandPayload(raw json.RawMessage, defaultSession string) (string, json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", nil, errors.New("payload is required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if len(fields) == 0 {
		return "", nil, errors.New("payload must include action")
	}
	name, err := stringField(fields, "action", true)
	if err != nil {
		return "", nil, err
	}
	name = strings.TrimSpace(name)
	if !isKimiToolName(name) {
		return "", nil, fmt.Errorf("payload.action %q is not a supported Kimi WebBridge tool", name)
	}

	args := map[string]json.RawMessage{}
	if rawArgs, ok := fields["args"]; ok && len(bytes.TrimSpace(rawArgs)) > 0 && string(bytes.TrimSpace(rawArgs)) != "null" {
		if err := json.Unmarshal(rawArgs, &args); err != nil || args == nil {
			if err == nil {
				err = errors.New("payload.args must be a JSON object")
			}
			return "", nil, fmt.Errorf("payload.args must be a JSON object: %w", err)
		}
	}
	session := strings.TrimSpace(defaultSession)
	if _, ok := fields["session"]; ok {
		session, err = stringField(fields, "session", false)
		if err != nil {
			return "", nil, err
		}
		session = strings.TrimSpace(session)
	}
	if session != "" {
		if _, exists := args["_session"]; !exists {
			rawSession, _ := json.Marshal(session)
			args["_session"] = rawSession
		}
	}
	out, err := json.Marshal(args)
	if err != nil {
		return "", nil, err
	}
	return name, out, nil
}

func stringField(fields map[string]json.RawMessage, name string, required bool) (string, error) {
	raw, ok := fields[name]
	if !ok {
		if required {
			return "", fmt.Errorf("payload.%s is required", name)
		}
		return "", nil
	}
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		if required {
			return "", fmt.Errorf("payload.%s is required", name)
		}
		return "", nil
	}
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("payload.%s must be a string: %w", name, err)
	}
	if required && strings.TrimSpace(out) == "" {
		return "", fmt.Errorf("payload.%s is required", name)
	}
	return out, nil
}

func ensureJSONObject(raw json.RawMessage) json.RawMessage {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil && obj != nil {
		return raw
	}
	wrapped, _ := json.Marshal(map[string]any{"data": json.RawMessage(raw)})
	return wrapped
}

func payloadWithStatus(raw json.RawMessage, status, reason string) json.RawMessage {
	fields := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, &fields)
	}
	fields["status"] = status
	if reason != "" {
		fields["reason"] = reason
	}
	out, _ := json.Marshal(fields)
	return out
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func (m *Module) statusDetail() map[string]any {
	detail := map[string]any{
		"listen_addr":         "",
		"port":                0,
		"extension_connected": false,
		"extension_version":   "",
	}
	if m.server == nil {
		return detail
	}
	detail["listen_addr"] = m.server.Addr()
	detail["port"] = m.server.Port()
	detail["extension_connected"] = m.server.HasConnectedExtension()
	detail["extension_version"] = m.server.ExtensionVersion()
	return detail
}

func (m *Module) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Duration(m.maxPendingMs())*time.Millisecond)
}

func (m *Module) listenStartPort() int {
	if m.serverStartPort > 0 {
		return m.serverStartPort
	}
	return DefaultWSPort
}

func (m *Module) listenFallbackPort() int {
	if m.serverFallbackPort > 0 {
		return m.serverFallbackPort
	}
	return DefaultWSFallbackPort
}

func (m *Module) maxPendingMs() int64 {
	if m.cfg.TimeoutMs > 0 {
		return m.cfg.TimeoutMs
	}
	return DefaultMaxPendingMs
}

var _ actorapi.ActorModule = (*Module)(nil)
