package kimi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type Config struct {
	ActorID        actor.ActorID `json:"actor_id,omitempty"`
	BaseURL        string        `json:"base_url,omitempty"`
	TimeoutMs      int64         `json:"timeout_ms,omitempty"`
	DefaultSession string        `json:"default_session,omitempty"`
}

type Module struct {
	cfg        Config
	httpClient *http.Client
	baseURL    *url.URL
}

type CommandResponse struct {
	OK    *bool           `json:"ok,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *CommandError   `json:"error,omitempty"`
}

type CommandError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type StatusResponse struct {
	Running            bool   `json:"running"`
	Version            string `json:"version,omitempty"`
	Port               int    `json:"port,omitempty"`
	UptimeSeconds      int64  `json:"uptime_seconds,omitempty"`
	ExtensionConnected bool   `json:"extension_connected"`
	ExtensionID        string `json:"extension_id,omitempty"`
	ExtensionVersion   string `json:"extension_version,omitempty"`
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

func (m *Module) Init(_ context.Context, cfg actorapi.ModuleConfig) error {
	parsed := Config{}
	if len(cfg.Raw) > 0 {
		if err := json.Unmarshal(cfg.Raw, &parsed); err != nil {
			return fmt.Errorf("kimi proxy module config: %w", err)
		}
	}
	if parsed.ActorID == "" {
		parsed.ActorID = DefaultAdapterActorID
	}
	if parsed.BaseURL == "" {
		parsed.BaseURL = DefaultBaseURL
	}
	if parsed.TimeoutMs <= 0 {
		parsed.TimeoutMs = DefaultMaxPendingMs
	}
	u, err := url.Parse(strings.TrimRight(parsed.BaseURL, "/"))
	if err != nil {
		return fmt.Errorf("kimi proxy module base_url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("kimi proxy module base_url scheme %q unsupported", u.Scheme)
	}
	m.cfg = parsed
	m.baseURL = u
	m.httpClient = &http.Client{Timeout: time.Duration(parsed.TimeoutMs) * time.Millisecond}
	return nil
}

func (m *Module) Shutdown(context.Context) error { return nil }

func (m *Module) Readiness(ctx context.Context) (bool, string, error) {
	if m.httpClient == nil || m.baseURL == nil {
		return false, "initializing", nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.endpoint("/status"), nil)
	if err != nil {
		return false, "probe_error", err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return false, "daemon_unreachable", nil
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return false, "daemon_unreachable", nil
	}
	var status StatusResponse
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if len(bytes.TrimSpace(raw)) == 0 {
		return true, "ok", nil
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return false, "probe_error", err
	}
	if !status.Running {
		return false, "daemon_unreachable", nil
	}
	if !status.ExtensionConnected {
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
			"available": ready,
			"reason":    reason,
			"kind":      string(actor.KindTool),
			"binding":   string(actor.BindingRuntimeInboundViaRelay),
			"detail": map[string]any{
				"base_url": m.cfg.BaseURL,
			},
			"checked_at": time.Now().UnixMilli(),
		})), nil
	case TypeCommand:
	default:
		return m.failedResponse(env, message.TerminalReceiverInternalError, "unknown_type",
			fmt.Sprintf("kimi proxy module does not handle type %q", env.Type)), nil
	}
	if m.httpClient == nil || m.baseURL == nil {
		return m.failedResponse(env, message.TerminalReceiverUnavailable, "daemon_unreachable", "kimi module not initialized"), nil
	}
	body, err := normalizeCommandPayload(env.Payload, m.cfg.DefaultSession)
	if err != nil {
		return m.failedResponse(env, message.TerminalReceiverInternalError, "payload_decode_failed", err.Error()), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint("/command"), bytes.NewReader(body))
	if err != nil {
		return message.Envelope{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return m.failedResponse(env, message.TerminalReceiverUnavailable, "daemon_unreachable", err.Error()), nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return message.Envelope{}, err
	}
	if resp.StatusCode >= 400 {
		code, detail := errorFromCommandBody(raw)
		if code == "" {
			code = "daemon_call_failed"
		}
		if detail == "" {
			detail = fmt.Sprintf("kimi-webbridge HTTP %d", resp.StatusCode)
		}
		return m.failedResponse(env, message.TerminalReceiverUnavailable, code, detail), nil
	}
	return m.responseFromCommandBody(env, raw)
}

func (m *Module) responseFromCommandBody(req message.Envelope, raw json.RawMessage) (message.Envelope, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return m.completedResponse(req, json.RawMessage(`{}`)), nil
	}
	var wrapped CommandResponse
	if err := json.Unmarshal(raw, &wrapped); err == nil && (wrapped.OK != nil || wrapped.Error != nil || len(wrapped.Data) > 0) {
		if wrapped.OK != nil && !*wrapped.OK {
			code := "tool_failed"
			detail := "kimi-webbridge command failed"
			if wrapped.Error != nil {
				if wrapped.Error.Code != "" {
					code = wrapped.Error.Code
				}
				if wrapped.Error.Message != "" {
					detail = wrapped.Error.Message
				}
			}
			return m.failedResponse(req, message.TerminalReceiverInternalError, code, detail), nil
		}
		data := wrapped.Data
		if len(data) == 0 {
			data = json.RawMessage(`{}`)
		}
		return m.completedResponse(req, ensureJSONObject(data)), nil
	}
	return m.completedResponse(req, ensureJSONObject(raw)), nil
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
	return message.Envelope{
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
}

func normalizeCommandPayload(raw json.RawMessage, defaultSession string) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("payload is required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if len(fields) == 0 {
		return nil, errors.New("payload must include action")
	}
	if _, ok := fields["action"]; !ok {
		return nil, errors.New("payload.action is required")
	}
	if defaultSession != "" {
		if _, ok := fields["session"]; !ok {
			session, _ := json.Marshal(defaultSession)
			fields["session"] = session
			return json.Marshal(fields)
		}
	}
	return raw, nil
}

func errorFromCommandBody(raw json.RawMessage) (string, string) {
	var wrapped CommandResponse
	if err := json.Unmarshal(raw, &wrapped); err != nil || wrapped.Error == nil {
		return "", snippet(raw)
	}
	return wrapped.Error.Code, wrapped.Error.Message
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

func (m *Module) endpoint(path string) string {
	u := *m.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String()
}

func (m *Module) maxPendingMs() int64 {
	if m.cfg.TimeoutMs > 0 {
		return m.cfg.TimeoutMs
	}
	return DefaultMaxPendingMs
}

func snippet(raw []byte) string {
	const max = 256
	if len(raw) <= max {
		return string(raw)
	}
	return string(raw[:max]) + "...(truncated)"
}

var _ actorapi.ActorModule = (*Module)(nil)
