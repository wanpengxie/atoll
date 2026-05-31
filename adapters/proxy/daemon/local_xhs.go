package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const DefaultXHSProxyActorID actor.ActorID = "tool:xhs"

type XHSLocalModule struct {
	actorID actor.ActorID
	clock   func() time.Time

	mu      sync.Mutex
	session *xhsLocalSession
	pending map[string]chan xhsLocalCallbackResult
}

type xhsLocalSession struct {
	ws     *websocket.Conn
	mu     sync.Mutex
	closed chan struct{}
	once   sync.Once
}

type xhsLocalCallbackResult struct {
	callback devicexhs.Callback
	err      error
}

func NewXHSLocalModule() *XHSLocalModule {
	return &XHSLocalModule{
		actorID: DefaultXHSProxyActorID,
		clock:   time.Now,
		pending: map[string]chan xhsLocalCallbackResult{},
	}
}

func (m *XHSLocalModule) ActorID() actor.ActorID {
	if m.actorID == "" {
		return DefaultXHSProxyActorID
	}
	return m.actorID
}

func (m *XHSLocalModule) Declaration() adapter.Declaration {
	return devicexhs.ContractDeclaration(m.ActorID(), devicexhs.DefaultMaxPendingMs)
}

func (m *XHSLocalModule) Init(_ context.Context, cfg actorapi.ModuleConfig) error {
	if len(cfg.Raw) > 0 {
		var parsed struct {
			ActorID actor.ActorID `json:"actor_id,omitempty"`
		}
		if err := json.Unmarshal(cfg.Raw, &parsed); err != nil {
			return fmt.Errorf("xhs proxy local module config: %w", err)
		}
		if parsed.ActorID != "" {
			m.actorID = parsed.ActorID
		}
	}
	if m.clock == nil {
		m.clock = time.Now
	}
	if m.pending == nil {
		m.pending = map[string]chan xhsLocalCallbackResult{}
	}
	return nil
}

func (m *XHSLocalModule) Shutdown(context.Context) error {
	m.mu.Lock()
	sess := m.session
	m.session = nil
	m.mu.Unlock()
	if sess != nil {
		sess.close()
	}
	m.failPending(errors.New("xhs extension disconnected"))
	return nil
}

func (m *XHSLocalModule) OnUpstreamAck(_ context.Context, frame DeviceFrame) error {
	sess := m.currentSession()
	if sess == nil {
		return nil
	}
	frame.Direction = "to_device"
	frame.FrameType = FrameTypeAck
	if frame.ActorID == "" {
		frame.ActorID = string(m.ActorID())
	}
	return sess.writeJSON(frame)
}

func (m *XHSLocalModule) Readiness(context.Context) (bool, string, error) {
	if m.currentSession() == nil {
		return false, "extension_disconnected", nil
	}
	return true, "ok", nil
}

func (m *XHSLocalModule) AcceptLocalWebSocket(ctx context.Context, conn *websocket.Conn) error {
	sess := &xhsLocalSession{ws: conn, closed: make(chan struct{})}
	m.mu.Lock()
	prev := m.session
	m.session = sess
	m.mu.Unlock()
	if prev != nil {
		prev.close()
	}
	go func() {
		<-ctx.Done()
		sess.close()
	}()
	defer func() {
		m.mu.Lock()
		if m.session == sess {
			m.session = nil
		}
		m.mu.Unlock()
		sess.close()
		m.failPending(errors.New("xhs extension disconnected"))
	}()

	for {
		var frame DeviceFrame
		if err := conn.ReadJSON(&frame); err != nil {
			return err
		}
		if frame.Direction != "from_device" {
			continue
		}
		cb, err := decodeXHSCallback(frame)
		if err != nil {
			continue
		}
		m.deliverCallback(cb)
	}
}

func (m *XHSLocalModule) Handle(ctx context.Context, env message.Envelope) (message.Envelope, error) {
	switch env.Type {
	case "actor.describe":
		return m.completedResponse(env, mustJSON(map[string]any{
			"actor_id":    string(m.ActorID()),
			"name":        devicexhs.AdapterName,
			"description": m.Declaration().Description,
			"skill_doc":   m.Declaration().SkillDoc,
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
			"checked_at": m.clock().UnixMilli(),
		})), nil
	}

	sess := m.currentSession()
	if sess == nil {
		return failedResponse(m.clock, env, m.ActorID(), message.TerminalReceiverUnavailable, "device_offline", "xhs extension is not connected to the local proxy daemon"), nil
	}
	cmd, err := xhsCommandFromEnvelope(env)
	if err != nil {
		return failedResponse(m.clock, env, m.ActorID(), message.TerminalReceiverInternalError, "payload_decode_failed", err.Error()), nil
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		return message.Envelope{}, err
	}
	corr := env.ID.String()
	wait := m.addPending(corr)
	defer m.removePending(corr)

	out := DeviceFrame{
		Direction:     "to_device",
		ActorID:       string(m.ActorID()),
		ChannelID:     string(env.ChannelID),
		RequestID:     corr,
		CorrelationID: corr,
		Payload:       raw,
	}
	if env.ExpiresAt != nil {
		out.ExpiresAt = *env.ExpiresAt
	}
	if err := sess.writeJSON(out); err != nil {
		return failedResponse(m.clock, env, m.ActorID(), message.TerminalReceiverUnavailable, "device_push_failed", err.Error()), nil
	}

	select {
	case got := <-wait:
		if got.err != nil {
			return failedResponse(m.clock, env, m.ActorID(), message.TerminalReceiverUnavailable, "device_offline", got.err.Error()), nil
		}
		return m.responseFromCallback(env, got.callback), nil
	case <-ctx.Done():
		return failedResponse(m.clock, env, m.ActorID(), message.TerminalReceiverUnavailable, "callback_timeout", ctx.Err().Error()), nil
	}
}

func (m *XHSLocalModule) currentSession() *xhsLocalSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.session == nil {
		return nil
	}
	select {
	case <-m.session.closed:
		return nil
	default:
		return m.session
	}
}

func (m *XHSLocalModule) addPending(id string) chan xhsLocalCallbackResult {
	ch := make(chan xhsLocalCallbackResult, 1)
	m.mu.Lock()
	m.pending[id] = ch
	m.mu.Unlock()
	return ch
}

func (m *XHSLocalModule) removePending(id string) {
	m.mu.Lock()
	delete(m.pending, id)
	m.mu.Unlock()
}

func (m *XHSLocalModule) deliverCallback(cb devicexhs.Callback) {
	m.mu.Lock()
	ch := m.pending[cb.CorrelationID]
	m.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- xhsLocalCallbackResult{callback: cb}:
	default:
	}
}

func (m *XHSLocalModule) failPending(err error) {
	m.mu.Lock()
	pending := m.pending
	m.pending = map[string]chan xhsLocalCallbackResult{}
	m.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- xhsLocalCallbackResult{err: err}:
		default:
		}
	}
}

func (m *XHSLocalModule) completedResponse(req message.Envelope, payload json.RawMessage) message.Envelope {
	return responseEnvelopeFromPayload(m.clock, req, m.ActorID(), payloadWithStatus(payload, "completed", ""))
}

func (m *XHSLocalModule) responseFromCallback(req message.Envelope, cb devicexhs.Callback) message.Envelope {
	status := strings.ToLower(strings.TrimSpace(cb.Status))
	switch status {
	case "ok", "completed", "success":
		body := map[string]any{"status": "completed"}
		for k, v := range cb.Result {
			body[k] = v
		}
		if cb.DeviceID != "" && req.Type == devicexhs.TypePublish {
			body["device_id"] = cb.DeviceID
		}
		return responseEnvelopeFromPayload(m.clock, req, m.ActorID(), mustJSON(body))
	case "error", "failed", "failure":
		body := map[string]any{
			"status":     "failed",
			"reason":     string(message.TerminalReceiverInternalError),
			"error_code": xhsCallbackErrorCode(cb.ErrorObj),
			"detail":     xhsCallbackErrorDetail(cb.ErrorObj),
		}
		for k, v := range cb.ErrorObj {
			if _, exists := body[k]; !exists {
				body[k] = v
			}
		}
		return responseEnvelopeFromPayload(m.clock, req, m.ActorID(), mustJSON(body))
	default:
		return failedResponse(m.clock, req, m.ActorID(), message.TerminalReceiverInternalError, "callback_status_unknown", "xhs extension returned an unknown callback status")
	}
}

func (s *xhsLocalSession) writeJSON(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return errors.New("local xhs websocket closed")
	default:
	}
	_ = s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.ws.WriteJSON(v)
}

func (s *xhsLocalSession) close() {
	s.once.Do(func() {
		close(s.closed)
		_ = s.ws.Close()
	})
}

func decodeXHSCallback(frame DeviceFrame) (devicexhs.Callback, error) {
	var cb devicexhs.Callback
	if len(frame.Payload) == 0 {
		return cb, errors.New("xhs callback payload required")
	}
	if err := json.Unmarshal(frame.Payload, &cb); err != nil {
		return cb, err
	}
	cb.CorrelationID = strings.TrimSpace(cb.CorrelationID)
	if cb.CorrelationID == "" {
		cb.CorrelationID = strings.TrimSpace(frame.CorrelationID)
	}
	if cb.CorrelationID == "" {
		cb.CorrelationID = strings.TrimSpace(frame.RequestID)
	}
	if cb.CorrelationID == "" {
		return cb, errors.New("xhs callback correlation_id required")
	}
	return cb, nil
}

func xhsCommandFromEnvelope(env message.Envelope) (devicexhs.Command, error) {
	if !strings.HasPrefix(env.Type, "xhs.") {
		return devicexhs.Command{}, fmt.Errorf("type %q lacks xhs. prefix", env.Type)
	}
	params := map[string]any{}
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &params); err != nil {
			return devicexhs.Command{}, err
		}
	}
	return devicexhs.Command{
		Type:          devicexhs.CommandWireType,
		CorrelationID: env.ID.String(),
		Cmd:           devicexhs.WireCmdFor(env.Type),
		Params:        params,
	}, nil
}

func responseEnvelopeFromPayload(clock func() time.Time, req message.Envelope, sender actor.ActorID, payload json.RawMessage) message.Envelope {
	hash, err := message.CanonicalHashPayload(payload)
	if err != nil {
		hash = fmt.Sprintf("%d", clock().UnixNano())
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}
	visibility := req.Visibility
	if visibility == "" {
		visibility = message.VisibilityPublic
	}
	now := clock().UnixMilli()
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

func payloadWithStatus(raw json.RawMessage, status, reason string) json.RawMessage {
	body := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	body["status"] = status
	if reason != "" {
		body["reason"] = reason
	}
	return mustJSON(body)
}

func xhsCallbackErrorCode(err map[string]any) string {
	for _, key := range []string{"code", "reason"} {
		if v, ok := err[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return "callback_failed"
}

func xhsCallbackErrorDetail(err map[string]any) string {
	for _, key := range []string{"message", "detail"} {
		if v, ok := err[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return xhsCallbackErrorCode(err)
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
