package coagentsdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	defaultHTTPTimeout = 30 * time.Second
	defaultCallTimeout = 30 * time.Second
	sessionCookieName  = "coagent_session"
)

var subscribeSettleDelay = 50 * time.Millisecond

// Client is a minimal Go SDK client for calling an actor inside a channel.
type Client struct {
	BaseURL      string
	SessionToken string
	HTTPClient   *http.Client
}

type CallActorRequest struct {
	ChannelID string          `json:"channel_id"`
	ActorID   string          `json:"actor_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timeout   time.Duration   `json:"-"`
}

type CallActorResult struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *CallActorError `json:"error,omitempty"`
	Raw   json.RawMessage `json:"-"`
}

type CallActorError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RecoveryHint string `json:"recovery_hint,omitempty"`
}

type ActorInfo struct {
	ActorID           string          `json:"actor_id"`
	Kind              string          `json:"kind,omitempty"`
	Binding           string          `json:"binding,omitempty"`
	DisplayName       string          `json:"display_name,omitempty"`
	Ready             bool            `json:"ready"`
	ReadyReason       string          `json:"ready_reason,omitempty"`
	ReadyDetail       json.RawMessage `json:"ready_detail,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
	Types             []ActorTypeInfo `json:"types,omitempty"`
}

type ActorTypeInfo struct {
	Type           string   `json:"type"`
	AllowedKinds   []string `json:"allowed_kinds,omitempty"`
	HandlerBinding string   `json:"handler_binding,omitempty"`
	MaxPendingMs   int64    `json:"max_pending_ms,omitempty"`
}

type ActorStatusResult struct {
	Available         bool            `json:"available"`
	Reason            string          `json:"reason,omitempty"`
	Kind              string          `json:"kind,omitempty"`
	Binding           string          `json:"binding,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
	Detail            json.RawMessage `json:"detail,omitempty"`
	CheckedAt         int64           `json:"checked_at,omitempty"`
	Raw               json.RawMessage `json:"-"`
}

type actorListResponse struct {
	ChannelID string      `json:"channel_id"`
	Actors    []ActorInfo `json:"actors"`
}

type emitRequest struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
	Audience []string        `json:"audience"`
}

type emitAck struct {
	MessageID    string `json:"message_id"`
	Accepted     bool   `json:"accepted"`
	RejectReason string `json:"reject_reason"`
	RejectDetail string `json:"reject_detail"`
}

type cursorResponse struct {
	LastReceivedSeq int64 `json:"last_received_seq"`
}

type wsPushFrame struct {
	Type      string          `json:"type"`
	ChannelID string          `json:"channel_id"`
	Seq       int64           `json:"seq"`
	Envelope  json.RawMessage `json:"envelope"`
}

// CallActor emits a kind=request envelope to req.ActorID and waits for the
// matching kind=response envelope on the channel push WebSocket.
func (c *Client) CallActor(ctx context.Context, req CallActorRequest) (*CallActorResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateCallActorRequest(req); err != nil {
		return nil, err
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	hc := c.httpClient()

	cursor, err := c.fetchCursor(ctx, hc, baseURL, req.ChannelID)
	if err != nil {
		return nil, err
	}

	wsURL, err := websocketURL(baseURL)
	if err != nil {
		return nil, err
	}
	ws, _, err := c.dialWebSocket(ctx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("coagentsdk: websocket connect: %w", err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(map[string]any{
		"type":       "subscribe",
		"channel_id": req.ChannelID,
		"since_seq":  cursor,
	}); err != nil {
		return nil, fmt.Errorf("coagentsdk: websocket subscribe: %w", err)
	}
	if err := waitSubscribeSettle(ctx); err != nil {
		return timeoutResult(req, timeout), nil
	}

	requestID, err := newRequestID()
	if err != nil {
		return nil, err
	}
	ack, err := c.emitRequest(ctx, hc, baseURL, req, requestID)
	if err != nil {
		return nil, err
	}
	matchIDs := map[string]struct{}{requestID: {}}
	if ack.MessageID != "" {
		matchIDs[ack.MessageID] = struct{}{}
	}

	return c.waitResponse(ctx, ws, req, matchIDs, timeout)
}

func (c *Client) ListActors(ctx context.Context, channelID string) ([]ActorInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(channelID) == "" {
		return nil, fmt.Errorf("coagentsdk: channel_id is required")
	}
	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	var out actorListResponse
	if err := c.doJSON(ctx, c.httpClient(), http.MethodGet, baseURL+"/api/channels/"+url.PathEscape(channelID)+"/actors", nil, &out); err != nil {
		return nil, fmt.Errorf("coagentsdk: list actors: %w", err)
	}
	return out.Actors, nil
}

func (c *Client) ActorStatus(ctx context.Context, channelID, actorID string) (*ActorStatusResult, error) {
	res, err := c.CallActor(ctx, CallActorRequest{
		ChannelID: channelID,
		ActorID:   actorID,
		Type:      "actor.status",
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("coagentsdk: actor.status returned nil result")
	}
	if !res.OK {
		if res.Error != nil {
			return nil, fmt.Errorf("coagentsdk: actor.status failed: %s: %s", res.Error.Code, res.Error.Message)
		}
		return nil, fmt.Errorf("coagentsdk: actor.status failed")
	}
	var payload struct {
		Status            string          `json:"status"`
		Available         bool            `json:"available"`
		Reason            string          `json:"reason"`
		Kind              string          `json:"kind"`
		Binding           string          `json:"binding"`
		LastReadyAt       int64           `json:"last_ready_at"`
		LastStateChangeAt int64           `json:"last_state_change_at"`
		Detail            json.RawMessage `json:"detail"`
		CheckedAt         int64           `json:"checked_at"`
	}
	if err := json.Unmarshal(res.Raw, &payload); err != nil {
		return nil, fmt.Errorf("coagentsdk: decode actor.status: %w", err)
	}
	return &ActorStatusResult{
		Available:         payload.Available,
		Reason:            payload.Reason,
		Kind:              payload.Kind,
		Binding:           payload.Binding,
		LastReadyAt:       payload.LastReadyAt,
		LastStateChangeAt: payload.LastStateChangeAt,
		Detail:            append(json.RawMessage(nil), payload.Detail...),
		CheckedAt:         payload.CheckedAt,
		Raw:               append(json.RawMessage(nil), res.Raw...),
	}, nil
}

func validateCallActorRequest(req CallActorRequest) error {
	switch {
	case strings.TrimSpace(req.ChannelID) == "":
		return fmt.Errorf("coagentsdk: channel_id is required")
	case strings.TrimSpace(req.ActorID) == "":
		return fmt.Errorf("coagentsdk: actor_id is required")
	case strings.TrimSpace(req.Type) == "":
		return fmt.Errorf("coagentsdk: type is required")
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return fmt.Errorf("coagentsdk: payload must be valid JSON")
	}
	return nil
}

func normalizeBaseURL(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("coagentsdk: base URL is required")
	}
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("coagentsdk: parse base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("coagentsdk: base URL scheme must be http or https")
	}
	if u.Host == "" {
		return "", fmt.Errorf("coagentsdk: base URL host is required")
	}
	return raw, nil
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

func (c *Client) fetchCursor(ctx context.Context, hc *http.Client, baseURL, channelID string) (int64, error) {
	var out cursorResponse
	if err := c.doJSON(ctx, hc, http.MethodGet, baseURL+"/api/channels/"+url.PathEscape(channelID)+"/cursor", nil, &out); err != nil {
		return 0, fmt.Errorf("coagentsdk: fetch cursor: %w", err)
	}
	return out.LastReceivedSeq, nil
}

func (c *Client) emitRequest(ctx context.Context, hc *http.Client, baseURL string, req CallActorRequest, requestID string) (emitAck, error) {
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	body := emitRequest{
		ID:       requestID,
		Type:     req.Type,
		Kind:     string(message.KindRequest),
		Payload:  payload,
		Audience: []string{req.ActorID},
	}
	var ack emitAck
	if err := c.doJSON(ctx, hc, http.MethodPost, baseURL+"/api/channels/"+url.PathEscape(req.ChannelID)+"/messages", body, &ack); err != nil {
		return emitAck{}, fmt.Errorf("coagentsdk: emit request: %w", err)
	}
	if ack.RejectReason != "" {
		return emitAck{}, fmt.Errorf("coagentsdk: emit rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}
	return ack, nil
}

func (c *Client) doJSON(ctx context.Context, hc *http.Client, method, endpoint string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	c.applySession(httpReq.Header)

	resp, err := hc.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Body: string(raw)}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) applySession(header http.Header) {
	if c.SessionToken == "" {
		return
	}
	header.Set("Cookie", (&http.Cookie{
		Name:  sessionCookieName,
		Value: c.SessionToken,
	}).String())
}

func websocketURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/ws"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (c *Client) dialWebSocket(ctx context.Context, wsURL string) (*websocket.Conn, *http.Response, error) {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	header := http.Header{}
	c.applySession(header)
	return dialer.DialContext(ctx, wsURL, header)
}

func waitSubscribeSettle(ctx context.Context) error {
	if subscribeSettleDelay <= 0 {
		return nil
	}
	timer := time.NewTimer(subscribeSettleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) waitResponse(ctx context.Context, ws *websocket.Conn, req CallActorRequest, matchIDs map[string]struct{}, timeout time.Duration) (*CallActorResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return timeoutResult(req, timeout), nil
		}
		_ = ws.SetReadDeadline(time.Now().Add(nextReadWindow(ctx)))
		mt, raw, err := ws.ReadMessage()
		if err != nil {
			if isTimeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return timeoutResult(req, timeout), nil
			}
			return nil, fmt.Errorf("coagentsdk: websocket read: %w", err)
		}
		if mt != websocket.TextMessage {
			continue
		}
		var frame wsPushFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			continue
		}
		if frame.Type != "message" || frame.ChannelID != req.ChannelID || len(frame.Envelope) == 0 {
			continue
		}
		var env message.Envelope
		if err := json.Unmarshal(frame.Envelope, &env); err != nil {
			continue
		}
		if env.Kind != message.KindResponse {
			continue
		}
		if _, ok := matchIDs[env.ParentID.String()]; !ok {
			continue
		}
		return resultFromResponse(env)
	}
}

func nextReadWindow(ctx context.Context) time.Duration {
	window := 500 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return time.Nanosecond
		}
		if remaining < window {
			return remaining
		}
	}
	return window
}

func isTimeout(err error) bool {
	var netErr net.Error
	return err != nil && (strings.Contains(err.Error(), "i/o timeout") || (errors.As(err, &netErr) && netErr.Timeout()))
}

func resultFromResponse(env message.Envelope) (*CallActorResult, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(env.Payload, &obj); err != nil {
		return nil, fmt.Errorf("coagentsdk: response payload must be a JSON object: %w", err)
	}
	status := rawString(obj["status"])
	switch status {
	case "completed":
		data := removePayloadFields(obj, "status", "reason")
		return &CallActorResult{
			OK:   true,
			Data: data,
			Raw:  append(json.RawMessage(nil), env.Payload...),
		}, nil
	case "failed":
		code := firstNonEmpty(rawString(obj["error_code"]), rawString(obj["reason"]), "failed")
		msg := firstNonEmpty(rawString(obj["detail"]), rawString(obj["message"]))
		return &CallActorResult{
			OK: false,
			Error: &CallActorError{
				Code:         code,
				Message:      msg,
				RecoveryHint: rawString(obj["recovery_hint"]),
			},
			Raw: append(json.RawMessage(nil), env.Payload...),
		}, nil
	default:
		return nil, fmt.Errorf("coagentsdk: response payload status=%q", status)
	}
}

func removePayloadFields(obj map[string]json.RawMessage, keys ...string) json.RawMessage {
	cp := make(map[string]json.RawMessage, len(obj))
	for k, v := range obj {
		cp[k] = v
	}
	for _, k := range keys {
		delete(cp, k)
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}

func timeoutResult(req CallActorRequest, timeout time.Duration) *CallActorResult {
	return &CallActorResult{
		OK: false,
		Error: &CallActorError{
			Code:    "timeout",
			Message: fmt.Sprintf("timed out waiting %s for response to %s on channel %s", timeout, req.Type, req.ChannelID),
		},
	}
}

func newRequestID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("coagentsdk: generate request id: %w", err)
	}
	return "req-" + hex.EncodeToString(buf[:]), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// HTTPError reports a non-2xx server response.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http status %d: %s", e.StatusCode, truncate(e.Body, 500))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
