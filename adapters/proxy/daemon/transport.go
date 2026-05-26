package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSConnection struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func Dial(ctx context.Context, serverWS, apiKey string, dialer *websocket.Dialer) (*WSConnection, string, error) {
	endpoint, err := BuildDevicebusURL(serverWS, apiKey)
	if err != nil {
		return nil, "", err
	}
	base := *websocket.DefaultDialer
	if dialer != nil {
		base = *dialer
	}
	if base.HandshakeTimeout <= 0 {
		base.HandshakeTimeout = 10 * time.Second
	}
	base.Subprotocols = []string{WSSubprotocolV2}
	ws, resp, err := base.DialContext(ctx, endpoint, http.Header{})
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return nil, endpoint, fmt.Errorf("proxy transport dial status=%d: %w", status, err)
	}
	if got := ws.Subprotocol(); got != WSSubprotocolV2 {
		_ = ws.Close()
		return nil, endpoint, fmt.Errorf("proxy transport subprotocol %q, want %q", got, WSSubprotocolV2)
	}
	return &WSConnection{ws: ws}, endpoint, nil
}

func BuildDevicebusURL(serverWS, apiKey string) (string, error) {
	serverWS = strings.TrimSpace(serverWS)
	apiKey = strings.TrimSpace(apiKey)
	if serverWS == "" {
		return "", fmt.Errorf("proxy transport: server ws required")
	}
	if apiKey == "" {
		return "", fmt.Errorf("proxy transport: api key required")
	}
	if !strings.Contains(serverWS, "://") {
		serverWS = "ws://" + serverWS
	}
	u, err := url.Parse(serverWS)
	if err != nil {
		return "", fmt.Errorf("proxy transport parse server ws: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("proxy transport: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("proxy transport: server host required")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = WSPathV2
	}
	q := u.Query()
	q.Set(QueryParamAPIKey, apiKey)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *WSConnection) ReadFrame() (DeviceFrame, error) {
	_, raw, err := c.ws.ReadMessage()
	if err != nil {
		return DeviceFrame{}, err
	}
	var frame DeviceFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return DeviceFrame{}, err
	}
	return frame, nil
}

func (c *WSConnection) WriteFrame(ctx context.Context, frame DeviceFrame) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.ws.SetWriteDeadline(deadline)
	if err := c.ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		_ = c.ws.Close()
		return err
	}
	return nil
}

func (c *WSConnection) Close() error {
	return c.ws.Close()
}
