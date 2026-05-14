package transit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
)

// WSClientConfig wires a WSClient — the production daemon-side transit
// Transport that speaks gorilla/websocket against
// `server/daemonbus.HandleWS`.
//
// The server side dials are auth'd via two query params:
//
//	?daemon_id=<id>&key=<shared-secret>
//
// `host` and `version` are optional metadata the server records but
// does not validate (kept for ops triage).
type WSClientConfig struct {
	// URL is the daemonbus WS endpoint, e.g.
	// "ws://localhost:8080/api/daemonbus". The trailing query string
	// is appended by Connect — callers MUST NOT include it.
	URL string

	// DaemonID is the daemon identity the server uses to key the
	// connection registry. Required.
	DaemonID string

	// Key is the shared secret matching server.daemonbus.cfg.SharedSecret.
	// Required.
	Key string

	// Host / Version are optional metadata fields the server stores in
	// daemon_registry but does not validate.
	Host    string
	Version string

	// Dialer overrides the websocket dialer (tests inject a dialer
	// pointing at httptest.Server.URL). Defaults to
	// websocket.DefaultDialer when nil.
	Dialer *websocket.Dialer

	// HandshakeTimeout caps the first-frame read waiting for the
	// `control.connection_accepted` echo. Defaults to 5s.
	HandshakeTimeout time.Duration
}

// WSClient is a gorilla/websocket Transport implementation suitable
// for cmd/daemon production wiring. It satisfies the transit.Transport
// interface.
//
// Goroutine safety:
//   - Send may be invoked concurrently — internal mutex serializes the
//     underlying WriteMessage call (gorilla websocket forbids concurrent
//     writers).
//   - Recv MUST be invoked from a single reader goroutine (gorilla also
//     forbids concurrent readers). The transit.Dispatcher.Loop is the
//     canonical caller.
//   - Connect and Close serialize on a separate mutex protecting the
//     underlying *websocket.Conn pointer; Connect tears down any
//     existing connection before dialing a fresh one (reconnect path).
type WSClient struct {
	cfg WSClientConfig

	connMu sync.Mutex
	conn   *websocket.Conn

	sendMu sync.Mutex
}

// NewWSClient builds a WSClient. Connect MUST be called before Send /
// Recv to establish the underlying websocket.
func NewWSClient(cfg WSClientConfig) (*WSClient, error) {
	if cfg.URL == "" {
		return nil, errors.New("transit: WSClientConfig.URL empty")
	}
	if cfg.DaemonID == "" {
		return nil, errors.New("transit: WSClientConfig.DaemonID empty")
	}
	if cfg.Key == "" {
		return nil, errors.New("transit: WSClientConfig.Key empty")
	}
	if cfg.Dialer == nil {
		cfg.Dialer = websocket.DefaultDialer
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	return &WSClient{cfg: cfg}, nil
}

// Connect (re-)establishes the underlying websocket and reads the
// `control.connection_accepted` frame to learn the assigned
// connection_epoch. Closes any prior connection first so callers can
// drive reconnect by simply calling Connect again.
func (c *WSClient) Connect(ctx context.Context) (daemonbus.ConnectionEpoch, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	u, err := url.Parse(c.cfg.URL)
	if err != nil {
		return 0, fmt.Errorf("transit: parse URL: %w", err)
	}
	q := u.Query()
	q.Set("daemon_id", c.cfg.DaemonID)
	q.Set("key", c.cfg.Key)
	if c.cfg.Host != "" {
		q.Set("host", c.cfg.Host)
	}
	if c.cfg.Version != "" {
		q.Set("version", c.cfg.Version)
	}
	u.RawQuery = q.Encode()

	dialCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, c.cfg.HandshakeTimeout)
		defer cancel()
	}
	conn, _, err := c.cfg.Dialer.DialContext(dialCtx, u.String(), http.Header{})
	if err != nil {
		return 0, fmt.Errorf("transit: dial %s: %w", u.String(), err)
	}

	// Read the server-issued connection_accepted frame to learn epoch.
	deadline := time.Now().Add(c.cfg.HandshakeTimeout)
	_ = conn.SetReadDeadline(deadline)
	_, data, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return 0, fmt.Errorf("transit: read connection_accepted: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear deadline for normal Recv

	var frame daemonbus.Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		_ = conn.Close()
		return 0, fmt.Errorf("transit: parse connection_accepted: %w", err)
	}
	epoch, err := extractEpoch(frame)
	if err != nil {
		_ = conn.Close()
		return 0, err
	}
	c.conn = conn
	return epoch, nil
}

// Send marshals the frame as JSON text and writes it to the websocket.
// Returns an error when no connection is established or the websocket
// errors — caller can recover by calling Connect again.
func (c *WSClient) Send(ctx context.Context, frame daemonbus.Frame) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return errors.New("transit: ws not connected")
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("transit: marshal frame: %w", err)
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	// Best-effort ctx-deadline → write deadline.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	} else {
		_ = conn.SetWriteDeadline(time.Time{})
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("transit: ws write: %w", err)
	}
	return nil
}

// Recv blocks for the next text frame and parses it into daemonbus.Frame.
// Returns an error when the websocket is closed or the JSON parse fails.
func (c *WSClient) Recv(ctx context.Context) (daemonbus.Frame, error) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return daemonbus.Frame{}, errors.New("transit: ws not connected")
	}
	// gorilla/websocket has no ctx-aware ReadMessage; we approximate
	// cancellation by closing the conn on ctx.Done. Spawn a watchdog
	// only when the ctx has a deadline.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	} else {
		_ = conn.SetReadDeadline(time.Time{})
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-stop:
		}
	}()
	_, data, err := conn.ReadMessage()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return daemonbus.Frame{}, ctxErr
		}
		return daemonbus.Frame{}, fmt.Errorf("transit: ws read: %w", err)
	}
	var frame daemonbus.Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		return daemonbus.Frame{}, fmt.Errorf("transit: parse frame: %w", err)
	}
	return frame, nil
}

// Close shuts down the underlying websocket. Safe to call more than
// once.
func (c *WSClient) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// extractEpoch parses the `connection_accepted` payload — the demo-
// period informational frame the server sends right after upgrading.
// We accept both the typed payload and a generic map shape so the
// client survives the spec-vs-impl gap noted in
// server/daemonbus/ws_handler.go:93 (connection_accepted is NOT in the
// L2 §9.1 closed set).
func extractEpoch(frame daemonbus.Frame) (daemonbus.ConnectionEpoch, error) {
	if frame.DaemonConnectionEpoch != 0 {
		return frame.DaemonConnectionEpoch, nil
	}
	if len(frame.Payload) > 0 {
		var p struct {
			ConnectionEpoch int64 `json:"connection_epoch"`
		}
		if err := json.Unmarshal(frame.Payload, &p); err == nil && p.ConnectionEpoch != 0 {
			return daemonbus.ConnectionEpoch(p.ConnectionEpoch), nil
		}
	}
	return 0, fmt.Errorf("transit: connection_accepted frame missing epoch (frame_type=%s)",
		frame.FrameType)
}

// compile-time interface check
var _ Transport = (*WSClient)(nil)
