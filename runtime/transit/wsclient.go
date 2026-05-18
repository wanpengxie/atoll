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
	"github.com/rs/zerolog"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
)

// DefaultWSWriteTimeout caps an individual WriteMessage call. Without
// this floor a stuck TCP send buffer (cloudflare tunnel half-open, lost
// FIN, etc.) deadlocks the write goroutine on the underlying syscall
// while still holding `sendMu` — and every subsequent Send (heartbeat,
// write_message ack reply, ...) blocks behind it, silently bricking the
// daemon. 10s is the deliberate ceiling: heartbeat cadence is 15s so a
// failing write surfaces inside one heartbeat interval; SendAndAwait
// callers (server gateway) typically run with their own 30s+ ctx
// deadline so this lower bound dominates.
const DefaultWSWriteTimeout = 10 * time.Second

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
	// "ws://localhost:8832/api/daemonbus". The trailing query string
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

	// WriteTimeout is the maximum time a single WriteMessage call may
	// block before being abandoned. Defaults to DefaultWSWriteTimeout.
	// MUST be > 0 in production: without an effective deadline a TCP
	// half-open stall blocks the write goroutine indefinitely while
	// holding sendMu, freezing every subsequent Send (heartbeat,
	// write_message ack reply, ...). A timed-out write returns an error
	// and marks the underlying conn dead so the caller can reconnect.
	WriteTimeout time.Duration

	// Logger receives connection lifecycle + transport error events. May
	// be nil — when nil a no-op zerolog.Nop() is used.
	Logger *zerolog.Logger
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
	log zerolog.Logger

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
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWSWriteTimeout
	}
	log := zerolog.Nop()
	if cfg.Logger != nil {
		log = *cfg.Logger
	}
	return &WSClient{cfg: cfg, log: log}, nil
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
		c.log.Warn().Str("event", "transit.ws.dial_failed").
			Str("url", u.String()).
			Err(err).
			Msg("daemonbus dial failed")
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
	c.log.Info().Str("event", "transit.ws.connected").
		Int64("connection_epoch", int64(epoch)).
		Str("daemon_id", c.cfg.DaemonID).
		Msg("daemonbus websocket connected")
	return epoch, nil
}

// markDead closes the current conn (if any) and clears the pointer so
// the next Send/Recv returns "not connected" — letting the supervisor /
// caller drive a reconnect. Safe to call from any goroutine; idempotent.
func (c *WSClient) markDead(reason string, err error) {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return
	}
	_ = conn.Close()
	c.log.Warn().Str("event", "transit.ws.conn_dead").
		Str("reason", reason).
		Err(err).
		Str("daemon_id", c.cfg.DaemonID).
		Msg("daemonbus websocket marked dead")
}

// Send marshals the frame as JSON text and writes it to the websocket.
// Returns an error when no connection is established or the websocket
// errors — caller can recover by calling Connect again.
//
// A bounded write deadline is ALWAYS applied (min of ctx.Deadline and
// cfg.WriteTimeout). Without this floor a stuck TCP write
// (cloudflare-tunnel half-open, network partition with no RST) blocks
// the call indefinitely while still holding sendMu, starving every
// other Send (heartbeat, server→daemon ack reply). On any write error
// (including i/o timeout) the underlying conn is closed and the pointer
// cleared so the next Send returns "not connected" — making the
// failure observable to the dispatcher / supervisor that drives
// reconnect.
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

	// Always cap the write with a deadline. Use the earlier of
	// ctx.Deadline (if any) and now+WriteTimeout so a ctx that has its
	// own tighter deadline still wins, but a ctx with no deadline (the
	// HeartbeatSender / dispatcher runCtx case — the exact path that
	// deadlocked in the production incident) gets WriteTimeout as a
	// hard floor instead of `time.Time{}` (= no deadline at all).
	deadline := time.Now().Add(c.cfg.WriteTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetWriteDeadline(deadline)

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		// Treat ANY write error as fatal for this connection. gorilla
		// websocket does not surface "transient" write errors that are
		// safe to retry on the same conn — a write deadline / closed
		// pipe / framing error all leave the conn in an undefined
		// state. Close + nil so Send sees "not connected" next time.
		c.markDead("write_error", err)
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
			// Caller is shutting down; do NOT mark dead — close is
			// the supervisor's job.
			return daemonbus.Frame{}, ctxErr
		}
		// A read error means the conn is gone (peer closed, framing
		// error, deadline). Close it and clear the pointer so the
		// dispatcher Loop's caller can reconnect cleanly.
		c.markDead("read_error", err)
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
	c.log.Info().Str("event", "transit.ws.closed").
		Str("daemon_id", c.cfg.DaemonID).
		Msg("daemonbus websocket closed")
	return err
}

// IsConnected reports whether the WS conn pointer is currently non-nil.
// Used by reconnect supervisors to decide whether to dial.
func (c *WSClient) IsConnected() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
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
