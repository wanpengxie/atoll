package devicebus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

// DefaultDeviceWSWriteTimeout caps a single device-side WS write.
// Mirror of the daemonbus floor — see runtime/transit/wsclient.go for
// the rationale: a stuck send buffer would otherwise deadlock the
// write goroutine while holding wsDeviceTransport.mu.
const DefaultDeviceWSWriteTimeout = 10 * time.Second

// DefaultDevicePingCadence is how often the server pings the device.
// 30s gives ~3× margin against the cloudflare tunnel ~100s idle
// reaper. Pings use gorilla WriteControl (bypasses application-frame
// mutex) so a stuck app write cannot starve them.
const DefaultDevicePingCadence = 30 * time.Second

// DefaultDeviceIdleReadTimeout is the maximum time the server will
// block reading from a device WS before declaring it dead. ~2.3× the
// ping cadence absorbs one missed pong without false-positive
// teardown.
const DefaultDeviceIdleReadTimeout = 70 * time.Second

// DefaultDeviceWSReadLimit caps a single inbound devicebus frame at 4 MiB.
// Larger frames are closed before JSON allocation/validation.
const DefaultDeviceWSReadLimit int64 = 4 << 20

// DefaultDevicePingWriteTimeout caps a single ping write; on failure
// the conn is closed and the read loop unwedges.
const DefaultDevicePingWriteTimeout = 5 * time.Second

// Connection wraps one open device WS — sender of device_transit
// frames to the daemon (via daemonbus) + receiver of frames pushed
// back from the daemon.
type Connection struct {
	Session    Session
	transport  DeviceTransport
	Generation uint64

	closeOnce sync.Once
	closed    chan struct{}
}

// DeviceTransport is the wire-level interface — gorilla/websocket
// implementation lives below; tests use an in-memory pipe.
type DeviceTransport interface {
	ReadFrame(ctx context.Context) (DeviceFrame, error)
	WriteFrame(ctx context.Context, frame DeviceFrame) error
	Close() error
}

// DeviceFrame is the shared device_transit payload contract. The
// device WS and daemonbus mux carry the same JSON body; only the outer
// transport envelope differs.
type DeviceFrame = devicetransit.SendFrame

// NewConnection wires a transport.
func NewConnection(s Session, tx DeviceTransport) *Connection {
	return &Connection{
		Session:   s,
		transport: tx,
		closed:    make(chan struct{}),
	}
}

// SendToDevice writes one frame to the device — used by the daemon→
// device fan-out path.
func (c *Connection) SendToDevice(ctx context.Context, frame DeviceFrame) error {
	if c.IsClosed() {
		return errors.New("devicebus: connection closed")
	}
	return c.transport.WriteFrame(ctx, frame)
}

// IsClosed reports whether the connection has shut down.
func (c *Connection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// Close shuts down the transport.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.transport.Close()
	})
	return nil
}

// wsDeviceTransport adapts gorilla to DeviceTransport.
//
// Keepalive: ping/pong identical to server/daemonbus/wsTransport.
// Server pings the device every cadence via WriteControl; PongHandler
// + per-read refresh keeps the read deadline alive; an idle peer
// trips the deadline after idleReadTimeout and the conn closes.
type wsDeviceTransport struct {
	mu sync.Mutex
	ws *websocket.Conn

	idleReadTimeout time.Duration
	stopPing        chan struct{}
	stopOnce        sync.Once
}

func newWSDeviceTransport(ws *websocket.Conn) *wsDeviceTransport {
	return &wsDeviceTransport{
		ws:       ws,
		stopPing: make(chan struct{}),
	}
}

func (t *wsDeviceTransport) startPinger(cadence, pingWriteTimeout, idleReadTimeout time.Duration) {
	t.idleReadTimeout = idleReadTimeout
	ws := t.ws
	defaultPingHandler := ws.PingHandler()
	ws.SetPingHandler(func(appData string) error {
		_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))
		return defaultPingHandler(appData)
	})
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))
		return nil
	})
	_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))

	go func() {
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()
		for {
			select {
			case <-t.stopPing:
				return
			case <-ticker.C:
				err := ws.WriteControl(
					websocket.PingMessage,
					nil,
					time.Now().Add(pingWriteTimeout),
				)
				if err != nil {
					_ = ws.Close()
					return
				}
			}
		}
	}()
}

func (t *wsDeviceTransport) ReadFrame(ctx context.Context) (DeviceFrame, error) {
	_, data, err := t.ws.ReadMessage()
	if err != nil {
		return DeviceFrame{}, err
	}
	if t.idleReadTimeout > 0 {
		_ = t.ws.SetReadDeadline(time.Now().Add(t.idleReadTimeout))
	}
	var f DeviceFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return DeviceFrame{}, err
	}
	return f, nil
}
func (t *wsDeviceTransport) WriteFrame(ctx context.Context, f DeviceFrame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	// Always cap with a write deadline — same fix as daemonbus
	// wsTransport (avoids stuck send buffer blocking the mu).
	deadline := time.Now().Add(DefaultDeviceWSWriteTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = t.ws.SetWriteDeadline(deadline)
	if err := t.ws.WriteMessage(websocket.TextMessage, data); err != nil {
		_ = t.ws.Close()
		return err
	}
	return nil
}
func (t *wsDeviceTransport) Close() error {
	t.stopOnce.Do(func() {
		if t.stopPing != nil {
			close(t.stopPing)
		}
	})
	return t.ws.Close()
}

// TransitForwarder is implemented by the gateway — it knows how to
// reach the daemonbus connection for the channel + how to wrap a
// DeviceFrame into a `device_transit.send` daemonbus frame
// (impl-layer2 §5.3.1 inbound — device → server → daemon adapter).
type TransitForwarder interface {
	ForwardDeviceFrame(ctx context.Context, frame DeviceFrame) error
}

// DeviceWSSubprotocol is the real WebSocket subprotocol negotiated on
// the /devicebus handshake. Per impl-layer3 §6.5.1: the client offers
// `coagent.device.v1, token.<token>`; the server selects (and echoes
// back) ONLY `coagent.device.v1` — the `token.*` slot carries the
// bearer token and MUST NOT be reflected back to the client.
const DeviceWSSubprotocol = "coagent.device.v1"

// deviceWSTokenPrefix is the prefix used on the `token.*` pseudo-
// subprotocol slot to ferry the bearer token through `Sec-WebSocket-
// Protocol`. token is everything after the prefix.
const deviceWSTokenPrefix = "token."

// HandleWS upgrades the device socket. The gateway passes a
// TransitForwarder that knows how to route the frame to daemonbus.
//
// Handshake (impl-layer3 §6.5.1):
//
//   - URL:    /devicebus?session_id=<sid>
//   - Header: Sec-WebSocket-Protocol: coagent.device.v1, token.<token>
//
// The server parses the offered subprotocols, extracts the token from
// the `token.<token>` slot, validates (session_id, token), and on success
// upgrades. The durable session states accepted for this handshake are
// ready, active, and offline: ready is the first connect, active is a
// duplicate connect that replaces the previous live socket, and offline is
// reconnect after a drop. Pending and terminal states reject with
// session_not_ready / token_expired / session_revoked. The Upgrader selects
// `coagent.device.v1` as the negotiated subprotocol (RFC 6455 requires the
// server to echo the chosen subprotocol in the handshake response). The
// `token.*` slot is deliberately NOT echoed back.
func (s *Service) HandleWS(forwarder TransitForwarder) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID := c.Query("session_id")
		token, hasRealProto, parseOK := parseDeviceWSSubprotocols(c.Request.Header.Values("Sec-WebSocket-Protocol"))
		if !parseOK || sessionID == "" || token == "" || !hasRealProto {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "session_id query + Sec-WebSocket-Protocol: coagent.device.v1, token.<token> required",
			})
			return
		}
		row, err := s.ValidateToken(c.Request.Context(), sessionID, token)
		if err != nil {
			status := http.StatusUnauthorized
			if errors.Is(err, ErrSessionNotReady) {
				status = http.StatusConflict
			}
			c.JSON(status, gin.H{
				"error":         err.Error(),
				"reject_reason": string(deviceTokenRejectReason(err)),
			})
			return
		}
		upgrader := websocket.Upgrader{
			CheckOrigin: s.checkOrigin,
			// Only the real subprotocol is selected; the `token.*` slot
			// is consumed server-side and MUST NOT be echoed back to the
			// client (§6.5.1: "不 echo token.* 子协议——token 不应反射回
			// client").
			Subprotocols: []string{DeviceWSSubprotocol},
		}
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		ws.SetReadLimit(DefaultDeviceWSReadLimit)
		tx := newWSDeviceTransport(ws)
		cadence := s.cfg.PingCadence
		if cadence <= 0 {
			cadence = DefaultDevicePingCadence
		}
		idle := s.cfg.IdleReadTimeout
		if idle <= 0 {
			idle = DefaultDeviceIdleReadTimeout
		}
		pingWrite := s.cfg.PingWriteTimeout
		if pingWrite <= 0 {
			pingWrite = DefaultDevicePingWriteTimeout
		}
		tx.startPinger(cadence, pingWrite, idle)
		conn := NewConnection(row, tx)
		s.registerConnection(sessionID, conn)
		if err := s.MarkActive(c.Request.Context(), sessionID); err != nil {
			s.unregisterConnection(sessionID, conn)
			_ = conn.Close()
			return
		}
		defer func() {
			if s.unregisterConnection(sessionID, conn) {
				_ = s.MarkOffline(c.Request.Context(), sessionID)
			}
			_ = conn.Close()
		}()

		for {
			frame, err := conn.transport.ReadFrame(c.Request.Context())
			if err != nil {
				return
			}
			current, err := s.ValidateToken(c.Request.Context(), sessionID, token)
			if err != nil {
				return
			}
			frame.DeviceSessionID = devicetransit.DeviceSessionID(sessionID)
			frame.ChannelID = current.ChannelID
			frame.AdapterActorID = current.AdapterActorID
			if err := forwarder.ForwardDeviceFrame(c.Request.Context(), frame); err != nil {
				return
			}
		}
	}
}

func deviceTokenRejectReason(err error) kerneldaemonbus.DeviceSessionRejectReason {
	switch {
	case errors.Is(err, ErrTokenInvalid):
		return kerneldaemonbus.DeviceSessionRejectTokenInvalid
	case errors.Is(err, ErrSessionExpired):
		return kerneldaemonbus.DeviceSessionRejectTokenExpired
	case errors.Is(err, ErrSessionRevoked):
		return kerneldaemonbus.DeviceSessionRejectSessionRevoked
	default:
		return kerneldaemonbus.DeviceSessionRejectSessionUnknown
	}
}

// parseDeviceWSSubprotocols parses the Sec-WebSocket-Protocol header
// values offered by the client and returns the extracted bearer token
// plus whether the real `coagent.device.v1` subprotocol was offered.
//
// Per RFC 6455 §1.9 the header is a comma-separated list (possibly
// across multiple header lines); gorilla/Gin keeps each header line as
// a separate string. We split each value on `,`, trim spaces, then
// classify:
//
//   - `coagent.device.v1` → real protocol present (handshake well-formed)
//   - prefix `token.`     → bearer token (everything after the prefix)
//   - anything else       → fail-closed
//
// Returns ok=false for duplicate/unknown slots. Empty slots are ignored.
func parseDeviceWSSubprotocols(headers []string) (token string, hasRealProto bool, ok bool) {
	ok = true
	var seenToken bool
	for _, line := range headers {
		for _, part := range strings.Split(line, ",") {
			p := strings.TrimSpace(part)
			if p == "" {
				continue
			}
			if p == DeviceWSSubprotocol {
				if hasRealProto {
					return "", false, false
				}
				hasRealProto = true
				continue
			}
			if strings.HasPrefix(p, deviceWSTokenPrefix) {
				if seenToken {
					return "", false, false
				}
				seenToken = true
				token = strings.TrimPrefix(p, deviceWSTokenPrefix)
				continue
			}
			return "", false, false
		}
	}
	return token, hasRealProto, true
}

func (s *Service) checkOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return s.cfg.AllowMissingOrigin
	}
	_, ok := s.allowedOrigins[origin]
	return ok
}

// SendFrameToDevice is invoked by the gateway when a daemon-pushed
// `device_transit.recv` frame arrives (impl-layer2 §5.3.2 outbound —
// adapter → device) — looks up the local device connection, rechecks the
// durable session state/time, and forwards only for a still-active session.
func (s *Service) SendFrameToDevice(ctx context.Context, sessionID string, frame DeviceFrame) error {
	s.mu.Lock()
	conn, ok := s.sessions[sessionID]
	s.mu.Unlock()
	if !ok {
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "session_not_found",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
		)
		return ErrSessionNotFound
	}
	row, err := s.Get(ctx, sessionID)
	if err != nil {
		s.closeCurrentConnection(sessionID)
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "session_lookup_failed",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
			"error", err.Error(),
		)
		return err
	}
	now := s.nowMs()
	if now > row.ExpiresAt {
		if err := s.expireSessionIfDue(ctx, sessionID, now); err != nil {
			return err
		}
		s.closeCurrentConnection(sessionID)
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "expired",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
			"channel_id", string(row.ChannelID),
			"device_id", row.DeviceID,
			"expires_at", row.ExpiresAt,
		)
		return ErrSessionExpired
	}
	switch row.State {
	case StateActive:
	case StateExpired:
		s.closeCurrentConnection(sessionID)
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "expired_state",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
			"channel_id", string(row.ChannelID),
			"device_id", row.DeviceID,
		)
		return ErrSessionExpired
	case StateRevoked:
		s.closeCurrentConnection(sessionID)
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "revoked",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
			"channel_id", string(row.ChannelID),
			"device_id", row.DeviceID,
		)
		return ErrSessionRevoked
	default:
		s.closeCurrentConnection(sessionID)
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "not_active",
			"request_id", requestctx.RequestID(ctx),
			"device_session_id", sessionID,
			"channel_id", string(row.ChannelID),
			"device_id", row.DeviceID,
			"state", string(row.State),
		)
		return ErrSessionNotReady
	}
	return conn.SendToDevice(ctx, frame)
}

func (s *Service) registerConnection(sessionID string, conn *Connection) {
	var previous *Connection
	s.mu.Lock()
	conn.Generation = s.connGen.Add(1)
	previous = s.sessions[sessionID]
	s.sessions[sessionID] = conn
	s.mu.Unlock()
	if previous != nil && previous != conn {
		_ = previous.Close()
	}
}

func (s *Service) unregisterConnection(sessionID string, conn *Connection) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.sessions[sessionID]
	if current != conn || current.Generation != conn.Generation {
		return false
	}
	delete(s.sessions, sessionID)
	return true
}

func (s *Service) closeCurrentConnection(sessionID string) bool {
	s.mu.Lock()
	conn := s.sessions[sessionID]
	if conn != nil {
		delete(s.sessions, sessionID)
	}
	s.mu.Unlock()
	if conn == nil {
		return false
	}
	_ = conn.Close()
	return true
}
