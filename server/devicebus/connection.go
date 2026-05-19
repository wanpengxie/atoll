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

// DeviceFrame is the JSON wire shape exchanged between server and
// device. It's deliberately distinct from the daemonbus mux frame:
// the device sees a simpler shape and the server transforms.
type DeviceFrame struct {
	Direction       string          `json:"direction"` // "from_device" | "to_device"
	DeviceSessionID string          `json:"device_session_id"`
	ChannelID       string          `json:"channel_id"`
	RequestID       string          `json:"request_id,omitempty"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	ExpiresAt       int64           `json:"expires_at,omitempty"`
}

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
// DeviceFrame into a device_transit.recv daemonbus frame.
type TransitForwarder interface {
	ForwardDeviceFrame(ctx context.Context, frame DeviceFrame) error
}

// HandleWS upgrades the device socket. The gateway passes a
// TransitForwarder that knows how to route the frame to daemonbus.
func (s *Service) HandleWS(forwarder TransitForwarder) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID := c.Query("session_id")
		token := c.Query("token")
		if sessionID == "" || token == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "session_id + token required"})
			return
		}
		row, err := s.ValidateToken(c.Request.Context(), sessionID, token)
		if err != nil {
			status := http.StatusUnauthorized
			if errors.Is(err, ErrSessionNotReady) {
				status = http.StatusConflict
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		upgrader := websocket.Upgrader{CheckOrigin: s.checkOrigin}
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
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
			frame.DeviceSessionID = sessionID
			frame.ChannelID = string(row.ChannelID)
			frame.Direction = "from_device"
			if err := forwarder.ForwardDeviceFrame(c.Request.Context(), frame); err != nil {
				return
			}
		}
	}
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
// device_transit.send frame arrives — looks up the local device
// connection and forwards.
func (s *Service) SendFrameToDevice(ctx context.Context, sessionID string, frame DeviceFrame) error {
	s.mu.Lock()
	conn, ok := s.sessions[sessionID]
	s.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
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
