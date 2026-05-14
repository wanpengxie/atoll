package devicebus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Connection wraps one open device WS — sender of device_transit
// frames to the daemon (via daemonbus) + receiver of frames pushed
// back from the daemon.
type Connection struct {
	Session  Session
	transport DeviceTransport

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
	Direction       string          `json:"direction"`        // "from_device" | "to_device"
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
type wsDeviceTransport struct {
	mu sync.Mutex
	ws *websocket.Conn
}

func (t *wsDeviceTransport) ReadFrame(ctx context.Context) (DeviceFrame, error) {
	_, data, err := t.ws.ReadMessage()
	if err != nil {
		return DeviceFrame{}, err
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
	return t.ws.WriteMessage(websocket.TextMessage, data)
}
func (t *wsDeviceTransport) Close() error { return t.ws.Close() }

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
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		conn := NewConnection(row, &wsDeviceTransport{ws: ws})
		if err := s.MarkActive(c.Request.Context(), sessionID); err != nil {
			_ = conn.Close()
			return
		}
		s.registerConnection(sessionID, conn)
		defer func() {
			s.unregisterConnection(sessionID)
			_ = s.MarkOffline(c.Request.Context(), sessionID)
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
	s.mu.Lock()
	s.sessions[sessionID] = conn
	s.mu.Unlock()
}

func (s *Service) unregisterConnection(sessionID string) {
	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.mu.Unlock()
}
