package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// upgrader is shared between daemonbus WS upgrades. Demo-period
// allows any origin (gateway is single-host) — production should
// tighten this.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// HandlersProvider is what the gateway implements to give daemonbus
// the runtime hooks (push / ack / heartbeat / reclaim handlers).
//
// Carved out as an interface so server/gateway can compose without
// daemonbus importing the higher-level subsystems (which would
// create a cycle).
type HandlersProvider interface {
	DaemonbusHandlers() Handlers
}

// HandleWS returns a gin.HandlerFunc that:
//
//  1. Reads daemon_id + key + version from query string
//  2. Validates the key against Service.cfg.SharedSecret
//  3. Issues a fresh connection_epoch
//  4. Upgrades to WS + spawns Connection.Run
//
// Demo-period auth is intentionally simple. Production should sign
// the connect frame with bcrypt(per-daemon-key) and verify.
func (s *Service) HandleWS(provider HandlersProvider) gin.HandlerFunc {
	return func(c *gin.Context) {
		daemonID := placement.DaemonID(c.Query("daemon_id"))
		key := c.Query("key")
		host := c.Query("host")
		version := c.Query("version")
		if daemonID == "" || key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_id + key required"})
			return
		}
		if key != s.cfg.SharedSecret {
			c.JSON(http.StatusUnauthorized, gin.H{"error": ErrDaemonAuthFailed.Error()})
			return
		}
		if err := s.RegisterDaemon(c.Request.Context(), daemonID, host, version, 0, key); err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		epoch, err := s.IssueConnectionEpoch(c.Request.Context(), daemonID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}

		conn := NewConnection(daemonID, epoch, &wsTransport{ws: ws})
		s.Register(conn)
		defer s.Unregister(daemonID)

		// Send the connection_accepted frame so daemon learns its
		// epoch (mirrors the L2 §9.4 contract).
		_, _ = conn.SendFrame(c.Request.Context(), daemonbus.FrameType("control.connection_accepted"), connectionAcceptedPayload{
			DaemonID:        string(daemonID),
			ConnectionEpoch: int64(epoch),
		})

		if err := conn.Run(c.Request.Context(), provider.DaemonbusHandlers()); err != nil {
			// log via gin context for ops visibility — keep it stderr
			// rather than fatal.
			_ = c.Error(err)
		}
	}
}

// connectionAcceptedPayload mirrors a tiny custom frame the gateway
// sends right after upgrading. Strictly speaking control.connection_
// accepted is not in the L2 §9.1 closed set (which is 18 control
// types) — this is a demo-period nicety that the daemon side may
// ignore. Treat it as informational metadata, NOT a protocol
// requirement.
type connectionAcceptedPayload struct {
	DaemonID        string `json:"daemon_id"`
	ConnectionEpoch int64  `json:"connection_epoch"`
}

// Register tracks an open connection.
func (s *Service) Register(conn *Connection) {
	s.mu.Lock()
	s.connections[conn.DaemonID] = conn
	s.mu.Unlock()
}

// Unregister drops a connection from the registry.
func (s *Service) Unregister(daemonID placement.DaemonID) {
	s.mu.Lock()
	delete(s.connections, daemonID)
	s.mu.Unlock()
}

// ConnectionFor returns the open Connection for daemonID, if any.
func (s *Service) ConnectionFor(daemonID placement.DaemonID) (*Connection, bool) {
	s.mu.RLock()
	conn, ok := s.connections[daemonID]
	s.mu.RUnlock()
	return conn, ok
}

// ConnectionForChannel resolves channel_id → daemon via placement +
// returns the open connection. Returns ErrNoDaemonForChannel when no
// active placement exists; ErrDaemonNotRegistered when the placement
// references a daemon that isn't currently connected.
func (s *Service) ConnectionForChannel(ctx context.Context, channelID string) (*Connection, error) {
	daemonID, err := s.LookupDaemonForChannel(ctx, channel.ID(channelID))
	if err != nil {
		return nil, err
	}
	conn, ok := s.ConnectionFor(daemonID)
	if !ok {
		return nil, ErrDaemonNotRegistered
	}
	return conn, nil
}

// wsTransport adapts gorilla/websocket to the Transport interface.
type wsTransport struct {
	mu sync.Mutex // gorilla doesn't support concurrent writers
	ws *websocket.Conn
}

func (t *wsTransport) ReadFrame(ctx context.Context) (daemonbus.Frame, error) {
	_, data, err := t.ws.ReadMessage()
	if err != nil {
		return daemonbus.Frame{}, err
	}
	var f daemonbus.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return daemonbus.Frame{}, fmt.Errorf("daemonbus: parse frame: %w", err)
	}
	return f, nil
}

func (t *wsTransport) WriteFrame(ctx context.Context, frame daemonbus.Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return t.ws.WriteMessage(websocket.TextMessage, data)
}

func (t *wsTransport) Close() error { return t.ws.Close() }

// ensure compile-time interface satisfaction.
var (
	_ Transport = (*wsTransport)(nil)
	_           = errors.New
)
