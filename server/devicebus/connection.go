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

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

const DefaultDeviceWSWriteTimeout = 10 * time.Second
const DefaultDevicePingCadence = 30 * time.Second
const DefaultDeviceIdleReadTimeout = 70 * time.Second
const DefaultDeviceWSReadLimit int64 = 4 << 20
const DefaultDevicePingWriteTimeout = 5 * time.Second

type Connection struct {
	Registration ActorRegistration
	transport    DeviceTransport
	Generation   uint64

	closeOnce sync.Once
	closed    chan struct{}
}

type DeviceTransport interface {
	ReadFrame(ctx context.Context) (DeviceFrame, error)
	WriteFrame(ctx context.Context, frame DeviceFrame) error
	Close() error
}

type DeviceFrame struct {
	Direction     string          `json:"direction"`
	ActorID       string          `json:"actor_id"`
	ChannelID     string          `json:"channel_id"`
	RequestID     string          `json:"request_id,omitempty"`
	ParentID      string          `json:"parent_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	ExpiresAt     int64           `json:"expires_at,omitempty"`
	TransitSeq    int64           `json:"transit_seq,omitempty"`
}

func NewConnection(r ActorRegistration, tx DeviceTransport) *Connection {
	return &Connection{
		Registration: r,
		transport:    tx,
		closed:       make(chan struct{}),
	}
}

func (c *Connection) SendToDevice(ctx context.Context, frame DeviceFrame) error {
	if c.IsClosed() {
		return errors.New("devicebus: connection closed")
	}
	return c.transport.WriteFrame(ctx, frame)
}

func (c *Connection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.transport.Close()
	})
	return nil
}

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
				err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(pingWriteTimeout))
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

type TransitForwarder interface {
	ForwardDeviceFrame(ctx context.Context, frame DeviceFrame, adapterActorID actor.ActorID) error
}

const DeviceWSSubprotocol = "coagent.device.v1"
const deviceWSTokenPrefix = "token."

func (s *Service) HandleWS(forwarder TransitForwarder) gin.HandlerFunc {
	return func(c *gin.Context) {
		actorID := actor.ActorID(strings.TrimSpace(c.Query("actor_id")))
		token, hasRealProto, parseOK := parseDeviceWSSubprotocols(c.Request.Header.Values("Sec-WebSocket-Protocol"))
		if !parseOK || actorID == "" || token == "" || !hasRealProto {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "actor_id query + Sec-WebSocket-Protocol: coagent.device.v1, token.<token> required",
			})
			return
		}
		row, err := s.ValidateToken(c.Request.Context(), actorID, token)
		if err != nil {
			status := http.StatusUnauthorized
			c.JSON(status, gin.H{
				"error":         err.Error(),
				"reject_reason": actorTokenRejectReason(err),
			})
			return
		}
		upgrader := websocket.Upgrader{
			CheckOrigin:  s.checkOrigin,
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
		s.registerConnection(row.ChannelID, row.ActorID, conn)
		defer func() {
			_ = s.unregisterConnection(row.ChannelID, row.ActorID, conn)
			_ = conn.Close()
		}()

		for {
			frame, err := conn.transport.ReadFrame(c.Request.Context())
			if err != nil {
				return
			}
			current, err := s.ValidateToken(c.Request.Context(), actorID, token)
			if err != nil {
				return
			}
			frame.ActorID = string(current.ActorID)
			frame.ChannelID = string(current.ChannelID)
			if err := forwarder.ForwardDeviceFrame(c.Request.Context(), frame, current.ActorID); err != nil {
				return
			}
		}
	}
}

func actorTokenRejectReason(err error) string {
	switch {
	case errors.Is(err, ErrTokenInvalid):
		return "device_token_invalid"
	case errors.Is(err, ErrTokenExpired):
		return "device_token_expired"
	default:
		return "device_actor_unknown"
	}
}

func parseDeviceWSSubprotocols(headers []string) (token string, hasRealProto bool, ok bool) {
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

func (s *Service) SendFrameToDevice(ctx context.Context, channelID channel.ID, actorID actor.ActorID, frame DeviceFrame) error {
	key := routeKey(channelID, actorID)
	s.mu.Lock()
	conn, ok := s.routes[key]
	s.mu.Unlock()
	if !ok {
		s.log.Warn("devicebus.outbound_rejected",
			"reason", "route_not_found",
			"request_id", requestctx.RequestID(ctx),
			"actor_id", string(actorID),
			"channel_id", string(channelID),
		)
		return ErrRegistrationNotFound
	}
	row, err := s.GetActor(ctx, channelID, actorID)
	if err != nil {
		s.closeCurrentConnection(channelID, actorID)
		return err
	}
	if s.nowMs() > row.ExpiresAt {
		_ = s.RevokeActor(ctx, channelID, actorID)
		if n := s.lifecycleNotifier(); n != nil {
			n.NotifyDeviceLifecycle(
				ctx,
				channelID,
				actorID,
				devicetransit.LifecycleTokenExpired,
				row.DeviceID,
				"actor token past expires_at",
			)
		}
		return ErrTokenExpired
	}
	frame.ActorID = string(actorID)
	frame.ChannelID = string(channelID)
	return conn.SendToDevice(ctx, frame)
}

func (s *Service) registerConnection(channelID channel.ID, actorID actor.ActorID, conn *Connection) {
	var previous *Connection
	key := routeKey(channelID, actorID)
	s.mu.Lock()
	conn.Generation = s.connGen.Add(1)
	previous = s.routes[key]
	s.routes[key] = conn
	s.mu.Unlock()
	if previous != nil && previous != conn {
		_ = previous.Close()
	}
	if n := s.lifecycleNotifier(); n != nil {
		n.NotifyDeviceLifecycle(
			context.Background(),
			channelID,
			actorID,
			devicetransit.LifecycleConnected,
			conn.Registration.DeviceID,
			"",
		)
	}
}

func (s *Service) unregisterConnection(channelID channel.ID, actorID actor.ActorID, conn *Connection) bool {
	key := routeKey(channelID, actorID)
	s.mu.Lock()
	current := s.routes[key]
	if current != conn || current.Generation != conn.Generation {
		s.mu.Unlock()
		return false
	}
	delete(s.routes, key)
	s.mu.Unlock()
	if n := s.lifecycleNotifier(); n != nil {
		n.NotifyDeviceLifecycle(
			context.Background(),
			channelID,
			actorID,
			devicetransit.LifecycleDisconnected,
			conn.Registration.DeviceID,
			"",
		)
	}
	return true
}

func (s *Service) closeCurrentConnection(channelID channel.ID, actorID actor.ActorID) bool {
	key := routeKey(channelID, actorID)
	s.mu.Lock()
	conn := s.routes[key]
	if conn != nil {
		delete(s.routes, key)
	}
	s.mu.Unlock()
	if conn == nil {
		return false
	}
	_ = conn.Close()
	return true
}
