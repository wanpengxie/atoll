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
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	proxycontract "github.com/wanpengxie/ActOS/internal/proxy/contract"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

const DefaultDeviceWSWriteTimeout = 10 * time.Second
const DefaultDevicePingCadence = 30 * time.Second
const DefaultDeviceIdleReadTimeout = 70 * time.Second
const DefaultDeviceWSReadLimit int64 = 4 << 20
const DefaultDevicePingWriteTimeout = 5 * time.Second

type DaemonConnection struct {
	Daemon     Daemon
	transport  DeviceTransport
	Generation uint64
	SessionID  string

	closeOnce sync.Once
	closed    chan struct{}
}

type DeviceTransport interface {
	ReadFrame(ctx context.Context) (DeviceFrame, error)
	WriteFrame(ctx context.Context, frame DeviceFrame) error
	Close() error
}

type DeviceFrame struct {
	Direction     string                  `json:"direction"`
	FrameType     proxycontract.FrameType `json:"frame_type,omitempty"`
	ActorID       string                  `json:"actor_id"`
	ChannelID     string                  `json:"channel_id"`
	RequestID     string                  `json:"request_id,omitempty"`
	ParentID      string                  `json:"parent_id,omitempty"`
	CorrelationID string                  `json:"correlation_id,omitempty"`
	Payload       json.RawMessage         `json:"payload,omitempty"`
	ExpiresAt     int64                   `json:"expires_at,omitempty"`
	TransitSeq    int64                   `json:"transit_seq,omitempty"`

	Hostname     string                       `json:"hostname,omitempty"`
	HostLabel    string                       `json:"host_label,omitempty"`
	Actors       []proxycontract.ReadyActorV2 `json:"actors,omitempty"`
	ProxyVersion string                       `json:"proxy_version,omitempty"`
}

type DeviceFrameAckPayload struct {
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	Detail    string `json:"detail,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

const (
	DeviceFrameAckAccepted          = "accepted"
	DeviceFrameAckRejectedPermanent = "rejected_permanent"
	DeviceFrameAckRejectedRetryable = "rejected_retryable"
)

func NewDaemonConnection(d Daemon, tx DeviceTransport, sessionID string) *DaemonConnection {
	return &DaemonConnection{
		Daemon:    d,
		transport: tx,
		SessionID: sessionID,
		closed:    make(chan struct{}),
	}
}

func (c *DaemonConnection) SendToDaemon(ctx context.Context, frame DeviceFrame) error {
	if c.IsClosed() {
		return errors.New("devicebus: daemon connection closed")
	}
	return c.transport.WriteFrame(ctx, frame)
}

func (c *DaemonConnection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *DaemonConnection) Close() error {
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

func (s *Service) HandleWSV2(forwarder TransitForwarder) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := strings.TrimSpace(c.Query(proxycontract.QueryParamApiKey))
		if apiKey == "" || !hasDeviceWSSubprotocol(c.Request.Header.Values("Sec-WebSocket-Protocol"), proxycontract.WSSubprotocolV2) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "valid api key and coagent.device.v2 subprotocol required"})
			return
		}
		daemon, err := s.GetDaemonByAPIKey(c.Request.Context(), apiKey)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
			return
		}
		upgrader := websocket.Upgrader{
			CheckOrigin:  s.checkDaemonOrigin,
			Subprotocols: []string{proxycontract.WSSubprotocolV2},
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
		conn := NewDaemonConnection(daemon, tx, "daemon-session-"+s.newConnectionID())
		s.registerDaemonConnection(daemon, conn)
		if err := s.MarkDaemonOnline(context.WithoutCancel(c.Request.Context()), daemon.ID); err != nil {
			s.log.Warn("devicebus.daemon_online_failed",
				"daemon_id", string(daemon.ID),
				"channel_id", string(daemon.ChannelID),
				"daemon_session_id", conn.SessionID,
				"err", err.Error(),
			)
			_ = conn.Close()
			return
		}
		s.log.Info("devicebus.daemon_connected",
			"daemon_id", string(daemon.ID),
			"daemon_name", daemon.Name,
			"channel_id", string(daemon.ChannelID),
			"daemon_session_id", conn.SessionID,
		)
		defer func() {
			s.unregisterDaemonConnection(daemon.ID, conn)
			_ = conn.Close()
		}()

		first, err := conn.transport.ReadFrame(c.Request.Context())
		if err != nil {
			return
		}
		if first.FrameType != proxycontract.FrameTypeReady {
			s.log.Warn("devicebus.daemon_ready_rejected",
				"reason", "first_frame_not_ready",
				"daemon_id", string(daemon.ID),
				"channel_id", string(daemon.ChannelID),
				"daemon_session_id", conn.SessionID,
				"frame_type", string(first.FrameType),
			)
			return
		}
		// Generation guard on the INITIAL ready projection (codex P1
		// connection.go:278): a proxy reconnect may have registered a newer
		// *DaemonConnection (superseding this one) between
		// registerDaemonConnection above and reading this ready frame off the
		// wire. Without the check, a stale connection would still
		// ApplyDaemonReady / notify the cloud daemon / mark facade state,
		// projecting active actors + facade wiring under the live
		// connection's identity. Same guard the read-loop frames carry below;
		// it MUST also gate the first ready frame and every server-state
		// mutation (heartbeat included).
		if !s.isCurrentConnection(conn) {
			s.log.Warn("devicebus.daemon_ready_rejected",
				"reason", "stale_connection_generation",
				"daemon_id", string(daemon.ID),
				"channel_id", string(daemon.ChannelID),
				"daemon_session_id", conn.SessionID,
				"generation", conn.Generation,
			)
			return
		}
		readyInput := readyInputFromFrame(first)
		if err := s.ApplyDaemonReady(context.WithoutCancel(c.Request.Context()), daemon, readyInput); err != nil {
			s.log.Warn("devicebus.daemon_ready_rejected",
				"reason", "ready_apply_failed",
				"daemon_id", string(daemon.ID),
				"channel_id", string(daemon.ChannelID),
				"daemon_session_id", conn.SessionID,
				"err", err.Error(),
			)
			return
		}
		if n := s.proxyDaemonNotifier(); n != nil {
			if err := n.NotifyProxyDaemonReady(context.WithoutCancel(c.Request.Context()), daemon, readyInput); err != nil {
				s.log.Warn("devicebus.daemon_ready_notify_failed",
					"daemon_id", string(daemon.ID),
					"channel_id", string(daemon.ChannelID),
					"daemon_session_id", conn.SessionID,
					"err", err.Error(),
				)
				if markErr := s.MarkDaemonFacadeState(context.WithoutCancel(c.Request.Context()), daemon.ID, readyActorIDs(readyInput), "failed", err.Error()); markErr != nil {
					s.log.Warn("devicebus.daemon_facade_state_failed",
						"daemon_id", string(daemon.ID),
						"daemon_session_id", conn.SessionID,
						"state", "failed",
						"err", markErr.Error(),
					)
				}
			} else {
				if markErr := s.MarkDaemonFacadeState(context.WithoutCancel(c.Request.Context()), daemon.ID, readyActorIDs(readyInput), "installed", ""); markErr != nil {
					s.log.Warn("devicebus.daemon_facade_state_failed",
						"daemon_id", string(daemon.ID),
						"daemon_session_id", conn.SessionID,
						"state", "installed",
						"err", markErr.Error(),
					)
				}
			}
		}
		s.log.Info("devicebus.daemon_ready",
			"daemon_id", string(daemon.ID),
			"channel_id", string(daemon.ChannelID),
			"daemon_session_id", conn.SessionID,
			"actors", len(first.Actors),
			"hostname", first.Hostname,
			"proxy_version", first.ProxyVersion,
		)

		for {
			frame, err := conn.transport.ReadFrame(c.Request.Context())
			if err != nil {
				return
			}
			switch frame.FrameType {
			case proxycontract.FrameTypeHeartbeat:
				// Generation guard before the heartbeat server-state mutation
				// (codex P1: "any inbound frame that affects server state").
				// A stale connection's heartbeat must not refresh liveness for
				// a daemon row a newer connection now owns.
				if !s.isCurrentConnection(conn) {
					s.log.Warn("devicebus.daemon_frame_dropped",
						"reason", "stale_connection_generation",
						"daemon_id", string(daemon.ID),
						"channel_id", string(daemon.ChannelID),
						"daemon_session_id", conn.SessionID,
						"generation", conn.Generation,
						"frame_type", string(frame.FrameType),
					)
					return
				}
				if err := s.HeartbeatDaemon(context.WithoutCancel(c.Request.Context()), daemon.ID); err != nil {
					s.log.Warn("devicebus.daemon_heartbeat_failed",
						"daemon_id", string(daemon.ID),
						"channel_id", string(daemon.ChannelID),
						"daemon_session_id", conn.SessionID,
						"err", err.Error(),
					)
					return
				}
				continue
			case proxycontract.FrameTypeReady, proxycontract.FrameTypeShutdown:
				s.log.Warn("devicebus.daemon_frame_rejected",
					"reason", "unexpected_reserved_frame",
					"daemon_id", string(daemon.ID),
					"channel_id", string(daemon.ChannelID),
					"daemon_session_id", conn.SessionID,
					"frame_type", string(frame.FrameType),
				)
				return
			case "":
				if strings.TrimSpace(frame.ActorID) == "" {
					continue
				}
				// Generation guard: a proxy reconnect may have already
				// installed a newer *DaemonConnection (closing this one)
				// while this read loop was mid-iteration on a frame it
				// already pulled off the wire. Drop such stale frames so
				// the superseded instance cannot project readiness or
				// inject callbacks/calls under the live connection's
				// identity. Routing (actorToDaemon) is keyed by daemon,
				// not generation, so without this the orphan frame would
				// otherwise be projected/forwarded as if current.
				if !s.isCurrentConnection(conn) {
					s.log.Warn("devicebus.daemon_frame_dropped",
						"reason", "stale_connection_generation",
						"daemon_id", string(daemon.ID),
						"channel_id", string(frame.ChannelID),
						"daemon_session_id", conn.SessionID,
						"generation", conn.Generation,
						"actor_id", frame.ActorID,
					)
					return
				}
				s.projectActorReadinessFrame(context.WithoutCancel(c.Request.Context()), daemon.ID, frame)
				actorID := actor.ActorID(frame.ActorID)
				targetChannel := channel.ID(strings.TrimSpace(frame.ChannelID))
				if targetChannel == "" {
					// Multi-channel daemon may host the same actor in N
					// channels. With no explicit channel_id on the
					// inbound frame, route to the unique attached
					// channel for this (daemon, actor) pair. If multiple
					// match we reject (ambiguous — caller MUST stamp
					// channel_id when serving more than one channel).
					targetChannel = s.resolveDaemonActorChannel(daemon.ID, actorID)
				}
				if targetChannel == "" || !s.daemonOwnsActor(daemon.ID, targetChannel, actorID) {
					// Drop the frame but keep the ws alive: an idle
					// daemon (no attachments yet) or a transient
					// readiness event from before attach can both hit
					// this path. Closing the ws used to cause an
					// install loop where the daemon reconnected,
					// re-sent ready, fired the same orphan frame, and
					// got kicked again. With `continue` the connection
					// stays up and UI shows the daemon as online while
					// the user attaches channels via the UI.
					s.log.Warn("devicebus.daemon_frame_dropped",
						"reason", "actor_not_registered",
						"daemon_id", string(daemon.ID),
						"channel_id", string(targetChannel),
						"daemon_session_id", conn.SessionID,
						"actor_id", frame.ActorID,
					)
					s.sendCallbackAck(c.Request.Context(), conn, frame, DeviceFrameAckRejectedRetryable, "actor_not_registered", "actor is not attached to this daemon route")
					continue
				}
				frame.ChannelID = string(targetChannel)
				if err := forwarder.ForwardDeviceFrame(c.Request.Context(), frame, actorID); err != nil {
					status, reason, detail := callbackAckFromError(err)
					s.log.Warn("devicebus.daemon_frame_forward_failed",
						"daemon_id", string(daemon.ID),
						"channel_id", string(targetChannel),
						"daemon_session_id", conn.SessionID,
						"actor_id", frame.ActorID,
						"ack_status", status,
						"err", err.Error(),
					)
					s.sendCallbackAck(c.Request.Context(), conn, frame, status, reason, detail)
					continue
				}
				s.sendCallbackAck(c.Request.Context(), conn, frame, DeviceFrameAckAccepted, "", "")
			default:
				s.log.Warn("devicebus.daemon_frame_rejected",
					"reason", "unknown_frame_type",
					"daemon_id", string(daemon.ID),
					"channel_id", string(daemon.ChannelID),
					"daemon_session_id", conn.SessionID,
					"frame_type", string(frame.FrameType),
				)
				return
			}
		}
	}
}

func hasDeviceWSSubprotocol(headers []string, want string) bool {
	for _, line := range headers {
		for _, part := range strings.Split(line, ",") {
			if strings.TrimSpace(part) == want {
				return true
			}
		}
	}
	return false
}

func (s *Service) checkDaemonOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	_, ok := s.allowedOrigins[origin]
	return ok
}

func (s *Service) SendFrameToActor(ctx context.Context, channelID channel.ID, actorID actor.ActorID, frame DeviceFrame) error {
	key := routeKey(channelID, actorID)
	s.mu.Lock()
	daemonID, hasProxyRoute := s.actorToDaemon[key]
	var dconn *DaemonConnection
	if hasProxyRoute {
		dconn = s.daemonConns[daemonID]
	}
	s.mu.Unlock()
	if !hasProxyRoute {
		s.log.Warn("devicebus.proxy_outbound_rejected",
			"reason", "daemon_route_not_found",
			"request_id", requestctx.RequestID(ctx),
			"actor_id", string(actorID),
			"channel_id", string(channelID),
		)
		return ErrRegistrationNotFound
	}
	if dconn == nil || dconn.IsClosed() {
		s.log.Warn("devicebus.proxy_outbound_rejected",
			"reason", "daemon_route_not_connected",
			"request_id", requestctx.RequestID(ctx),
			"actor_id", string(actorID),
			"channel_id", string(channelID),
			"daemon_id", string(daemonID),
		)
		return ErrRegistrationNotFound
	}
	frame.ActorID = string(actorID)
	frame.ChannelID = string(channelID)
	return dconn.SendToDaemon(ctx, frame)
}

func (s *Service) sendCallbackAck(ctx context.Context, conn *DaemonConnection, frame DeviceFrame, status, reason, detail string) {
	if conn == nil || (strings.TrimSpace(frame.RequestID) == "" && strings.TrimSpace(frame.CorrelationID) == "") {
		return
	}
	payload, err := json.Marshal(DeviceFrameAckPayload{
		Status:    status,
		Reason:    reason,
		Detail:    detail,
		Retryable: status == DeviceFrameAckRejectedRetryable,
	})
	if err != nil {
		s.log.Warn("devicebus.callback_ack_marshal_failed",
			"daemon_id", string(conn.Daemon.ID),
			"daemon_session_id", conn.SessionID,
			"request_id", frame.RequestID,
			"err", err.Error(),
		)
		return
	}
	ackCtx, cancel := context.WithTimeout(ctx, DefaultDeviceWSWriteTimeout)
	defer cancel()
	if err := conn.SendToDaemon(ackCtx, DeviceFrame{
		Direction:     "to_device",
		FrameType:     proxycontract.FrameTypeAck,
		ActorID:       frame.ActorID,
		ChannelID:     frame.ChannelID,
		RequestID:     frame.RequestID,
		CorrelationID: frame.CorrelationID,
		ExpiresAt:     frame.ExpiresAt,
		Payload:       payload,
	}); err != nil {
		s.log.Warn("devicebus.callback_ack_failed",
			"daemon_id", string(conn.Daemon.ID),
			"daemon_session_id", conn.SessionID,
			"request_id", frame.RequestID,
			"status", status,
			"err", err.Error(),
		)
	}
}

func callbackAckFromError(err error) (status, reason, detail string) {
	var ackErr *devicetransit.AckError
	if errors.As(err, &ackErr) {
		reason = ackErr.Reason
		detail = ackErr.Detail
		switch ackErr.Result {
		case devicetransit.AckAccepted, devicetransit.AckDelivered:
			return DeviceFrameAckAccepted, reason, detail
		case devicetransit.AckRejectedPermanent, devicetransit.AckDropped:
			return DeviceFrameAckRejectedPermanent, reason, detail
		case devicetransit.AckRejectedRetryable:
			return DeviceFrameAckRejectedRetryable, reason, detail
		}
	}
	return DeviceFrameAckRejectedRetryable, "forward_failed", err.Error()
}

func (s *Service) registerDaemonConnection(d Daemon, conn *DaemonConnection) {
	var previous *DaemonConnection
	s.mu.Lock()
	conn.Generation = s.connGen.Add(1)
	previous = s.daemonConns[d.ID]
	s.daemonConns[d.ID] = conn
	s.mu.Unlock()
	if previous != nil && previous != conn {
		metrics.Default().IncCounter("devicebus.daemon.reconnect",
			"daemon_id", string(d.ID),
			"channel_id", string(d.ChannelID),
		)
		_ = previous.Close()
	}
}

func (s *Service) unregisterDaemonConnection(daemonID placement.DaemonID, conn *DaemonConnection) bool {
	actors, _ := s.ListDaemonActiveActorIDs(context.Background(), daemonID)
	s.mu.Lock()
	current := s.daemonConns[daemonID]
	if current != conn || current.Generation != conn.Generation {
		s.mu.Unlock()
		return false
	}
	delete(s.daemonConns, daemonID)
	s.clearDaemonActorRoutesLocked(daemonID)
	s.mu.Unlock()
	ctx := context.Background()
	if err := s.MarkDaemonOffline(ctx, daemonID); err != nil {
		s.log.Warn("devicebus.daemon_offline_failed",
			"daemon_id", string(daemonID),
			"daemon_session_id", conn.SessionID,
			"err", err.Error(),
		)
	}
	if err := s.clearDaemonActiveActors(ctx, daemonID); err != nil {
		s.log.Warn("devicebus.daemon_clear_active_failed",
			"daemon_id", string(daemonID),
			"daemon_session_id", conn.SessionID,
			"err", err.Error(),
		)
	}
	if err := s.MarkDaemonFacadeState(ctx, daemonID, actors, "offline", "daemon websocket disconnected"); err != nil {
		s.log.Warn("devicebus.daemon_facade_offline_failed",
			"daemon_id", string(daemonID),
			"daemon_session_id", conn.SessionID,
			"err", err.Error(),
		)
	}
	if n := s.proxyDaemonNotifier(); n != nil && len(actors) > 0 {
		if err := n.NotifyProxyDaemonOffline(ctx, conn.Daemon, actors); err != nil {
			s.log.Warn("devicebus.daemon_offline_notify_failed",
				"daemon_id", string(daemonID),
				"daemon_session_id", conn.SessionID,
				"err", err.Error(),
			)
		}
	}
	s.log.Info("devicebus.daemon_disconnected",
		"daemon_id", string(daemonID),
		"channel_id", string(conn.Daemon.ChannelID),
		"daemon_session_id", conn.SessionID,
	)
	return true
}

func (s *Service) closeDaemonConnection(daemonID placement.DaemonID, sendShutdown bool) bool {
	s.mu.Lock()
	conn := s.daemonConns[daemonID]
	if conn != nil {
		delete(s.daemonConns, daemonID)
		s.clearDaemonActorRoutesLocked(daemonID)
	}
	s.mu.Unlock()
	if conn == nil {
		return false
	}
	if sendShutdown {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultDeviceWSWriteTimeout)
		_ = conn.SendToDaemon(ctx, DeviceFrame{
			Direction: "to_device",
			FrameType: proxycontract.FrameTypeShutdown,
		})
		cancel()
	}
	_ = conn.Close()
	return true
}

// KickDaemonForReload closes the daemon's ws (no shutdown frame) so its
// reconnect path fires immediately and the next ready frame picks up
// whatever attachments + actors are now in DB. Returns true if a
// connection was actually closed (i.e. daemon was online).
func (s *Service) KickDaemonForReload(daemonID placement.DaemonID) bool {
	ctx := context.Background()
	if !s.closeDaemonConnection(daemonID, false) {
		if err := s.invalidateDaemonRuntimeState(ctx, daemonID, "daemon reload requested without live websocket"); err != nil {
			s.log.Warn("devicebus.daemon_reload_invalidate_failed",
				"daemon_id", string(daemonID),
				"err", err.Error(),
			)
		}
		return false
	}
	if err := s.invalidateDaemonRuntimeState(ctx, daemonID, "daemon websocket closed for reload"); err != nil {
		s.log.Warn("devicebus.daemon_reload_invalidate_failed",
			"daemon_id", string(daemonID),
			"err", err.Error(),
		)
	}
	return true
}

func (s *Service) clearDaemonActorRoutesLocked(daemonID placement.DaemonID) {
	for key, got := range s.actorToDaemon {
		if got == daemonID {
			delete(s.actorToDaemon, key)
		}
	}
}

// clearDaemonChannelRoutesLocked drops only the (channel, *) → daemon
// entries — used by DetachDaemonFromChannel when the daemon stays
// connected but loses one channel of attachment.
func (s *Service) clearDaemonChannelRoutesLocked(daemonID placement.DaemonID, channelID channel.ID) {
	prefix := routeKey(channelID, "")
	for key, got := range s.actorToDaemon {
		if got == daemonID && strings.HasPrefix(key, prefix) {
			delete(s.actorToDaemon, key)
		}
	}
}

func (s *Service) daemonOwnsActor(daemonID placement.DaemonID, channelID channel.ID, actorID actor.ActorID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.actorToDaemon[routeKey(channelID, actorID)] == daemonID
}

// isCurrentConnection reports whether conn is still the registered
// (live) connection for its daemon. A proxy reconnect installs a new
// *DaemonConnection with a fresh generation and closes the old one, but
// the old read loop can still be mid-iteration on a frame it already
// pulled off the wire. Gating inbound projection/forwarding on this
// check rejects those stale frames so a superseded instance cannot
// mutate readiness projection or inject callbacks/calls under the new
// connection's identity.
func (s *Service) isCurrentConnection(conn *DaemonConnection) bool {
	if conn == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.daemonConns[conn.Daemon.ID]
	return current == conn && current.Generation == conn.Generation
}

// resolveDaemonActorChannel returns the unique channel id where this
// daemon owns the actor. Called when an inbound frame lacks channel_id
// (legacy single-channel ext flow). Empty return means either zero or
// >1 matches — caller treats both as "ambiguous, reject the frame".
func (s *Service) resolveDaemonActorChannel(daemonID placement.DaemonID, actorID actor.ActorID) channel.ID {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found channel.ID
	suffix := "\x00" + string(actorID)
	for key, got := range s.actorToDaemon {
		if got != daemonID || !strings.HasSuffix(key, suffix) {
			continue
		}
		if found != "" {
			// Ambiguous — daemon serves this actor in multiple
			// channels and the caller didn't disambiguate.
			return ""
		}
		found = channel.ID(strings.TrimSuffix(key, suffix))
	}
	return found
}

func (s *Service) newConnectionID() string {
	return uuid.NewString()
}

func readyActorIDs(in DaemonReadyInput) []actor.ActorID {
	out := make([]actor.ActorID, 0, len(in.Actors))
	for _, a := range in.Actors {
		if a.ActorID != "" {
			out = append(out, a.ActorID)
		}
	}
	return out
}

func readyInputFromFrame(frame DeviceFrame) DaemonReadyInput {
	actors := make([]ReadyActor, 0, len(frame.Actors))
	for _, a := range frame.Actors {
		actors = append(actors, ReadyActor{
			ActorID:       actor.ActorID(strings.TrimSpace(a.ActorID)),
			CapabilitySet: append(json.RawMessage(nil), a.CapabilitySet...),
		})
	}
	return DaemonReadyInput{
		Hostname:     frame.Hostname,
		HostLabel:    frame.HostLabel,
		ProxyVersion: frame.ProxyVersion,
		Actors:       actors,
	}
}
