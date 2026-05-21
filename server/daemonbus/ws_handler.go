package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// DefaultServerWSWriteTimeout is the upper bound on a single WS write
// from the server's wsTransport. Without it, a stuck send buffer (peer
// gone half-open) would block forever while holding wsTransport.mu —
// any subsequent SendAndAwait / SendFrame on the same Connection would
// queue behind it and the gateway HTTP handler would wait up to its
// own ctx timeout (often tens of seconds) before failing. 10s matches
// the daemon-side floor (runtime/transit.DefaultWSWriteTimeout).
const DefaultServerWSWriteTimeout = 10 * time.Second

// DefaultPingCadence is how often the server sends a WS PingMessage to
// the daemon. 30s is chosen against cloudflare tunnel ~100s idle
// reaper — 3× margin lets a missed ping still keep the conn alive on
// the next tick. Pings are sent via gorilla `WriteControl`, which
// holds its own internal control-frame mutex and does NOT contend
// against wsTransport.mu (application-frame mutex), so a stuck
// business write will not delay a ping.
const DefaultPingCadence = 30 * time.Second

// DefaultServerIdleReadTimeout is the upper bound on time between
// frames (data or pong control) before the server gives up on the
// daemon and tears down. ~2.3× DefaultPingCadence absorbs one missed
// pong reply without false-positive disconnect. Cross-check with
// runtime/transit.DefaultWSIdleReadTimeout — kept in sync so neither
// side trips first on the steady-state.
const DefaultServerIdleReadTimeout = 70 * time.Second

// DefaultPingWriteTimeout caps a single WS ping write. Pings travel
// via WriteControl which itself takes a deadline; if the peer is gone
// half-open the ping write blocks until this deadline trips and the
// pinger then closes the conn so Connection.Run's reader-side
// surfaces the disconnect.
const DefaultPingWriteTimeout = 5 * time.Second

// DaemonWSSubprotocol is the real WebSocket subprotocol negotiated on
// the /daemonbus handshake. Daemon clients offer:
//
//	Sec-WebSocket-Protocol: coagent.daemon.v1, daemon.<daemon_id>, key.<key>
//
// Server selects (and echoes back per RFC 6455) ONLY
// `coagent.daemon.v1` — the `daemon.*` / `key.*` slots are pseudo-
// subprotocols ferrying handshake identity + secret and MUST NOT be
// reflected in the upgrade response.
const DaemonWSSubprotocol = "coagent.daemon.v1"

// daemonWSDaemonIDPrefix / daemonWSKeyPrefix are the slot prefixes used
// to ferry handshake identity (daemon_id) + shared secret (key) through
// `Sec-WebSocket-Protocol`. Mirrors devicebus's `token.*` slot pattern.
const (
	daemonWSDaemonIDPrefix = "daemon."
	daemonWSKeyPrefix      = "key."
)

// upgrader is shared between daemonbus WS upgrades. Demo-period
// allows any origin (gateway is single-host) — production should
// tighten this. The `Subprotocols` list selects the real
// `coagent.daemon.v1` protocol when offered; the `daemon.*` / `key.*`
// slots are consumed server-side and deliberately not advertised here.
var upgrader = websocket.Upgrader{
	CheckOrigin:  func(r *http.Request) bool { return true },
	Subprotocols: []string{DaemonWSSubprotocol},
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
//  1. Reads daemon_id + key from `Sec-WebSocket-Protocol` subprotocol
//     header (offered as `coagent.daemon.v1, daemon.<daemon_id>,
//     key.<key>`); reads `host` / `version` from URL query (operational
//     metadata only — not secret, no URL-leak concern)
//  2. Validates the key against Service.cfg.SharedSecret
//  3. Issues a fresh connection_epoch
//  4. Upgrades to WS + spawns Connection.Run
//
// Demo-period auth is intentionally simple. Production should sign
// the connect frame with bcrypt(per-daemon-key) and verify.
func (s *Service) HandleWS(provider HandlersProvider) gin.HandlerFunc {
	return func(c *gin.Context) {
		daemonIDRaw, key, hasRealProto := parseDaemonWSSubprotocols(c.Request.Header.Values("Sec-WebSocket-Protocol"))
		daemonID := placement.DaemonID(daemonIDRaw)
		host := c.Query("host")
		version := c.Query("version")
		if daemonID == "" || key == "" || !hasRealProto {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Sec-WebSocket-Protocol: coagent.daemon.v1, daemon.<daemon_id>, key.<key> required",
			})
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

		tx := newWSTransport(ws)
		conn := NewConnection(daemonID, epoch, tx)
		// Start the keepalive pinger now that the conn is wired. The
		// pinger stops when the conn closes (via Close → ws.Close →
		// ping write error → pinger goroutine exits). Config can
		// override the cadence / timeouts (tests use shorter values
		// to keep assertion windows tight).
		cadence := s.cfg.PingCadence
		if cadence <= 0 {
			cadence = DefaultPingCadence
		}
		idle := s.cfg.IdleReadTimeout
		if idle <= 0 {
			idle = DefaultServerIdleReadTimeout
		}
		pingWrite := s.cfg.PingWriteTimeout
		if pingWrite <= 0 {
			pingWrite = DefaultPingWriteTimeout
		}
		tx.startPinger(cadence, pingWrite, idle)
		s.Register(conn)
		defer s.UnregisterConnection(conn)

		// Send the connection_accepted frame so daemon learns its
		// epoch (mirrors the L2 §9.4 contract).
		_, _ = conn.SendFrame(c.Request.Context(), daemonbus.FrameTypeControlConnectionAccepted, connectionAcceptedPayload{
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

// parseDaemonWSSubprotocols parses the Sec-WebSocket-Protocol header
// values offered by a daemon client and returns (daemon_id, key,
// hasRealProto). Per RFC 6455 §1.9 the header is comma-separated; we
// split each line on `,`, trim, then classify slots:
//
//   - `coagent.daemon.v1` → real protocol present
//   - prefix `daemon.`    → daemon identity (everything after the prefix)
//   - prefix `key.`       → shared secret  (everything after the prefix)
//   - anything else       → ignored (forward-compat)
//
// Empty header or missing real protocol returns (_, _, false).
func parseDaemonWSSubprotocols(headers []string) (daemonID, key string, hasRealProto bool) {
	for _, line := range headers {
		for _, part := range strings.Split(line, ",") {
			p := strings.TrimSpace(part)
			if p == "" {
				continue
			}
			if p == DaemonWSSubprotocol {
				hasRealProto = true
				continue
			}
			if strings.HasPrefix(p, daemonWSDaemonIDPrefix) {
				daemonID = strings.TrimPrefix(p, daemonWSDaemonIDPrefix)
				continue
			}
			if strings.HasPrefix(p, daemonWSKeyPrefix) {
				key = strings.TrimPrefix(p, daemonWSKeyPrefix)
				continue
			}
		}
	}
	return daemonID, key, hasRealProto
}

// connectionAcceptedPayload mirrors the control.connection_accepted
// frame the gateway sends right after upgrading so the daemon learns
// its assigned connection epoch. It is handshake metadata rather than
// a routed business frame.
type connectionAcceptedPayload struct {
	DaemonID        string `json:"daemon_id"`
	ConnectionEpoch int64  `json:"connection_epoch"`
}

// Register tracks an open connection.
func (s *Service) Register(conn *Connection) {
	if conn == nil {
		return
	}
	var previous *Connection
	s.mu.Lock()
	conn.Generation = s.connGen.Add(1)
	previous = s.connections[conn.DaemonID]
	s.connections[conn.DaemonID] = conn
	s.mu.Unlock()
	if previous != nil && previous != conn {
		_ = previous.Close()
	}
}

// Unregister drops a connection from the registry.
func (s *Service) Unregister(daemonID placement.DaemonID) {
	s.mu.Lock()
	delete(s.connections, daemonID)
	s.mu.Unlock()
}

// UnregisterConnection drops conn only if it is still the current registry
// entry. This prevents an older WS read-loop defer from deleting a newer
// reconnect that registered under the same daemon id.
func (s *Service) UnregisterConnection(conn *Connection) bool {
	if conn == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.connections[conn.DaemonID]
	if current != conn || current.Generation != conn.Generation {
		return false
	}
	delete(s.connections, conn.DaemonID)
	return true
}

// ConnectionFor returns the open Connection for daemonID, if any.
func (s *Service) ConnectionFor(daemonID placement.DaemonID) (*Connection, bool) {
	s.mu.RLock()
	conn, ok := s.connections[daemonID]
	s.mu.RUnlock()
	if !ok || conn == nil || conn.IsClosed() {
		return nil, false
	}
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
//
// In addition to read/write framing it owns the keepalive pinger:
// a goroutine that emits a WS PingMessage every cfg.pingCadence and
// resets the read deadline on every successful pong / data frame. A
// failed ping write closes the conn, which makes Connection.Run's
// ReadFrame surface the disconnect; the gateway then Unregisters the
// daemon and the daemon-side supervisor reconnects with a fresh
// epoch.
type wsTransport struct {
	mu sync.Mutex // gorilla doesn't support concurrent application-frame writers
	ws *websocket.Conn

	// idleReadTimeout is set once by startPinger and read by ReadFrame
	// to refresh the deadline after every successful frame.
	idleReadTimeout time.Duration

	// stopPing closes when the pinger should exit. Independent of the
	// ws so we can detect "transport closed" without racing close.
	stopPing chan struct{}
	stopOnce sync.Once
}

func newWSTransport(ws *websocket.Conn) *wsTransport {
	return &wsTransport{
		ws:       ws,
		stopPing: make(chan struct{}),
	}
}

// startPinger installs the PongHandler + initial read deadline and
// launches the ping ticker goroutine. Safe to call exactly once per
// transport.
//
// idleReadTimeout SHOULD be at least 2× cadence so a single missed
// pong does not trip a false-positive disconnect.
func (t *wsTransport) startPinger(cadence, pingWriteTimeout, idleReadTimeout time.Duration) {
	t.idleReadTimeout = idleReadTimeout
	ws := t.ws

	// PingHandler + PongHandler both refresh the read deadline.
	// gorilla calls these from inside ReadMessage when the
	// corresponding control frame arrives — and crucially,
	// ReadMessage does NOT return on a control frame, so without
	// these the deadline would not refresh until a business frame
	// happens to arrive. The server-initiated ping/auto-pong loop is
	// the dominant traffic on an otherwise idle daemonbus; PongHandler
	// is the primary refresh path.
	defaultPingHandler := ws.PingHandler()
	ws.SetPingHandler(func(appData string) error {
		_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))
		return defaultPingHandler(appData)
	})
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))
		return nil
	})
	// Seed the initial deadline so even a silent peer post-upgrade
	// gets reaped after idleReadTimeout.
	_ = ws.SetReadDeadline(time.Now().Add(idleReadTimeout))

	go func() {
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()
		for {
			select {
			case <-t.stopPing:
				return
			case <-ticker.C:
				// WriteControl uses gorilla's internal control-frame
				// lock — it does NOT contend with wsTransport.mu
				// (application-frame lock). A pending huge business
				// write therefore cannot starve pings, and a stuck
				// ping cannot block other Sends. The deadline is the
				// "we expect kernel/TCP to ack this in N seconds"
				// floor; on failure we close the conn so ReadFrame
				// surfaces EOF and Connection.Run returns.
				err := ws.WriteControl(
					websocket.PingMessage,
					nil,
					time.Now().Add(pingWriteTimeout),
				)
				if err != nil {
					// Conn is in an undefined state; force close so
					// the reader side unwedges immediately.
					_ = ws.Close()
					return
				}
			}
		}
	}()
}

func (t *wsTransport) ReadFrame(ctx context.Context) (daemonbus.Frame, error) {
	_, data, err := t.ws.ReadMessage()
	if err != nil {
		return daemonbus.Frame{}, err
	}
	// Successful data frame = liveness signal. Refresh idle deadline
	// (mirror of the daemon-side floor in runtime/transit/wsclient.go).
	if t.idleReadTimeout > 0 {
		_ = t.ws.SetReadDeadline(time.Now().Add(t.idleReadTimeout))
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
	// Always cap the write with a deadline. Use the earlier of
	// ctx.Deadline (if any) and now+DefaultServerWSWriteTimeout. This
	// is the mirror image of the daemon-side fix (wsclient.go) — a
	// stuck TCP send buffer here would otherwise hang every gateway
	// handler waiting on SendAndAwait behind wsTransport.mu.
	deadline := time.Now().Add(DefaultServerWSWriteTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = t.ws.SetWriteDeadline(deadline)
	if err := t.ws.WriteMessage(websocket.TextMessage, data); err != nil {
		// Close the WS so subsequent writes fail fast and the
		// Connection.Run reader-side surfaces the disconnect.
		_ = t.ws.Close()
		return err
	}
	return nil
}

func (t *wsTransport) Close() error {
	t.stopOnce.Do(func() {
		if t.stopPing != nil {
			close(t.stopPing)
		}
	})
	return t.ws.Close()
}

// ensure compile-time interface satisfaction.
var (
	_ Transport = (*wsTransport)(nil)
	_           = errors.New
)
