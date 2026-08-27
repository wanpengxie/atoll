// Package plugindevice is the shared outward face for browser-plugin tool
// adapters: the private WS endpoint a plugin connects in to, the single
// accepted connection, the in-flight correlation table, and the read loop.
//
// It exists because xhs and kimi had grown two copies of it that were, with the
// tool name normalised away, 87% identical line for line — and the listen
// address was about to become settable from the channel, which would have made
// that a third and fourth copy of the same rebind and hardening logic. What
// stays in each adapter is only its DIALECT: which inward type maps to which
// device verb, with what budget, and what its words are called.
//
// Concurrency (unchanged from the adapters this came from): the worker
// goroutine calls Dispatch, the actor-owned maintenance goroutine runs Sweep,
// and the read loop has its own goroutine. The mutex guards ONLY the in-flight
// table + the current conn pointer + the stopped flag — never a blocking wire
// write. Dispatch registers under the lock, then writes the frame OUTSIDE it
// with a write deadline, so a stuck peer can never freeze the mutex.
package plugindevice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

// upgrader gates the WS handshake. CheckOrigin closes the same-machine
// cross-origin hole a loopback bind alone cannot: loopback keeps OTHER machines
// out, but a malicious web page in the user's OWN browser is same-machine and
// can open a cross-origin WS to this keyless endpoint (displacing the real
// plugin, stealing agent-issued commands). Origin defends against that page:
//   - empty Origin → allow. Non-browser clients (the real plugin's service
//     worker, a Go dialer, the端侧 forwarder) send no Origin header.
//   - "chrome-extension://…" → allow. The real browser extension.
//   - anything else (http/https web pages) → reject.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "" || strings.HasPrefix(origin, "chrome-extension://")
	},
}

// maxFrameBytes caps one inbound device frame.
const maxFrameBytes = 4 << 20 // 4 MiB

// writeTimeout bounds one downstream frame write. Real wall-clock (the socket
// deadline is real time, independent of the injectable logical clock).
const writeTimeout = 5 * time.Second

// Keepalive bounds a connection that stops answering. It is armed ONLY for a
// non-loopback bind, and that condition is the whole point: on loopback a dead
// peer surfaces immediately as a closed socket, while a half-dead WAN peer can
// hold the single connection slot forever without ever sending FIN — and the
// slot is exclusive, so holding it keeps the REAL plugin out.
const (
	pongWait   = 60 * time.Second
	pingPeriod = 25 * time.Second
)

// Spec is one inward type's dialect binding: which device verb it becomes and
// how long the device has to answer.
type Spec struct {
	Cmd      string
	Deadline time.Duration
}

// Deps is what an adapter supplies. Sys is a getter rather than a value because
// an actor's Sys is bound at run() (birth), after the device is constructed.
type Deps struct {
	// Tool names the adapter in logs and error text ("xhs", "kimi", …).
	Tool string
	// Sys returns the live Sys; it may return nil before birth.
	Sys func() actorbase.Sys
	// Clock is the injectable logical clock the reaper reads.
	Clock func() time.Time
	// Logger surfaces device-face edges. nil → discard.
	Logger *slog.Logger
	// OnPresence is called on the online/offline edges of the connection.
	OnPresence func(online bool)
}

type pending struct {
	request  actorbase.Msg
	deadline time.Time
}

// Device owns one adapter's outward transport.
type Device struct {
	deps Deps

	mu       sync.Mutex
	desired  string
	srv      *http.Server
	listener net.Listener
	conn     *websocket.Conn // current plugin connection; nil ⇒ offline
	inflight map[string]*pending
	stopped  bool

	wg sync.WaitGroup
}

func New(deps Deps) *Device {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	return &Device{deps: deps, inflight: make(map[string]*pending)}
}

// Addr is the resolved listen address (post-bind), or "" before Bind lands.
func (d *Device) Addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.listener == nil {
		return ""
	}
	return d.listener.Addr().String()
}

// Desired is the address this device was last asked to listen on, which differs
// from Addr while a bind is being retried.
func (d *Device) Desired() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.desired
}

// Online reports whether a plugin connection is currently registered.
func (d *Device) Online() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conn != nil
}

// Bind listens on addr and boots the serve goroutine. A bind failure is
// returned so the caller's maintenance loop can retry — the port may still be
// held by a predecessor incarnation.
func (d *Device) Bind(addr string) error {
	if err := ValidateAddr(addr); err != nil {
		return err
	}
	// Record the intent BEFORE attempting the listen: while the retry loop is
	// still fighting a predecessor for the port, "what is this endpoint trying
	// to be" is exactly the question being asked, and answering "" would hide
	// the only useful fact.
	d.mu.Lock()
	d.desired = addr
	d.mu.Unlock()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("%s: device listen %q: %w", d.deps.Tool, addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/device", d.handleAccept)
	srv := &http.Server{Handler: mux}
	d.mu.Lock()
	d.listener = ln
	d.srv = srv
	d.mu.Unlock()

	d.wg.Add(1)
	go d.serve(srv, ln)
	return nil
}

// Rebind moves the endpoint to addr atomically from the caller's point of view:
// the new address is bound FIRST, and only a successful bind swaps it in. A
// failure leaves the old endpoint exactly as it was — which is the whole
// contract, because a bad address must never cost the operator a working
// listener they cannot reach to fix.
//
// The old connection is closed on a successful swap: the endpoint it arrived
// through no longer exists, so the plugin reconnects through ordinary
// disconnect handling rather than being told a comforting lie about still
// being attached.
func (d *Device) Rebind(ctx context.Context, addr string) (string, error) {
	if err := ValidateAddr(addr); err != nil {
		return "", err
	}
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return "", errors.New(d.deps.Tool + ": device is shutting down")
	}
	current := d.listener
	d.mu.Unlock()
	if current != nil && current.Addr().String() == addr {
		return addr, nil // already there; nothing to disturb
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("%s: device listen %q: %w", d.deps.Tool, addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/device", d.handleAccept)
	srv := &http.Server{Handler: mux}

	d.mu.Lock()
	oldSrv, oldConn := d.srv, d.conn
	d.desired, d.listener, d.srv, d.conn = addr, ln, srv, nil
	d.mu.Unlock()

	d.wg.Add(1)
	go d.serve(srv, ln)

	if oldConn != nil {
		_ = oldConn.Close()
		d.deps.OnPresence(false)
	}
	if oldSrv != nil {
		_ = oldSrv.Shutdown(ctx)
	}
	d.deps.Logger.Info(d.deps.Tool+".device.rebound", "addr", ln.Addr().String())
	return ln.Addr().String(), nil
}

// serve runs one listener under the server it was bound with. Both are passed
// in rather than read back off the struct, so a Rebind landing in between can
// never make this goroutine serve one incarnation's listener under another's
// server — which would leave a listener nobody holds a handle to shut down.
func (d *Device) serve(srv *http.Server, ln net.Listener) {
	defer d.wg.Done()
	// Serve returns ErrServerClosed on graceful shutdown — expected, not a fault.
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		d.deps.Logger.Warn(d.deps.Tool+".device.serve_stopped", "err", err.Error())
	}
}

// Stop shuts the endpoint, closes the conn, and joins the goroutines. The
// stopped flag is set under the lock BEFORE wg.Wait so a concurrent accept
// either observes it (and bails without wg.Add) or already counted itself.
// Idempotent + safe when the endpoint never bound.
func (d *Device) Stop(ctx context.Context) error {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		d.wg.Wait()
		return nil
	}
	d.stopped = true
	conn := d.conn
	d.conn = nil
	srv := d.srv
	d.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if srv != nil {
		_ = srv.Shutdown(ctx)
	}
	d.wg.Wait()
	return nil
}

// handleAccept upgrades one plugin connection. A new connection REPLACES the
// old one (one adapter, one device).
//
// Trust model: the endpoint is KEYLESS, so whatever can reach the bound address
// can drive it. On the loopback default that is the same machine. An operator
// who binds a routable address has moved the trust boundary to that network on
// purpose — see ValidateAddr.
func (d *Device) handleAccept(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the error response.
	}

	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		_ = conn.Close()
		return
	}
	if old := d.conn; old != nil {
		_ = old.Close() // displace the previous device
	}
	d.conn = conn
	exposed := !IsLoopbackAddr(d.desired)
	// wg.Add lives in the SAME critical section as the stopped check, so it can
	// never race the wg.Wait in Stop.
	d.wg.Add(1)
	d.mu.Unlock()

	d.deps.Logger.Info(d.deps.Tool+".device.online", "actor", d.selfID())
	d.deps.OnPresence(true)
	go d.readLoop(conn, exposed)
}

// readLoop drains one connection's up-frames until it errors/closes. On exit it
// flips offline IFF it is still the live connection (pointer identity — a newer
// accept may have displaced it). In-flight requests are NOT failed here — the
// reaper collects them at their deadline.
func (d *Device) readLoop(conn *websocket.Conn, exposed bool) {
	defer d.wg.Done()
	conn.SetReadLimit(maxFrameBytes)
	if exposed {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})
		stopPing := make(chan struct{})
		defer close(stopPing)
		go d.pingLoop(conn, stopPing)
	}
	for {
		var up UpFrame
		if err := conn.ReadJSON(&up); err != nil {
			break
		}
		d.handleUp(up)
	}

	d.mu.Lock()
	live := d.conn == conn
	if live {
		d.conn = nil
	}
	d.mu.Unlock()
	if live {
		_ = conn.Close()
		d.deps.Logger.Info(d.deps.Tool+".device.offline", "actor", d.selfID())
		d.deps.OnPresence(false)
	}
}

// pingLoop keeps a routable connection honest. Writes go through the same
// single-writer discipline as Dispatch by way of the write deadline; a failed
// ping drops the connection rather than leaving the exclusive slot occupied by
// something that will never answer.
func (d *Device) pingLoop(conn *websocket.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				d.DropConn(conn)
				return
			}
		}
	}
}

// handleUp matches one reply to its in-flight request and authors the channel
// terminal directly. An unknown correlation_id (already reaped, or a stray) is
// dropped. Taking the pending out of the table under the lock makes the close
// atomic: a correlation can be claimed by the read loop OR the reaper, never
// both.
func (d *Device) handleUp(up UpFrame) {
	d.mu.Lock()
	p, ok := d.inflight[up.CorrelationID]
	if ok {
		delete(d.inflight, up.CorrelationID)
	}
	d.mu.Unlock()
	if !ok {
		return
	}
	sys := d.deps.Sys()
	if sys == nil {
		return
	}
	if up.OK {
		if _, err := sys.Reply(p.request, up.Result); err != nil {
			d.deps.Logger.Warn(d.deps.Tool+".device.reply_failed", "correlation_id", up.CorrelationID, "err", err.Error())
		}
		return
	}
	code, detail := "device_error", "device reported a failure"
	if up.Error != nil {
		if up.Error.Code != "" {
			code = up.Error.Code
		}
		if up.Error.Message != "" {
			detail = up.Error.Message
		}
	}
	if _, err := sys.Fail(p.request, code, detail); err != nil {
		d.deps.Logger.Warn(d.deps.Tool+".device.fail_failed", "correlation_id", up.CorrelationID, "err", err.Error())
	}
}

// Dispatch sends one inward request down to the plugin and registers it as
// in-flight.
func (d *Device) Dispatch(msg actorbase.Msg, spec Spec, params json.RawMessage) error {
	frame := DownFrame{CorrelationID: string(msg.ID), Cmd: spec.Cmd, Params: params}

	d.mu.Lock()
	conn := d.conn
	if conn == nil {
		d.mu.Unlock()
		return errors.New("no " + d.deps.Tool + " device connected")
	}
	// Register before sending so a fast reply can never find an empty table.
	d.inflight[string(msg.ID)] = &pending{request: msg, deadline: d.deps.Clock().Add(spec.Deadline)}
	d.mu.Unlock()

	// Write OUTSIDE the lock, with a deadline: a stuck peer must never freeze
	// the mutex. Only the worker goroutine dispatches, so conn writes stay
	// single-writer. The socket deadline is REAL wall-clock.
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteJSON(frame); err != nil {
		// The frame did not reach the device. Leave the entry in-flight (the
		// reaper times it out); treat the conn as dead so the next dispatch
		// sees offline.
		d.deps.Logger.Warn(d.deps.Tool+".device.write_failed", "correlation_id", string(msg.ID), "err", err.Error())
		d.DropConn(conn)
	}
	return nil
}

// DropConn closes conn IFF it is still the live one (pointer identity),
// flipping offline. Safe from any goroutine; idempotent.
func (d *Device) DropConn(conn *websocket.Conn) {
	d.mu.Lock()
	if d.conn != conn {
		d.mu.Unlock()
		return
	}
	d.conn = nil
	d.mu.Unlock()
	_ = conn.Close()
	d.deps.Logger.Info(d.deps.Tool+".device.offline", "actor", d.selfID())
	d.deps.OnPresence(false)
}

// Sweep fails every past-deadline in-flight request with timeout. It is the one
// timeout authority — the read loop never times out, Sweep never matches
// replies.
func (d *Device) Sweep() {
	now := d.deps.Clock()
	var expired []*pending
	d.mu.Lock()
	for id, p := range d.inflight {
		if now.After(p.deadline) {
			expired = append(expired, p)
			delete(d.inflight, id)
		}
	}
	d.mu.Unlock()

	sys := d.deps.Sys()
	if sys == nil {
		return
	}
	for _, p := range expired {
		if _, err := sys.Fail(p.request, "timeout", "device did not reply within deadline"); err != nil {
			d.deps.Logger.Warn(d.deps.Tool+".device.timeout_fail_failed", "request_id", string(p.request.ID), "err", err.Error())
		}
	}
}

func (d *Device) selfID() string {
	if sys := d.deps.Sys(); sys != nil {
		return string(sys.Self())
	}
	return ""
}

// ValidateAddr refuses the two addresses that are mistakes rather than choices.
//
// A wildcard bind ("", 0.0.0.0, ::) is refused outright: this endpoint is
// keyless, so "listen everywhere" hands it to every network the host happens to
// be on, including ones the operator never thought about. Naming the specific
// address keeps the trust boundary something that was decided rather than
// inherited.
//
// A routable address is ALLOWED — that is the point of making this settable —
// but it is a decision with consequences, and the ones that matter are:
// whatever can reach that address can drive the plugin, and a new connection
// displaces the old one. On the loopback default those read as "a process on
// this machine"; on a tailnet they read as "anything on the tailnet".
func ValidateAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen_addr must be host:port: %w", err)
	}
	if port == "" {
		return errors.New("listen_addr must name a port")
	}
	switch host {
	case "":
		return errors.New(`listen_addr must name an address; a wildcard bind would expose this keyless endpoint on every network this host is on`)
	case "0.0.0.0", "::":
		return fmt.Errorf(`listen_addr %q is a wildcard; name the specific address to listen on, because this endpoint has no key and a wildcard bind exposes it on every network this host is on`, host)
	}
	if host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("listen_addr host %q is not an IP address", host)
	}
	return nil
}

// IsLoopbackAddr reports whether host:port binds the loopback interface. An
// unparseable or hostname host is treated as non-loopback (warn, don't guess).
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
