package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"log/slog"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

// device.go is the outward (device) face: a PRIVATE WS endpoint the extension
// connect-ins to, the single accepted connection, the in-flight correlation
// table, the read loop, and the deadline reaper. This is the adapter's own
// closed transport — inlined here, NOT a shared framework piece.
//
// Concurrency: the worker goroutine (Actor.handle) calls dispatch; the read
// loop + reaper run on their own goroutines. The mutex guards ONLY the
// in-flight table + the current conn pointer + the stopped flag — never a
// blocking wire write. dispatch registers under the lock, then writes the
// frame OUTSIDE the lock with a write deadline, so a stuck peer can never
// freeze the mutex (which the reaper, stop, and accept all share). Terminal
// writes (sys.Reply/sys.Fail) run straight off whichever goroutine closes the
// request — legal fan-out (spec §1.2: Sys is concurrency-safe, Msg is
// immutable) — because the substrate has no self-send.
//
// Device presence is tracked internally (conn==nil ⇒ offline ⇒ handle fast-
// fails device_offline) AND pushed on its up/down edges via the actor-source obs
// PUSH axis (owner.publishDevicePresence → Sys.PublishObs, D7). That is
// OUT-OF-BAND obs (non-truth), NOT a channel event / truth-log entry — the home
// folds it into a volatile L3 level (advisory; authoritative reachability stays
// send→terminal). See actor.go publishDevicePresence.
//
// Liveness limit (v1): this endpoint serves a LOCAL loopback extension, so a
// dropped socket surfaces as a ReadJSON error and flips offline. Ping/pong
// keepalive + read deadlines (for a half-dead WAN peer that never sends FIN) are
// an additive hardening — see doc.go.

// deviceUpgrader gates the WS handshake. CheckOrigin closes the same-machine
// cross-origin hole the loopback bind alone cannot: loopback keeps OTHER machines
// out, but a malicious web page in the user's OWN browser is same-machine and can
// open a cross-origin WS to this keyless endpoint (displacing the real extension,
// stealing agent-issued commands). Origin defends against that page:
//   - empty Origin → allow. Non-browser clients (the real extension's
//     service-worker, our Go mock dialer) send no Origin header.
//   - "chrome-extension://…" → allow. The real browser extension.
//   - anything else (http/https web pages) → reject.
var deviceUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "" || strings.HasPrefix(origin, "chrome-extension://")
	},
}

// maxDeviceFrameBytes caps one inbound device frame. The endpoint is local, so
// this is defensive, not adversarial.
const maxDeviceFrameBytes = 4 << 20 // 4 MiB

// deviceWriteTimeout bounds one downstream frame write. Real wall-clock (the
// socket deadline is real time, independent of the injectable logical clock).
const deviceWriteTimeout = 5 * time.Second

// pending is one in-flight request awaiting its device reply. The original
// delivery is held in-hand (as its Msg, not a re-fetched envelope) so the
// closing path (read loop or reaper) can call sys.Reply/sys.Fail directly.
type pending struct {
	request  actorbase.Msg
	deadline time.Time
}

// device owns the adapter's outward transport.
type device struct {
	owner          *Actor // back-reference for sys + clock
	addrCfg        string
	clock          func() time.Time
	reaperInterval time.Duration
	logger         *slog.Logger

	srv      *http.Server
	listener net.Listener

	mu       sync.Mutex
	conn     *websocket.Conn // current device connection; nil ⇒ offline
	inflight map[string]*pending
	stopped  bool

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newDevice(owner *Actor, addr string, clock func() time.Time, reaperInterval time.Duration, logger *slog.Logger) *device {
	return &device{
		owner:          owner,
		addrCfg:        addr,
		clock:          clock,
		reaperInterval: reaperInterval,
		logger:         logger,
		inflight:       make(map[string]*pending),
		stopCh:         make(chan struct{}),
	}
}

// addr returns the resolved listen address (post-bind), or "" before run() binds.
func (d *device) addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.listener == nil {
		return ""
	}
	return d.listener.Addr().String()
}

// start binds the WS endpoint and boots the serve + reaper goroutines. A bind
// failure is returned so the incarnation dies fast. ctx is the process-life ctx
// (sys.Life()) — the reaper's own long-lived scope.
func (d *device) start(ctx context.Context) error {
	ln, err := net.Listen("tcp", d.addrCfg)
	if err != nil {
		return fmt.Errorf("kimi: device listen %q: %w", d.addrCfg, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/device", d.handleAccept)
	d.mu.Lock()
	d.listener = ln
	d.srv = &http.Server{Handler: mux}
	d.mu.Unlock()

	d.wg.Add(2)
	go d.serve(ln)
	go d.reaper(ctx)
	return nil
}

func (d *device) serve(ln net.Listener) {
	defer d.wg.Done()
	// Serve returns ErrServerClosed on graceful shutdown — expected, not a fault.
	if err := d.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		d.logger.Warn("kimi.device.serve_stopped", "err", err.Error())
	}
}

// stop shuts the endpoint, closes the conn, and joins the goroutines. The
// stopped flag is set under the lock BEFORE wg.Wait so a concurrent handleAccept
// either observes it (and bails without wg.Add) or already counted itself — Add
// can never race Wait.
func (d *device) stop(ctx context.Context) error {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		d.mu.Lock()
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
	})
	d.wg.Wait()
	return nil
}

// handleAccept upgrades one extension connection. A new connection REPLACES the
// old one (one adapter, one device).
//
// Trust model (default-trust-local): the endpoint binds loopback (127.0.0.1),
// so only same-machine processes — the user's local browser extension reached
// through the local daemon — can connect. That bind IS the entire trust
// boundary; there is no key (pre-launch minimal). Real authn (multi-user /
// remote) is an additive layer if ever needed.
func (d *device) handleAccept(w http.ResponseWriter, r *http.Request) {
	conn, err := deviceUpgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the error response.
		return
	}

	d.mu.Lock()
	if d.stopped {
		// Stop already ran (it does not wait for hijacked conns). Discard this
		// late connection rather than register a goroutine past wg.Wait.
		d.mu.Unlock()
		_ = conn.Close()
		return
	}
	if old := d.conn; old != nil {
		_ = old.Close() // displace the previous device
	}
	d.conn = conn
	// wg.Add lives in the SAME critical section as the stopped check, so it can
	// never race the wg.Wait in stop().
	d.wg.Add(1)
	d.mu.Unlock()

	d.logger.Info("kimi.device.online", "actor", string(d.owner.sys.Self()))
	d.owner.publishDevicePresence(true)
	go d.readLoop(conn)
}

// readLoop drains one connection's up-frames until it errors/closes. On exit it
// flips offline IFF it is still the live connection (pointer identity — a newer
// accept may have displaced it). In-flight requests are NOT failed here — the
// reaper collects them at their deadline instead — don't brute-fail on drop.
func (d *device) readLoop(conn *websocket.Conn) {
	defer d.wg.Done()
	conn.SetReadLimit(maxDeviceFrameBytes)
	for {
		var up upFrame
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
		d.logger.Info("kimi.device.offline", "actor", string(d.owner.sys.Self()))
		d.owner.publishDevicePresence(false)
	}
}

// handleUp matches one reply to its in-flight request and authors the channel
// terminal directly through sys.Reply/sys.Fail. An unknown correlation_id
// (already reaped, or a stray) is dropped. Taking the pending out of the table
// under the lock makes the close atomic: a correlation can be claimed by the
// read loop OR the reaper, never both.
//
// LATE-REPLY NOTE (actorbase migration): a reply landing after its request
// already closed no longer writes the pen blindly and lets the terminal-
// uniqueness index arbitrate — sys.Reply/sys.Fail now judge locally first,
// returning ErrRequestClosed without a write (spec §1.5's local判定先行). A
// behaviour change from the old direct-write form, closer to spec, recorded
// here.
func (d *device) handleUp(up upFrame) {
	d.mu.Lock()
	p, ok := d.inflight[up.CorrelationID]
	if ok {
		delete(d.inflight, up.CorrelationID)
	}
	d.mu.Unlock()
	if !ok {
		return
	}

	if up.OK {
		var result any = map[string]any{}
		if len(up.Result) > 0 {
			result = up.Result
		}
		if _, err := d.owner.sys.Reply(p.request, result); err != nil {
			d.logger.Warn("kimi.device.respond_failed", "correlation_id", up.CorrelationID, "err", err.Error())
		}
		return
	}

	code, detail := "device_error", ""
	if up.Error != nil {
		if up.Error.Code != "" {
			code = up.Error.Code
		}
		detail = up.Error.Message
	}
	if _, err := d.owner.sys.Fail(p.request, code, detail); err != nil {
		d.logger.Warn("kimi.device.fail_failed", "correlation_id", up.CorrelationID, "err", err.Error())
	}
}

// dispatch encodes + registers + sends one request down to the device. It
// returns an error ONLY for the digestible offline case (no live conn); the
// caller (Actor.handle) turns that into a device_offline business failure. A
// write error after registration is left to the reaper (the request stays
// in-flight and times out) rather than racing a second terminal.
//
// Unlike xhs, the device verb (cmd) is not a static per-type constant — it is
// the request's `action`, resolved by Actor.handle — so dispatch takes cmd +
// deadline as explicit arguments.
func (d *device) dispatch(msg actorbase.Msg, cmd string, deadline time.Duration, params json.RawMessage) error {
	frame := downFrame{
		CorrelationID: string(msg.ID),
		Cmd:           cmd,
		Params:        params,
	}

	d.mu.Lock()
	conn := d.conn
	if conn == nil {
		d.mu.Unlock()
		return errors.New("no kimi device connected")
	}
	// Register before sending so a fast reply can never find an empty table.
	d.inflight[string(msg.ID)] = &pending{
		request:  msg,
		deadline: d.clock().Add(deadline),
	}
	d.mu.Unlock()

	// Write OUTSIDE the lock, with a deadline: a stuck peer must never freeze the
	// mutex. Only the worker goroutine dispatches, so conn writes stay single-
	// writer (gorilla's rule). The socket deadline is REAL wall-clock, not the
	// injectable logical clock.
	_ = conn.SetWriteDeadline(time.Now().Add(deviceWriteTimeout))
	if err := conn.WriteJSON(frame); err != nil {
		// The frame did not reach the device. Leave the entry in-flight (the
		// reaper times it out); treat the conn as dead so the next dispatch sees
		// offline.
		d.logger.Warn("kimi.device.write_failed", "correlation_id", string(msg.ID), "err", err.Error())
		d.dropConn(conn)
	}
	return nil
}

// dropConn closes conn IFF it is still the live one (pointer identity), flipping
// offline. Safe to call from any goroutine; idempotent against a conn already
// displaced or reaped.
func (d *device) dropConn(conn *websocket.Conn) {
	d.mu.Lock()
	if d.conn != conn {
		d.mu.Unlock()
		return
	}
	d.conn = nil
	d.mu.Unlock()
	_ = conn.Close()
	d.logger.Info("kimi.device.offline", "actor", string(d.owner.sys.Self()))
	d.owner.publishDevicePresence(false)
}

// reaper sweeps the in-flight table for past-deadline requests and fails them
// with timeout. It is the one timeout authority — the read loop never times
// out, the reaper never matches replies.
func (d *device) reaper(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.sweep()
		}
	}
}

func (d *device) sweep() {
	now := d.clock()
	var expired []*pending
	d.mu.Lock()
	for id, p := range d.inflight {
		if now.After(p.deadline) {
			expired = append(expired, p)
			delete(d.inflight, id)
		}
	}
	d.mu.Unlock()

	for _, p := range expired {
		if _, err := d.owner.sys.Fail(p.request, "timeout", "device did not reply within deadline"); err != nil {
			d.logger.Warn("kimi.device.timeout_fail_failed", "request_id", string(p.request.ID), "err", err.Error())
		}
	}
}
