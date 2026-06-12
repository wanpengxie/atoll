package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"log/slog"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// device.go is the outward (device) face: a PRIVATE WS endpoint the extension
// connect-ins to, the single accepted connection, the in-flight correlation
// table, the read loop, and the deadline reaper. This is the adapter's封闭
// transport — inlined here, NOT a shared framework piece (adapter-actor-spec §7).
//
// Concurrency: the cell goroutine (Receive) calls dispatch; the read loop +
// reaper run on their own goroutines. The mutex guards ONLY the in-flight table
// + the current conn pointer + the stopped flag — never a blocking wire write.
// dispatch registers under the lock, then writes the frame OUTSIDE the lock with
// a write deadline, so a stuck peer can never freeze the mutex (which the reaper,
// Stop, and accept all share). Channel emits (RespondJSON/Fail) go straight
// through the writer from whichever goroutine closes the request, because the
// substrate has no self-send (adapter-actor-spec §4).
//
// Device presence is tracked internally (conn==nil ⇒ offline ⇒ Receive fast-
// fails device_offline), but NOT projected as a channel event: there is no
// consumer yet and the audience semantics of a presence broadcast are undefined.
// Emitting it additively (with a real consumer + named audience) is left for when
// the pain is concrete — a rejected event is worse than no event.
//
// Liveness limit (v1): this endpoint serves a LOCAL loopback extension, so a
// dropped socket surfaces as a ReadJSON error and flips offline. Ping/pong
// keepalive + read deadlines (for a half-dead WAN peer that never sends FIN) are
// an additive hardening — see doc.go.

var deviceUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// maxDeviceFrameBytes caps one inbound device frame. The endpoint is local, so
// this is defensive, not adversarial.
const maxDeviceFrameBytes = 4 << 20 // 4 MiB

// deviceWriteTimeout bounds one downstream frame write. Real wall-clock (the
// socket deadline is real time, independent of the injectable logical clock).
const deviceWriteTimeout = 5 * time.Second

// pending is one in-flight request awaiting its device reply. The original
// request envelope is held in-hand so the closing path (read loop or reaper)
// can author the channel response/terminal without recovering it from truth.
type pending struct {
	request  *message.Envelope
	deadline time.Time
}

// device owns the adapter's outward transport.
type device struct {
	owner          *Actor // back-reference for sender()/writer
	addrCfg        string
	apiKey         string
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

func newDevice(owner *Actor, addr, apiKey string, clock func() time.Time, reaperInterval time.Duration, logger *slog.Logger) *device {
	return &device{
		owner:          owner,
		addrCfg:        addr,
		apiKey:         apiKey,
		clock:          clock,
		reaperInterval: reaperInterval,
		logger:         logger,
		inflight:       make(map[string]*pending),
		stopCh:         make(chan struct{}),
	}
}

// addr returns the resolved listen address (post-bind), or "" before Start.
func (d *device) addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.listener == nil {
		return ""
	}
	return d.listener.Addr().String()
}

// start binds the WS endpoint and boots the serve + reaper goroutines. A bind
// failure is returned so the cell dies fast.
func (d *device) start(ctx context.Context) error {
	ln, err := net.Listen("tcp", d.addrCfg)
	if err != nil {
		return fmt.Errorf("xhs: device listen %q: %w", d.addrCfg, err)
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
		d.logger.Warn("xhs.device.serve_stopped", "err", err.Error())
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

// handleAccept upgrades one extension connection. Auth = ?actor match + ?key
// match, FAIL CLOSED: an unconfigured key rejects all connections (the private
// endpoint must never degrade to actor-only auth). A new connection REPLACES the
// old one (one adapter, one device).
func (d *device) handleAccept(w http.ResponseWriter, r *http.Request) {
	if got := r.URL.Query().Get("actor"); got != string(d.owner.actorID) {
		http.Error(w, "actor mismatch", http.StatusForbidden)
		return
	}
	if d.apiKey == "" || r.URL.Query().Get("key") != d.apiKey {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
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

	d.logger.Info("xhs.device.online")
	go d.readLoop(conn)
}

// readLoop drains one connection's up-frames until it errors/closes. On exit it
// flips offline IFF it is still the live connection (pointer identity — a newer
// accept may have displaced it). In-flight requests are NOT failed here — the
// reaper collects them at their deadline (adapter-actor-spec §6.2: don't brute-
// fail on drop).
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
		d.logger.Info("xhs.device.offline")
	}
}

// handleUp matches one reply to its in-flight request and authors the channel
// terminal. An unknown correlation_id (already reaped, or a stray) is dropped.
// Taking the pending out of the table under the lock makes the close atomic: a
// correlation can be claimed by the read loop OR the reaper, never both.
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

	ctx := context.Background()
	if up.OK {
		var result any = map[string]any{}
		if len(up.Result) > 0 {
			result = up.Result
		}
		if _, err := behavior.RespondJSON(ctx, d.owner.writer, d.clock, p.request, d.owner.sender(), result); err != nil {
			d.logger.Warn("xhs.device.respond_failed", "correlation_id", up.CorrelationID, "err", err.Error())
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
	if _, err := behavior.Fail(ctx, d.owner.writer, d.clock, p.request, d.owner.sender(), code, detail); err != nil {
		d.logger.Warn("xhs.device.fail_failed", "correlation_id", up.CorrelationID, "err", err.Error())
	}
}

// dispatch encodes + registers + sends one request down to the device. It
// returns an error ONLY for the digestible offline case (no live conn); the
// caller (Receive) turns that into a device_offline business failure. A write
// error after registration is left to the reaper (the request stays in-flight
// and times out) rather than racing a second terminal.
func (d *device) dispatch(_ context.Context, env *message.Envelope, spec typeSpec, params json.RawMessage) error {
	frame := downFrame{
		CorrelationID: string(env.ID),
		Cmd:           spec.cmd,
		Params:        params,
	}

	d.mu.Lock()
	conn := d.conn
	if conn == nil {
		d.mu.Unlock()
		return errors.New("no xhs device connected")
	}
	// Register before sending so a fast reply can never find an empty table.
	reqCopy := *env
	d.inflight[string(env.ID)] = &pending{
		request:  &reqCopy,
		deadline: d.clock().Add(spec.deadline),
	}
	d.mu.Unlock()

	// Write OUTSIDE the lock, with a deadline: a stuck peer must never freeze the
	// mutex. Only the cell goroutine dispatches, so conn writes stay single-
	// writer (gorilla's rule). The socket deadline is REAL wall-clock, not the
	// injectable logical clock.
	_ = conn.SetWriteDeadline(time.Now().Add(deviceWriteTimeout))
	if err := conn.WriteJSON(frame); err != nil {
		// The frame did not reach the device. Leave the entry in-flight (the
		// reaper times it out); treat the conn as dead so the next dispatch sees
		// offline.
		d.logger.Warn("xhs.device.write_failed", "correlation_id", string(env.ID), "err", err.Error())
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
	d.logger.Info("xhs.device.offline")
}

// reaper sweeps the in-flight table for past-deadline requests and fails them
// with timeout. It is the one timeout authority (adapter-actor-spec §3.3) — the
// read loop never times out, the reaper never matches replies.
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
		if _, err := behavior.Fail(context.Background(), d.owner.writer, d.clock, p.request, d.owner.sender(), "timeout", "device did not reply within deadline"); err != nil {
			d.logger.Warn("xhs.device.timeout_fail_failed", "request_id", string(p.request.ID), "err", err.Error())
		}
	}
}
