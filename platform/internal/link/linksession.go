package link

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// linksession.go is 期11 片②'s "换底": one top-level yamux.Session rides directly
// on a wsByteStream (the raw WS connection as a byte stream) and REPLACES the
// retired self-rolled mux that used to live in frame.go (now deleted). yamux
// gives us reliable, windowed, bidirectional substreams for free; we no longer
// hand-roll the frame codec, the per-stream buffers, or the demux loop.
//
// yamux's Open/Accept carry NO metadata, so a substream cannot say "I am the
// control plane / an actor stream / a lane carrier" out of band. Each substream
// therefore opens with a self-describing streamHeader{Kind} as its FIRST bytes
// (written with lane.go's writeLaneJSON / read with readLaneJSON — the SAME
// newline-JSON, zero-over-read framing the lane redeem header already uses, so
// the raw byte flow that follows the header is never swallowed). The accept loop
// reads that header and dispatches by Kind — never by "which stream number"
// (a positional convention would race the lane, which opens at arbitrary times).
//
// Three logical planes share the ONE session:
//   - control : a single long-lived substream carrying the stream-0 control JSON
//     the old ControlStream did (attach/attach_reply + the §4.7 storage frames +
//     §5's ResolveCoord + the idle ping). Opened by the daemon (client), accepted
//     by the home (server); used bidirectionally.
//   - actor   : one substream per hosted actor, running native ipc (zero
//     translation — ipc.Codec rides the substream exactly as it rode a *stream).
//   - lane    : §5's resource-lane redeem stream. 片③ FLATTENED the lane: each
//     redeem is its OWN top-level substream tagged lane (no nested yamux-in-
//     yamux any more). The requester daemon opens one toward the home (a redeem
//     attempt), and the home opens one toward the transfer's TARGET daemon (the
//     relay). Both ends dispatch tag=lane through onLane, ASYMMETRICALLY by
//     role: a daemon-opened lane stream reaching the home is a redeem attempt
//     (→ handleLaneRedeem); a home-opened lane stream reaching a daemon is an
//     inbound transfer that daemon is the target of (→ handleLaneInbound). The
//     laneRedeemHeader still rides the substream right after this streamHeader
//     (two newline-JSON reads back to back — streamHeader in dispatch, then the
//     lane header in the handler — readLaneJSON's zero-over-read makes that safe).
//
// linkSession is the unexported wrapper both ends build (Acceptor/Dialer hold a
// *linkSession, never a *yamux.Session directly) — this is also what keeps the
// archtest red line green: no yamux type appears in any exported signature.

// streamKind tags a substream's plane in its opening streamHeader.
type streamKind string

const (
	streamControl streamKind = "control"
	streamActor   streamKind = "actor"
	streamLane    streamKind = "lane"
)

const streamWriteBudget = 10 * time.Second
const controlQueueDepth = 64

type boundedConn struct {
	net.Conn
	logger *slog.Logger
}

func (c *boundedConn) Write(p []byte) (int, error) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(streamWriteBudget))
	n, err := c.Conn.Write(p)
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("link.stream_write_failed", "error", err)
		}
		_ = c.Conn.Close()
	}
	return n, err
}

// streamHeader is the first newline-JSON value written on every substream, read
// by the accept loop to dispatch the substream to its plane.
type streamHeader struct {
	Kind streamKind `json:"kind"`
}

func writeStreamHeader(w io.Writer, k streamKind) error {
	return writeLaneJSON(w, streamHeader{Kind: k})
}

// errLinkClosed is returned by a control send once the underlying session is
// gone (its control substream never established, or already torn down).
var errLinkClosed = errors.New("link: closed")

// yamuxKeepAliveInterval overrides yamux's own DefaultConfig KeepAliveInterval
// (30s) when non-zero (nanoseconds, atomic — linkYamuxConfig reads it from
// whichever goroutine builds a session). Production never sets it — this
// exists SOLELY so a test can speed yamux's real keepalive traffic up
// (export_test.go's SetYamuxKeepAliveIntervalForTest) to prove the Lease's TTL
// judgment survives independently of it, without waiting out a real 30s
// cadence.
var yamuxKeepAliveIntervalNS atomic.Int64

// linkYamuxConfig is the top-level session's config. EnableKeepAlive stays at
// DefaultConfig's true.
//
// TWO HEARTBEATS, DELIBERATELY NOT MERGED (期11 片② hard constraint):
//   - yamux keepalive (this config, 30s ping) probes the underlying CONNECTION —
//     "is the TCP/WS pipe and the peer's yamux stack still there". yamux answers
//     it entirely inside its own session loop — the ping/pong never surfaces to
//     a substream's Read, so it can never reach onFrame below.
//   - the Lease (lease.go, 10s ping / 30s TTL, refreshed via dispatch's per-
//     substream onFrame hook below — never the raw wsByteStream carrier) probes
//     the peer's APPLICATION response — the daemon's own pingLoop (dial.go) on
//     the control substream, or any actor ipc frame. A frozen-app-but-live-
//     socket daemon keeps answering yamux pings yet stops producing app frames,
//     so the Lease is the STRICTLY STRONGER liveness judgment. Do NOT delete the
//     Lease in favour of yamux keepalive, and do NOT refresh it from raw carrier
//     bytes — either merges the two heartbeats and defeats the Lease's one job
//     (a bug this file once had: onRead fired on every wsByteStream.Read, which
//     included yamux's own keepalive bytes, so a frozen app was kept "alive" by
//     keepalive alone).
func linkYamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard // route yamux's own logging away from stderr
	if ns := yamuxKeepAliveIntervalNS.Load(); ns > 0 {
		cfg.KeepAliveInterval = time.Duration(ns)
	}
	return cfg
}

// linkSession is the yamux-backed link mux. It preserves the exact seams the
// retired self-rolled mux exposed: sendControl (framed control JSON on the
// single control substream), openStream (a fresh tag=actor substream, daemon
// side), openLane (the tag=lane redeem substream), a closed() channel that
// fires on session death, and Close.
type linkSession struct {
	ys *yamux.Session

	// ctrl is the single bidirectional control substream (opened by the daemon,
	// accepted by the home). ctrlMu serialises writes to it (sendControl may be
	// called from many goroutines — pingLoop, the storage/lane RPC senders, the
	// reply paths) and guards the home-side lazy assignment (the home learns ctrl
	// only when the accept loop dispatches the control-tagged substream).
	ctrlMu sync.Mutex
	ctrl   net.Conn

	onControl func([]byte)   // inbound control JSON (both ends)
	onActor   func(net.Conn) // peer-opened actor substream (home only; nil daemon side)
	onLane    func(net.Conn) // peer-opened lane redeem substream (BOTH ends: home → redeem relay, daemon → inbound-target handler)

	// onFrame, when non-nil, is the Lease's liveness refresh (lease.go). dispatch
	// wraps every peer-opened substream so onFrame fires on that substream's OWN
	// Read calls — i.e. only when a real application frame is delivered (control-
	// plane JSON, including the daemon's app-level idle ping; an actor's ipc
	// frame; or a lane redeem's header/bytes). yamux's own keepalive ping/pong
	// never reaches a substream's Read (yamux consumes it inside the session
	// loop), so it can never fire this. nil on the daemon side (holds no Lease).
	onFrame func()

	logger   *slog.Logger
	killOnce sync.Once
}

// dialLinkSession builds the daemon (client) end: a yamux client over the raw WS
// byte stream, then the control substream opened + tagged FIRST (the attach JSON
// rides it right after). The daemon holds no Lease, so its onFrame stays nil
// (dispatch's wrap is a no-op when onFrame is nil). onLane routes a HOME-opened
// lane substream (the daemon is that transfer's target) to the daemon's inbound-
// target handler — the daemon opens no actor/control substreams for the home to
// accept, so it needs no onActor.
func dialLinkSession(ws *websocket.Conn, onControl func([]byte), onLane func(net.Conn), logger *slog.Logger) (*linkSession, error) {
	ys, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig())
	if err != nil {
		return nil, err
	}
	ls := &linkSession{ys: ys, onControl: onControl, onLane: onLane, logger: logger}
	ctrl, err := ys.Open()
	if err != nil {
		_ = ys.Close()
		return nil, err
	}
	ctrl = &boundedConn{Conn: ctrl, logger: logger}
	if err := writeStreamHeader(ctrl, streamControl); err != nil {
		_ = ys.Close()
		return nil, err
	}
	ls.ctrlMu.Lock()
	ls.ctrl = ctrl
	ls.ctrlMu.Unlock()
	return ls, nil
}

// acceptLinkSession builds the home (server) end: a yamux server over the raw WS
// byte stream, then the accept loop that discovers and dispatches every
// substream the daemon opens. onFrame is the Lease refresh — dispatch fires it
// per application frame on whichever substream carries it, never on the raw
// carrier (see linkSession.onFrame's doc). The control substream arrives via
// that loop (tag=control), never opened here.
func acceptLinkSession(ws *websocket.Conn, onControl func([]byte), onActor, onLane func(net.Conn), onFrame func(), logger *slog.Logger) (*linkSession, error) {
	ys, err := yamux.Server(newWSByteStream(ws), linkYamuxConfig())
	if err != nil {
		return nil, err
	}
	ls := &linkSession{ys: ys, onControl: onControl, onActor: onActor, onLane: onLane, onFrame: onFrame, logger: logger}
	return ls, nil
}

// frameHookConn wraps one yamux substream so onFrame fires on every Read that
// actually yielded bytes FROM THAT SUBSTREAM — i.e. an application frame, never
// yamux's own keepalive ping/pong (which yamux answers inside its session loop,
// below any substream, so it never reaches here). dispatch wraps every peer-
// opened substream with this before routing it to control/actor/lane — this is
// the Lease's ONLY refresh source (lease.go), deliberately one layer above the
// raw wsByteStream carrier.
type frameHookConn struct {
	net.Conn
	onFrame func()
}

func (c *frameHookConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.onFrame()
	}
	return n, err
}

// start launches the session's read/accept loops. It is called by the owner
// ONLY after the *linkSession is assigned into the Dialer/Acceptor's lc field —
// so onControl (which reaches back through lc, e.g. to sendControl a reply) can
// never fire against a not-yet-assigned lc. The daemon opened its own control
// substream (readControl it directly); the home discovers the control substream
// via the accept loop (dispatch starts its readControl there), so only the
// daemon has a non-nil ls.ctrl at start.
func (ls *linkSession) start() {
	if ls.ctrl != nil {
		go ls.readControl(ls.ctrl)
	}
	go ls.acceptLoop()
}

// acceptLoop accepts every peer-opened substream and dispatches it OFF the loop
// (one goroutine per substream): the header read blocks, so a substream that
// opens but never sends its header must not stall the loop for the others.
func (ls *linkSession) acceptLoop() {
	for {
		conn, err := ls.ys.Accept()
		if err != nil {
			return // session dead — closed() has fired for waiters
		}
		go ls.dispatch(conn)
	}
}

// dispatch reads a freshly-accepted substream's streamHeader and routes it to its
// plane. An unreadable header or an out-of-set Kind closes the substream (never a
// silent hang).
//
// Every peer-opened substream funnels through here exactly once, so this is
// also the single point that wraps the substream with the Lease's onFrame hook
// (frameHookConn, below) BEFORE the header read — the header itself is a real
// frame the peer sent, and every kind this switch dispatches to (control JSON
// including the daemon's app-level idle ping, an actor's ipc frames, a lane
// redeem's header/bytes) is exactly the "application frame" set onFrame's doc
// promises. The wrap is a no-op when ls.onFrame is nil (daemon side).
func (ls *linkSession) dispatch(conn net.Conn) {
	conn = &boundedConn{Conn: conn, logger: ls.logger}
	if ls.onFrame != nil {
		conn = &frameHookConn{Conn: conn, onFrame: ls.onFrame}
	}
	var hdr streamHeader
	if err := readLaneJSON(conn, &hdr); err != nil {
		_ = conn.Close()
		return
	}
	switch hdr.Kind {
	case streamControl:
		// The peer's control substream: adopt it for sends, then drive its reads
		// for the substream's life (this goroutine parks here).
		ls.ctrlMu.Lock()
		ls.ctrl = conn
		ls.ctrlMu.Unlock()
		ls.readControl(conn)
	case streamActor:
		if ls.onActor != nil {
			ls.onActor(conn)
		} else {
			_ = conn.Close()
		}
	case streamLane:
		if ls.onLane != nil {
			ls.onLane(conn)
		} else {
			_ = conn.Close()
		}
	default:
		if ls.logger != nil {
			ls.logger.Warn("link.unknown_stream_kind", "kind", string(hdr.Kind))
		}
		_ = conn.Close()
	}
}

// readControl drives one control substream's inbound JSON values into onControl.
// The substream carries ONLY newline-delimited control JSON (never raw bytes), so
// a json.Decoder — which may read ahead past a value into its own buffer — is safe
// here (unlike a lane data stream, where readLaneJSON's zero-over-read is required).
// Any decode error is the substream dying; return so the enclosing session death
// funnel (closed()) takes over.
func (ls *linkSession) readControl(conn net.Conn) {
	dec := json.NewDecoder(conn)
	queue := make(chan []byte, controlQueueDepth)
	go func() {
		for raw := range queue {
			if ls.onControl != nil {
				ls.onControl(raw)
			}
		}
	}()
	defer close(queue)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			ls.kill("control_decode", err)
			return
		}
		copyRaw := append([]byte(nil), raw...)
		select {
		case queue <- copyRaw:
		default:
			ls.kill("control_queue_full", errors.New("control dispatch queue full"))
			return
		}
	}
}

// sendControl writes one control-plane payload (already-marshalled JSON) on the
// control substream, newline-terminated, serialised against concurrent senders.
func (ls *linkSession) sendControl(payload []byte) error {
	ls.ctrlMu.Lock()
	defer ls.ctrlMu.Unlock()
	if ls.ctrl == nil {
		return errLinkClosed
	}
	buf := make([]byte, 0, len(payload)+1)
	buf = append(buf, payload...)
	buf = append(buf, '\n')
	_, err := ls.ctrl.Write(buf)
	if err != nil {
		ls.kill("control_write", err)
	}
	return err
}

// openStream opens one fresh actor substream (daemon side), tagging it so the
// home's accept loop routes it to runtime.Attach. yamux assigns the substream id
// itself — the retired mux's nextID hand-numbering is gone.
func (ls *linkSession) openStream() (net.Conn, error) {
	conn, err := ls.ys.Open()
	if err != nil {
		return nil, err
	}
	conn = &boundedConn{Conn: conn, logger: ls.logger}
	if err := writeStreamHeader(conn, streamActor); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// openLane opens a fresh top-level tag=lane substream (daemon side): §5's
// resource-lane redeem rides directly on it (片③ flattened the lane — no
// nested yamux session, see lane.go).
func (ls *linkSession) openLane() (net.Conn, error) {
	conn, err := ls.ys.Open()
	if err != nil {
		return nil, err
	}
	conn = &boundedConn{Conn: conn, logger: ls.logger}
	if err := writeStreamHeader(conn, streamLane); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// closed returns a channel closed when the session dies (peer gone, carrier
// error, or Close). It replaces the retired demux loop's return-then-teardown as
// the single link-death signal: yamux errors every open substream on session
// death, so each actor substream's ipc read loop fails and publishes its down
// edge — the SAME death funnel the old teardown() drove, now yamux's job.
func (ls *linkSession) closed() <-chan struct{} { return ls.ys.CloseChan() }

// Close tears the session (and thus every substream) down.
func (ls *linkSession) Close() error {
	if ls.ys == nil {
		return nil
	}
	ls.kill("explicit_close", nil)
	return nil
}

func (ls *linkSession) kill(reason string, err error) {
	ls.killOnce.Do(func() {
		if ls.logger != nil {
			ls.logger.Warn("link.session_killed", "reason", reason, "error", err)
		}
		if ls.ys != nil {
			_ = ls.ys.Close()
		}
	})
}
