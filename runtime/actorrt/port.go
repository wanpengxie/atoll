package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// EmitSink is the upward relay callback the substrate invokes when a port's
// remote actor emits an envelope. Injected so the port owns only the wire
// boundary: the caller owns where emits land.
//
// inc is THIS port's Incarnation — the (authenticated bound id, embodiment-
// pointer) pair resolved at the handshake. The substrate carries the author
// identity, the remote actor never self-reports it: the author of a port (out-
// of-process cell) emit is stamped by the basis from inc.ID(), exactly as a
// local cell's author is stamped by the basis; the wire's self-reported sender
// carries no authority. The incarnation (not just the bare id) is passed so the
// home-side sink can gate the emit on this port still being the live embodiment
// (the port death-write gate): a livePen welded to inc fences an in-flight
// emit from a port already replaced/torn down, exactly as the cell path fences a
// leaked pen — the same WHEN-validity membrane on both transports.
//
// It returns the authoritative write verdict (ipc.EmitResult: MessageID +
// RejectReason) so the port can ack it back to the remote actor — the writer
// contract is not downgraded across the wire. The error is the transport/write
// failure (relayed to the remote as the ack's Err string); a rejected-but-
// processed emit returns a non-zero RejectReason with a nil error.
type EmitSink func(ctx context.Context, inc Incarnation, env *message.Envelope) (ipc.EmitResult, error)

// RelaySink is the plane-AGNOSTIC upward relay callback for a port's non-message
// capability arms (plane-2 access/state, time-axis schedule). The substrate port
// carries the author identity in inc (exactly as EmitSink does) and forwards the
// OPAQUE request payload verbatim: it does NOT decode it. The wire payload's shape
// is owned by the injecting caller (the platform link layer), so actorrt never
// imports the access/schedule vocabulary — the port is a pure transport, the same
// way it never interprets a KindObs value. It returns the opaque response bytes
// the caller acks back to the remote (verdict, not downgraded across the wire) and
// an error for a host-side fault (relayed as the ack's Err string).
//
// inc is passed (not just the bare id) for the same reason as EmitSink: the home-
// side sink gates the invocation on this port still being the live embodiment
// (the plane-2 / time-axis twin of the port death-write gate).
type RelaySink func(ctx context.Context, inc Incarnation, payload []byte) ([]byte, error)

// Sinks is the full set of upward relay callbacks a port's incarnation carries —
// one per plane an in-process cell's Caps expose. Emit is the message plane (its
// type is welded to the envelope, as before); Access and Schedule are the plane-2
// off-log and time-axis arms, opaque to the substrate (RelaySink). A remote actor
// is an out-of-process embodiment, so its incarnation must carry every plane a
// local one does (transport neutrality) — else "write data verified live, write
// truth verified live, but off-log/timers unguarded" would be a new two-plane
// misalignment on the same wire.
type Sinks struct {
	Emit     EmitSink
	Access   RelaySink
	Schedule RelaySink
}

// ResolveFunc is the connect-in auth seam: it maps a connecting actor's lease
// credential to the ActorID the substrate binds the connection to. This is the
// connection's ONE-TIME
// authentication — there is no per-frame re-auth (the stream is trusted once
// the handshake binds it).
type ResolveFunc func(leaseID string) (actor.ActorID, error)

// portSendQueue bounds a port's outbound mailbox — the buffer Deliver enqueues
// into and writeLoop drains to the wire. A full queue is MailboxFull, exactly
// like a cell's bounded inbox.
const portSendQueue = 64

// port is one OUT-OF-PROCESS actor embodiment: the substrate-side endpoint of a
// byte-stream connection to a remote actor (Erlang-port model — the connection
// IS the actor). It mirrors cell:
//   - Deliver enqueues into a bounded send queue drained to the wire; a full
//     queue returns ErrMailboxFull (non-blocking, like cell.Deliver).
//   - the read loop relays the remote's EMIT frames to the injected emit
//     callback and turns DOWN / EOF / unknown-kind into the SAME self-eviction +
//     down edge a cell publishes.
//
// Death never self-joins (a goroutine cannot wait on its own exit): die()
// cancels + closes the conn to unblock the loops and self-evicts via onExit;
// stop() joins from outside on done.
type port struct {
	id    actor.ActorID
	codec *ipc.Codec
	conn  io.Closer
	sinks Sinks

	// started is the substrate-stamped bind instant (obs uptime source), set at
	// Attach. Same authority model as cell.started.
	started time.Time

	ctx    context.Context
	cancel context.CancelFunc

	onDown func(actor.ActorID, error)
	// onObs relays an inbound KindObs (the remote actor's obs PUSH) into the
	// runtime's per-actor obs fanout — the cross-wire arm of the actor-source obs
	// axis. It passes THIS port as the self pointer so the runtime can
	// pointer-identity-gate the fanout (a replaced predecessor cannot publish obs
	// attributed to a same-id successor). nil → inbound obs is dropped (no
	// consumer). Mirrors cell.onObs.
	onObs  func(actor.ActorID, embodiment, ObsKind, ObsValue)
	onExit func(actor.ActorID, embodiment)
	logger *slog.Logger

	// live is the per-incarnation WHEN-validity atomic (embodiment contract), set
	// true at Attach go-live and false on teardown. The home-side port death-write
	// gate consults it: a livePen minted for a remote actor fences its
	// welded capability through runtime IsLive, which reads this field lock-free
	// (isLive) so a dangling emit from a torn-down port is rejected.
	live atomic.Bool

	sendq chan *message.Envelope
	wg    sync.WaitGroup
	done  chan struct{}

	dieOnce   sync.Once
	stopOnce  sync.Once
	closeOnce sync.Once

	mu       sync.Mutex
	closed   bool
	stopping bool
}

// newPort performs the connect-in handshake on conn (read KindHandshake →
// resolve lease to an ActorID → reply KindHandshakeAck) and builds the port.
// The handshake is synchronous (runs inside Attach) and is the connection's
// one-time authentication.
//
// hsCtx bounds the handshake READ. The handshake is a substrate-OWNED protocol
// step, so its time bound is a substrate invariant — not a duty pushed onto the
// host's watchdog. The conn type (io.ReadWriteCloser) deliberately hides any
// SetReadDeadline, so the substrate self-imposes the bound: the blocking first
// read runs off-goroutine and newPort selects it against hsCtx; on expiry it
// closes the conn (unblocking the read) and returns. parent owns the port's
// LIFETIME (unchanged); hsCtx owns only this one read.
func newPort(parent context.Context, hsCtx context.Context, conn io.ReadWriteCloser, sinks Sinks, resolve ResolveFunc, onDown func(actor.ActorID, error), onObs func(actor.ActorID, embodiment, ObsKind, ObsValue), onExit func(actor.ActorID, embodiment), started time.Time, logger *slog.Logger) (p *port, err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	// The conn is handed to the substrate at Attach; from here the substrate owns
	// closing it if the port fails to build (single owner — the caller never
	// closes on Attach failure). On a handshake-deadline expiry this close is also
	// what unblocks the parked read, so the substrate self-guards the bound with
	// no host watchdog.
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	if sinks.Emit == nil {
		return nil, errors.New("actorrt: port requires EmitSink")
	}
	if resolve == nil {
		return nil, errors.New("actorrt: port requires ResolveFunc")
	}
	codec := ipc.NewCodec(conn, conn)
	hs, err := readHandshakeBounded(hsCtx, codec)
	if err != nil {
		return nil, fmt.Errorf("actorrt: port handshake read: %w", err)
	}
	if hs.Kind != ipc.KindHandshake {
		return nil, fmt.Errorf("actorrt: port expected handshake, got %s", hs.Kind)
	}
	var hp ipc.HandshakePayload
	if err := json.Unmarshal(hs.Payload, &hp); err != nil {
		return nil, fmt.Errorf("actorrt: port handshake decode: %w", err)
	}
	id, err := resolve(hp.LeaseID)
	if err != nil {
		return nil, fmt.Errorf("actorrt: port resolve %q: %w", hp.LeaseID, err)
	}
	if id == "" {
		return nil, errors.New("actorrt: port resolve returned empty actor id")
	}
	ackPayload, err := json.Marshal(ipc.HandshakeAckPayload{Actor: id})
	if err != nil {
		return nil, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshakeAck, Payload: ackPayload}); err != nil {
		return nil, fmt.Errorf("actorrt: port handshake ack: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	return &port{
		id:      id,
		codec:   codec,
		conn:    conn,
		sinks:   sinks,
		started: started,
		ctx:     ctx,
		cancel:  cancel,
		onDown:  onDown,
		onObs:   onObs,
		onExit:  onExit,
		logger:  logger,
		sendq:   make(chan *message.Envelope, portSendQueue),
		done:    make(chan struct{}),
	}, nil
}

// readHandshakeBounded reads the first frame under hsCtx. ipc.Codec.Read blocks
// on the wire with no deadline (the conn type hides SetReadDeadline), so the
// read runs off-goroutine and we select it against hsCtx. On hsCtx expiry we
// return the deadline error WITHOUT closing the conn: newPort's failure path
// (single conn owner) closes it, and THAT close unblocks the parked read — the
// goroutine then drains into the buffered channel, leaking nothing. So the
// substrate still self-guards the bound (the close is the substrate's, just at
// newPort), with one coherent owner instead of two. A nil hsCtx degrades to an
// unbounded read (parity with the previous contract for callers that opt out).
func readHandshakeBounded(hsCtx context.Context, codec *ipc.Codec) (ipc.Frame, error) {
	if hsCtx == nil {
		return codec.Read()
	}
	type readResult struct {
		frame ipc.Frame
		err   error
	}
	resCh := make(chan readResult, 1)
	go func() {
		f, err := codec.Read()
		resCh <- readResult{frame: f, err: err}
	}()
	select {
	case r := <-resCh:
		return r.frame, r.err
	case <-hsCtx.Done():
		return ipc.Frame{}, fmt.Errorf("handshake deadline: %w", hsCtx.Err())
	}
}

// startedAt implements embodiment: the substrate-stamped bind instant (obs uptime
// source), set at Attach.
func (p *port) startedAt() time.Time { return p.started }

// isLive implements embodiment: the lock-free WHEN-validity probe (per-incarnation
// atomic). True only between Attach go-live and teardown.
func (p *port) isLive() bool { return p.live.Load() }

// markDead implements embodiment: flip the liveness atomic to false (idempotent).
func (p *port) markDead() { p.live.Store(false) }

// Deliver enqueues env into the port's bounded send queue. Never blocks: a full
// queue returns ErrMailboxFull, a torn-down port ErrCellStopped. nil is not a
// message and is rejected (a mailbox carries only envelopes).
func (p *port) Deliver(env *message.Envelope) error {
	if env == nil {
		return errors.New("actorrt: port deliver nil envelope")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrCellStopped
	}
	p.mu.Unlock()
	select {
	case p.sendq <- env:
		return nil
	default:
		return ErrMailboxFull
	}
}

// cancelRequest implements embodiment for an out-of-process actor: the
// request-scope of cancel(scope) crossing the wire. It writes a KindCancel frame
// naming the request directly onto the codec — NOT through the sendq/writeLoop —
// because cancel is off-loop: it must not queue behind the very deliver work it
// means to interrupt, and the codec's write mutex already serialises it against
// concurrent deliver writes. The remote host fires the matching reqCtx off its
// own cell goroutine. Best-effort: a write error on a dying conn is dropped (the
// request's deadline + the caller's closure still own the terminal); a torn-down
// port is a no-op.
func (p *port) cancelRequest(id message.ID) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return
	}
	payload, err := json.Marshal(ipc.CancelPayload{RequestID: id})
	if err != nil {
		return
	}
	_ = p.codec.Write(ipc.Frame{Kind: ipc.KindCancel, Payload: payload})
}

// start launches the write + read loops and closes done once both exit.
func (p *port) start() {
	p.wg.Add(2)
	go p.writeLoop()
	go p.readLoop()
	go func() { p.wg.Wait(); close(p.done) }()
}

// writeLoop drains the send queue onto the wire.
func (p *port) writeLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			// A cancelled ctx is CHANNEL TEARDOWN (Config.Parent collapsed) —
			// not an observed death. Route through die(nil), the QUIET arm
			// (initiateStop's exact semantics): closeConn (unblocks readLoop,
			// releases the conn), retract addressing (a level scan reads
			// "absent"), but publish NO down edge. Publishing here would
			// materialise receiver_unavailable terminals mid-teardown for
			// every port-hosted actor while cell-hosted actors (whose ctx arm
			// returns with deathCause=nil) stay silent — the same event
			// splitting into two truth outcomes by transport, the classic
			// "two paths" disease. Teardown owes truth nothing: closure
			// correctness belongs to the level-scan reconciler on the next
			// open, which reads these ports as absent either way. An
			// INDIVIDUALLY dying port still publishes — its path is
			// die(cause) from a loop error, never this arm.
			p.die(nil)
			return
		case env := <-p.sendq:
			payload, err := json.Marshal(ipc.DeliverPayload{Envelope: *env})
			if err != nil {
				// A malformed envelope is dropped (not a transport death) —
				// the log is truth; closure belongs to the sender.
				continue
			}
			if err := p.codec.Write(ipc.Frame{Kind: ipc.KindDeliver, Payload: payload}); err != nil {
				p.die(fmt.Errorf("actorrt: port %s deliver write: %w", p.id, err))
				return
			}
		}
	}
}

// readLoop relays remote EMIT frames via the injected EmitSink and writes the
// verdict back as a KindEmitAck (FIFO, in receipt order), and turns
// DOWN / EOF / unknown-kind into death. The wire is a closed set: an unknown kind is a
// protocol violation and fail-closes the port (never silently ignored).
func (p *port) readLoop() {
	defer p.wg.Done()
	for {
		frame, err := p.codec.Read()
		if err != nil {
			p.die(fmt.Errorf("actorrt: port %s read: %w", p.id, err))
			return
		}
		switch frame.Kind {
		case ipc.KindEmit:
			var ep ipc.EmitPayload
			if err := json.Unmarshal(frame.Payload, &ep); err != nil {
				p.die(fmt.Errorf("actorrt: port %s emit decode: %w", p.id, err))
				return
			}
			env := ep.Envelope
			// SYNCHRONOUS: call EmitSink, then write its verdict back as a
			// KindEmitAck. readLoop is a single goroutine, so emits are processed
			// in receipt order and acked in receipt order — that ordering IS the
			// FIFO correlation (the wire contract pins acks to receipt order; no
			// per-emit id). The writer contract is not downgraded across the wire:
			// the remote actor's Respond observes the same MessageID/RejectReason
			// a local cell would.
			//
			// p.id is the connection's authenticated bound identity — the basis
			// stamps the author from it, never from the wire's self-reported sender.
			// The Incarnation (id + this very embodiment pointer) rides the sink so
			// the home can fence the emit if this port is no longer the live one.
			res, emitErr := p.sinks.Emit(p.ctx, Incarnation{id: p.id, p: p}, &env)
			ackPayload := ipc.EmitAckPayload{EmitResult: res}
			if emitErr != nil {
				ackPayload.Err = emitErr.Error()
			}
			raw, err := json.Marshal(ackPayload)
			if err != nil {
				p.die(fmt.Errorf("actorrt: port %s emit ack marshal: %w", p.id, err))
				return
			}
			if err := p.codec.Write(ipc.Frame{Kind: ipc.KindEmitAck, Payload: raw}); err != nil {
				p.die(fmt.Errorf("actorrt: port %s emit ack write: %w", p.id, err))
				return
			}
		case ipc.KindAccess:
			// Plane-2 off-log capability invocation —逐字同构 with KindEmit: relay
			// the OPAQUE payload to the injected access sink (the port never decodes
			// it), then write the verdict back as a KindAccessAck. Single goroutine ⇒
			// receipt-order processing ⇒ receipt-order acks = the FIFO correlation
			// (no per-op id). The Incarnation rides the sink so the home can fence the
			// invocation if this port is no longer the live embodiment.
			if !p.relayAck(ipc.KindAccessAck, p.sinks.Access, frame.Payload, "access") {
				return
			}
		case ipc.KindSchedule:
			// Time-axis capability invocation — same shape as KindAccess.
			if !p.relayAck(ipc.KindScheduleAck, p.sinks.Schedule, frame.Payload, "schedule") {
				return
			}
		case ipc.KindObs:
			// Actor-source obs PUSH from the remote actor: relay into the runtime's
			// per-actor obs fanout. Non-fatal (obs is non-truth, best-effort) — a
			// decode error IS a protocol violation (closed-set discipline) and
			// fail-closes the port, but a well-formed obs just fans out and the loop
			// continues. p.id is the connection's authenticated identity (the wire
			// never self-reports it).
			var op ipc.ObsPayload
			if err := json.Unmarshal(frame.Payload, &op); err != nil {
				p.die(fmt.Errorf("actorrt: port %s obs decode: %w", p.id, err))
				return
			}
			if p.onObs != nil {
				p.onObs(p.id, p, ObsKind(op.Kind), ObsValue(op.Value))
			}
		case ipc.KindDown:
			var dp ipc.DownPayload
			_ = json.Unmarshal(frame.Payload, &dp)
			reason := dp.Reason
			if reason == "" {
				reason = "remote down"
			}
			p.die(fmt.Errorf("actorrt: port %s down: %s", p.id, reason))
			return
		default:
			p.die(fmt.Errorf("actorrt: port %s unknown frame kind %q", p.id, frame.Kind))
			return
		}
	}
}

// relayAck processes one opaque capability frame (KindAccess / KindSchedule):
// invoke the injected sink with the raw payload + this port's Incarnation, then
// write the verdict back as ackKind. It returns false (and has already die()'d)
// on a fatal fault — a nil sink is a protocol violation (a frame arrived for a
// plane this port never wired: fail-closed, same closed-set discipline as an
// unknown kind), and an ack-write failure is a transport death. A non-nil sink
// ERROR is NOT fatal: it is the invocation's own verdict, relayed as the ack's
// Err (a rejected-but-processed op, exactly like KindEmit's emitErr path).
func (p *port) relayAck(ackKind ipc.Kind, sink RelaySink, payload []byte, plane string) bool {
	if sink == nil {
		p.die(fmt.Errorf("actorrt: port %s received %s frame but no %s sink is wired", p.id, plane, plane))
		return false
	}
	res, relayErr := sink(p.ctx, Incarnation{id: p.id, p: p}, payload)
	ackPayload := ipc.RelayAckPayload{Payload: res}
	if relayErr != nil {
		ackPayload.Err = relayErr.Error()
	}
	raw, err := json.Marshal(ackPayload)
	if err != nil {
		p.die(fmt.Errorf("actorrt: port %s %s ack marshal: %w", p.id, plane, err))
		return false
	}
	if err := p.codec.Write(ipc.Frame{Kind: ackKind, Payload: raw}); err != nil {
		p.die(fmt.Errorf("actorrt: port %s %s ack write: %w", p.id, plane, err))
		return false
	}
	return true
}

// die makes the port unaddressable (pointer-identity self-eviction) and
// publishes the down edge exactly once — UNLESS this teardown is an
// external stop() (clean despawn, no closure obligation). It cancels + closes the
// conn to unblock both loops; it NEVER joins them (stop() does that).
func (p *port) die(cause error) {
	p.dieOnce.Do(func() {
		p.live.Store(false)
		p.mu.Lock()
		stopping := p.stopping
		p.closed = true
		p.mu.Unlock()
		p.cancel()
		p.closeConn()
		if p.onExit != nil {
			p.onExit(p.id, p)
		}
		if !stopping && cause != nil && p.onDown != nil {
			p.onDown(p.id, cause)
		}
	})
}

// initiateStop implements embodiment: the non-blocking, idempotent SIGNAL half of
// teardown — port already has exactly this shape in die(): it never
// joins, it self-evicts via onExit, and cause==nil already skips the onDown
// death edge (the same "clean stop, no closure obligation" semantics stop()
// wants). A dying parent's cascade (removeIf) calls this to signal a forked
// child without joining it.
func (p *port) initiateStop() { p.die(nil) }

// stop is the external teardown: mark stopping (so the loops' die() publishes NO
// down edge), cancel + close to unblock the loops, then join on done.
func (p *port) stop() {
	p.stopOnce.Do(func() {
		p.live.Store(false)
		p.mu.Lock()
		p.stopping = true
		p.closed = true
		p.mu.Unlock()
		p.cancel()
		p.closeConn()
	})
	<-p.done
}

func (p *port) closeConn() {
	p.closeOnce.Do(func() {
		if p.conn != nil {
			_ = p.conn.Close()
		}
	})
}
