package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// EmitSink is the upward relay callback the substrate invokes when a port's
// remote actor emits an envelope. Injected so the port owns only the wire
// boundary: the caller owns where emits land.
//
// id is the connection's authenticated bound ActorID (resolved at the handshake)
// — the substrate carries the author identity, the remote actor never self-reports
// it. The author of a port (out-of-process cell) emit is stamped by the basis from
// this bound id, exactly as a local cell's author is stamped by the basis; the
// wire's self-reported sender carries no authority.
//
// It returns the authoritative write verdict (ipc.EmitResult: MessageID +
// RejectReason) so the port can ack it back to the remote actor — the writer
// contract is not downgraded across the wire. The error is the transport/write
// failure (relayed to the remote as the ack's Err string); a rejected-but-
// processed emit returns a non-zero RejectReason with a nil error.
type EmitSink func(ctx context.Context, id actor.ActorID, env *message.Envelope) (ipc.EmitResult, error)

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

// port is one OUT-OF-PROCESS actor presence: the substrate-side endpoint of a
// byte-stream connection to a remote actor (Erlang-port model — the connection
// IS the actor). It mirrors cell:
//   - Deliver enqueues into a bounded send queue drained to the wire; a full
//     queue returns ErrMailboxFull (non-blocking, like cell.Deliver).
//   - the read loop relays the remote's EMIT frames to the injected emit
//     callback and turns DOWN / EOF / unknown-kind into the SAME self-eviction +
//     presence-down edge a cell publishes.
//
// Death never self-joins (a goroutine cannot wait on its own exit): die()
// cancels + closes the conn to unblock the loops and self-evicts via onExit;
// stop() joins from outside on done.
type port struct {
	id    actor.ActorID
	codec *ipc.Codec
	conn  io.Closer
	emit  EmitSink

	// started is the substrate-stamped bind instant (obs uptime source), set at
	// Attach. Same authority model as cell.started.
	started time.Time

	ctx    context.Context
	cancel context.CancelFunc

	onDown func(actor.ActorID, error)
	onExit func(actor.ActorID, presence)
	logger *slog.Logger

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
func newPort(parent context.Context, conn io.ReadWriteCloser, emit EmitSink, resolve ResolveFunc, onDown func(actor.ActorID, error), onExit func(actor.ActorID, presence), started time.Time, logger *slog.Logger) (*port, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if emit == nil {
		return nil, errors.New("actorrt: port requires EmitSink")
	}
	if resolve == nil {
		return nil, errors.New("actorrt: port requires ResolveFunc")
	}
	codec := ipc.NewCodec(conn, conn)
	hs, err := codec.Read()
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
		id:       id,
		codec:    codec,
		conn:     conn,
		emit:     emit,
		started:  started,
		ctx:      ctx,
		cancel:   cancel,
		onDown:   onDown,
		onExit:   onExit,
		logger:   logger,
		sendq:    make(chan *message.Envelope, portSendQueue),
		done:     make(chan struct{}),
	}, nil
}

// startedAt implements presence: the substrate-stamped bind instant (obs uptime
// source), set at Attach.
func (p *port) startedAt() time.Time { return p.started }

// observe implements presence for an out-of-process actor: obs PULL over the
// wire is not yet wired (additive — a KindObs round-trip frame), so the port
// reports ErrObsUnsupported. The 2×2 cell exists; the wire arm is a no-op until
// a real consumer drives it.
func (p *port) observe(ctx context.Context, kind ObsKind) (ObsValue, error) {
	return nil, ErrObsUnsupported
}

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
			// A cancelled ctx is the actor-scope of cancel(scope): a cancellable
			// parent (runtime teardown / kill) collapsed this presence. Route it
			// through die(), not a bare return — die() closeConn (unblocks readLoop,
			// releases the conn, retracts addressing so a level scan reads "absent")
			// AND publishes the presence-down edge (lossy wakeup). die()'s stopping
			// guard suppresses the redundant edge when this is an external stop().
			p.die(p.ctx.Err())
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
			res, emitErr := p.emit(p.ctx, p.id, &env)
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

// die makes the port unaddressable (pointer-identity self-eviction) and
// publishes the presence-down edge exactly once — UNLESS this teardown is an
// external stop() (clean despawn, no closure obligation). It cancels + closes the
// conn to unblock both loops; it NEVER joins them (stop() does that).
func (p *port) die(cause error) {
	p.dieOnce.Do(func() {
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

// stop is the external teardown: mark stopping (so the loops' die() publishes NO
// presence-down edge), cancel + close to unblock the loops, then join on done.
func (p *port) stop() {
	p.stopOnce.Do(func() {
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
