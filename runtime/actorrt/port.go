package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// EmitSink relays an envelope a port's remote actor emitted upward into the
// channel (the harness write seam). Injected so actorrt stays harness-agnostic:
// the port owns the wire boundary, the caller owns where emits land.
type EmitSink func(ctx context.Context, env *message.Envelope) error

// ResolveFunc is the connect-in auth seam (lightcone /daemon/connect?key=
// style): it maps a connecting actor's lease credential to the ActorID the
// substrate binds the connection to, plus the fence token the host stamps for
// this connection (a later frame bearing a different token is a zombie → fence).
type ResolveFunc func(leaseID string) (id actor.ActorID, leaseToken string, err error)

// portSendQueue bounds a port's outbound mailbox — the buffer Deliver enqueues
// into and writeLoop drains to the wire. A full queue is MailboxFull, exactly
// like a cell's bounded inbox.
const portSendQueue = 64

// port is one OUT-OF-PROCESS actor presence: the substrate-side endpoint of a
// byte-stream connection to a remote actor (Erlang-port model — the connection
// IS the actor). It mirrors cell:
//   - Deliver enqueues into a bounded send queue drained to the wire; a full
//     queue returns ErrMailboxFull (non-blocking, like cell.Deliver).
//   - the read loop relays the remote's EMIT frames to the harness (emit) and
//     turns DOWN / EOF into the SAME self-eviction + DeathSignal a cell raises.
//
// Death never self-joins (a goroutine cannot wait on its own exit): die()
// cancels + closes the conn to unblock the loops and self-evicts via onExit;
// stop() joins from outside on done.
type port struct {
	id    actor.ActorID
	codec *ipc.Codec
	conn  io.Closer
	emit  EmitSink
	lease string

	ctx    context.Context
	cancel context.CancelFunc

	sup    Supervisor
	onExit func(actor.ActorID, presence)

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
// resolve lease to an ActorID → reply KindHandshakeAck with the fence token)
// and builds the port. The handshake is synchronous (runs inside Attach).
func newPort(parent context.Context, conn io.ReadWriteCloser, emit EmitSink, resolve ResolveFunc, sup Supervisor, onExit func(actor.ActorID, presence)) (*port, error) {
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
	id, token, err := resolve(hp.LeaseID)
	if err != nil {
		return nil, fmt.Errorf("actorrt: port resolve %q: %w", hp.LeaseID, err)
	}
	if id == "" {
		return nil, errors.New("actorrt: port resolve returned empty actor id")
	}
	ackPayload, err := json.Marshal(ipc.HandshakeAckPayload{Actor: id, LeaseToken: token})
	if err != nil {
		return nil, err
	}
	if err := codec.Write(ipc.Frame{ID: hs.ID, Kind: ipc.KindHandshakeAck, LeaseToken: token, Payload: ackPayload}); err != nil {
		return nil, fmt.Errorf("actorrt: port handshake ack: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	return &port{
		id:     id,
		codec:  codec,
		conn:   conn,
		emit:   emit,
		lease:  token,
		ctx:    ctx,
		cancel: cancel,
		sup:    sup,
		onExit: onExit,
		sendq:  make(chan *message.Envelope, portSendQueue),
		done:   make(chan struct{}),
	}, nil
}

// Deliver enqueues env into the port's bounded send queue. Never blocks: a full
// queue returns ErrMailboxFull, a torn-down port ErrCellStopped.
func (p *port) Deliver(env *message.Envelope) error {
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

// writeLoop drains the send queue onto the wire as KindDeliver frames.
func (p *port) writeLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case env := <-p.sendq:
			payload, err := json.Marshal(ipc.DeliverPayload{Envelope: *env})
			if err != nil {
				// A malformed envelope is dropped (not a transport death) —
				// the log is truth; closure belongs to the sender.
				continue
			}
			if err := p.codec.Write(ipc.Frame{Kind: ipc.KindDeliver, LeaseToken: p.lease, Payload: payload}); err != nil {
				p.die(fmt.Errorf("actorrt: port %s deliver write: %w", p.id, err))
				return
			}
		}
	}
}

// readLoop relays remote EMIT frames to the harness and turns DOWN / EOF /
// stale-fence into death.
func (p *port) readLoop() {
	defer p.wg.Done()
	for {
		frame, err := p.codec.Read()
		if err != nil {
			p.die(fmt.Errorf("actorrt: port %s read: %w", p.id, err))
			return
		}
		if frame.LeaseToken != "" && frame.LeaseToken != p.lease {
			p.die(fmt.Errorf("actorrt: port %s stale lease token", p.id))
			return
		}
		switch frame.Kind {
		case ipc.KindEmit:
			var ep ipc.EmitPayload
			if err := json.Unmarshal(frame.Payload, &ep); err != nil {
				continue
			}
			env := ep.Envelope
			_ = p.emit(p.ctx, &env)
		case ipc.KindDown:
			var dp ipc.DownPayload
			_ = json.Unmarshal(frame.Payload, &dp)
			reason := dp.Reason
			if reason == "" {
				reason = "remote down"
			}
			p.die(fmt.Errorf("actorrt: port %s down: %s", p.id, reason))
			return
		case ipc.KindHeartbeat:
			// liveness: the frame's arrival IS the signal; nothing to record.
		case ipc.KindShutdownAck:
			p.die(nil)
			return
		}
	}
}

// die makes the port unaddressable (pointer-identity self-eviction) and raises
// the DeathSignal exactly once — UNLESS this teardown is an external stop()
// (clean despawn, no supervisor closure obligation). It cancels + closes the
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
		if !stopping && cause != nil && p.sup != nil {
			func() {
				defer func() { _ = recover() }()
				p.sup.OnDeath(context.Background(), DeathSignal{Actor: p.id, Cause: cause})
			}()
		}
	})
}

// stop is the external teardown: mark stopping (so the loops' die() raises NO
// DeathSignal), cancel + close to unblock the loops, then join on done.
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
