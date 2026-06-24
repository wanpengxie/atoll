package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Dialer is the daemon end of the link: it dials the home, attaches the party
// (stream 0), and opens one stream per attached actor — each running the NATIVE
// port-wire protocol with a real handshake (LeaseID = actor id). A hosted cell's
// pen is the stream's ipc.RemoteWriter (emits flow UP, block on the home's
// EmitAck). Dial/Start are two phases (inherited from step 0): Dial does WS +
// attach with NO inbound consumption; Start installs the per-stream demux after
// the host is fully built, so no dispatch races a half-built host.
type Dialer struct {
	lc        *linkConn
	channelID string
	logger    *slog.Logger

	mu       sync.Mutex
	nextID   uint32
	streams  map[actor.ActorID]*actorStream
	attached chan struct{} // closed when attach_reply arrives
	reply    AttachReply

	done chan struct{}
}

// actorStream is one hosted actor's link stream + its native ipc plumbing. The
// dispatch handler is captured at OpenStream but the read loop that invokes it
// only starts at Start() — after the host has installed every cell — so an
// inbound deliver can never race a half-built host (the frame waits in the
// stream buffer until Start).
type actorStream struct {
	id       actor.ActorID
	stream   *stream
	codec    *ipc.Codec
	writer   *ipc.RemoteWriter
	dispatch func(env *message.Envelope) error
	cancel   func(requestID message.ID)
}

// Dial dials the home, sends the stream-0 attach, and waits for attach_reply. It
// does NOT open actor streams or start any demux — Start does that after the
// host is built. Window-period frames sit in the kernel socket buffer.
func Dial(ctx context.Context, serverURL, computeID string, decls []Declaration, logger *slog.Logger) (*Dialer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	d := &Dialer{
		channelID: "",
		logger:    logger,
		nextID:    1,
		streams:   map[actor.ActorID]*actorStream{},
		attached:  make(chan struct{}),
		done:      make(chan struct{}),
	}

	onControl := func(payload []byte) {
		cf, derr := decodeControl(payload)
		if derr != nil || cf.Kind != ctrlAttachReply || cf.AttachReply == nil {
			return
		}
		d.mu.Lock()
		select {
		case <-d.attached:
		default:
			d.reply = *cf.AttachReply
			d.channelID = string(cf.AttachReply.ChannelID)
			close(d.attached)
		}
		d.mu.Unlock()
	}
	d.lc = newLinkConn(&wsConn{ws: ws}, onControl, nil)

	// Send attach on stream 0.
	raw, err := encodeControl(controlFrame{Kind: ctrlAttach, Attach: &AttachRequest{
		ComputeID: computeID, Declarations: decls,
	}})
	if err != nil {
		_ = ws.Close()
		return nil, err
	}

	// The demux loop runs for the link's whole life. It only routes data frames
	// into per-stream buffers; the per-stream READ loops (which invoke dispatch)
	// start at Start(), after every cell is installed. So the demux running here
	// cannot race a half-built host — a buffered deliver just waits for Start.
	go func() {
		defer close(d.done)
		d.lc.run(nil)
	}()

	if err := d.lc.sendControl(raw); err != nil {
		_ = ws.Close()
		return nil, err
	}

	select {
	case <-d.attached:
	case <-ctx.Done():
		_ = d.lc.Close()
		return nil, ctx.Err()
	case <-d.done:
		_ = d.lc.Close()
		return nil, errors.New("link: dial closed before attach reply")
	}
	if !d.reply.Accepted {
		_ = d.lc.Close()
		reason := "link: attach rejected"
		if d.reply.Reason != "" {
			reason = "link: " + d.reply.Reason
		}
		return nil, errors.New(reason)
	}
	return d, nil
}

// ChannelID returns the channel the home assigned on attach.
func (d *Dialer) ChannelID() string { return d.channelID }

// OpenStream opens one actor's link stream, performs the native ipc handshake
// (LeaseID = actor id), and returns the cell's PEN (ipc.RemoteWriter) plus a
// downHandler the host installs (close the stream UP on cell death). dispatch is
// invoked for each KindDeliver frame the home sends down this stream — the host
// routes it into the cell's mailbox. cancel is invoked for each KindCancel frame
// — the host fires the named request's reqCtx OFF the cell goroutine (the work
// it interrupts is the goroutine's occupant). Call after Dial, before Start
// consumes the home's first dispatch.
func (d *Dialer) OpenStream(id actor.ActorID, dispatch func(env *message.Envelope) error, cancel func(requestID message.ID)) (harness.Pen, func(cause string), error) {
	d.mu.Lock()
	sid := d.nextID
	d.nextID++
	d.mu.Unlock()

	s, err := d.lc.openStream(sid)
	if err != nil {
		return nil, nil, err
	}
	codec := ipc.NewCodec(s, s)

	// Native ipc handshake on the stream: present the lease credential (actor
	// id), read the home's bound-actor ack.
	hsPayload, err := json.Marshal(ipc.HandshakePayload{LeaseID: string(id)})
	if err != nil {
		_ = s.Close()
		return nil, nil, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: hsPayload}); err != nil {
		_ = s.Close()
		return nil, nil, fmt.Errorf("link: handshake write %s: %w", id, err)
	}
	ack, err := codec.Read()
	if err != nil {
		_ = s.Close()
		return nil, nil, fmt.Errorf("link: handshake ack read %s: %w", id, err)
	}
	if ack.Kind != ipc.KindHandshakeAck {
		_ = s.Close()
		return nil, nil, fmt.Errorf("link: expected handshake_ack for %s, got %s", id, ack.Kind)
	}

	rw := ipc.NewRemoteWriter(codec)
	as := &actorStream{id: id, stream: s, codec: codec, writer: rw, dispatch: dispatch, cancel: cancel}
	d.mu.Lock()
	d.streams[id] = as
	d.mu.Unlock()

	// NB: the per-stream read loop is NOT started here — Start() launches it once
	// the host has installed every cell. Deliver frames that arrive in the window
	// between handshake and Start wait in the stream buffer; starting dispatch
	// before install would let an envelope hit a not-yet-hosted actor and be
	// silently dropped (the bug step 0 fixed, in per-stream form).

	downHandler := func(cause string) {
		downPayload, _ := json.Marshal(ipc.DownPayload{Reason: cause})
		_ = codec.Write(ipc.Frame{Kind: ipc.KindDown, Payload: downPayload})
		_ = s.Close()
	}
	return rw, downHandler, nil
}

// streamReadLoop drives one actor stream's inbound ipc frames after the
// handshake: deliver work down to the cell, route emit-acks back to the
// RemoteWriter, and on EOF fail any pending emits and drop the stream.
func (d *Dialer) streamReadLoop(as *actorStream, dispatch func(env *message.Envelope) error) {
	defer func() {
		as.writer.Close()
		d.mu.Lock()
		delete(d.streams, as.id)
		d.mu.Unlock()
	}()
	for {
		frame, err := as.codec.Read()
		if err != nil {
			return
		}
		switch frame.Kind {
		case ipc.KindDeliver:
			var dp ipc.DeliverPayload
			if err := json.Unmarshal(frame.Payload, &dp); err != nil {
				d.logger.Error("link.deliver_decode", "actor", string(as.id), "err", err)
				continue
			}
			env := dp.Envelope
			if err := dispatch(&env); err != nil {
				d.logger.Error("link.dispatch", "actor", string(as.id), "err", err)
			}
		case ipc.KindEmitAck:
			var ap ipc.EmitAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				d.logger.Error("link.emit_ack_decode", "actor", string(as.id), "err", err)
				continue
			}
			as.writer.DeliverAck(ap)
		case ipc.KindCancel:
			var cp ipc.CancelPayload
			if err := json.Unmarshal(frame.Payload, &cp); err != nil {
				d.logger.Error("link.cancel_decode", "actor", string(as.id), "err", err)
				continue
			}
			// Fire the cancel OFF this read loop's goroutine — and crucially OFF the
			// cell goroutine the host routes it to. The request to cancel is the one
			// occupying that cell goroutine; queuing the cancel on-loop behind the
			// work it means to interrupt would deadlock. The host's CancelRequest
			// fires the reqCtx's CancelFunc (concurrent-safe), so a bare goroutine
			// is the right vehicle. nil cancel (none installed) is a no-op.
			if as.cancel != nil {
				go as.cancel(cp.RequestID)
			}
		default:
			d.logger.Warn("link.unknown_kind", "actor", string(as.id), "kind", string(frame.Kind))
		}
	}
}

// Start launches every actor stream's read loop, then the idle-ping keepalive.
// Call once, after Dial + all OpenStream + host install. Deferring the read
// loops to here (rather than starting them in OpenStream) is the dispatch-race
// fix: by the time any deliver is consumed, every cell is installed, so an
// envelope can never hit a half-built host. Frames buffered during the window
// are drained in receipt order when the loop starts.
func (d *Dialer) Start() {
	d.mu.Lock()
	streams := make([]*actorStream, 0, len(d.streams))
	for _, as := range d.streams {
		streams = append(streams, as)
	}
	d.mu.Unlock()
	for _, as := range streams {
		go d.streamReadLoop(as, as.dispatch)
	}
	go d.pingLoop()
}

// pingLoop sends an idle keepalive on stream 0 every leasePing so the home's
// lease last-seen refreshes even with no actor traffic (no pong — refresh is the
// whole point). Exits when the link tears down.
func (d *Dialer) pingLoop() {
	t := time.NewTicker(leasePing)
	defer t.Stop()
	ping, _ := json.Marshal(struct{}{})
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			if err := d.lc.sendControl(ping); err != nil {
				return
			}
		}
	}
}

// SendObs forwards one obs snapshot the named hosted actor pushed UP the link as
// a KindObs frame (daemon-side arm of the actor-source obs PUSH axis: the home
// port relays it into the home runtime's obs fanout). Fire-and-forget: a write
// error on a dying stream is dropped (obs is non-truth — the next snapshot or the
// home lease supersedes). No-op if the actor has no open stream. The codec write
// mutex serialises this against the cell's KindEmit writes.
func (d *Dialer) SendObs(id actor.ActorID, kind string, value []byte) {
	d.mu.Lock()
	as := d.streams[id]
	d.mu.Unlock()
	if as == nil {
		return
	}
	payload, err := json.Marshal(ipc.ObsPayload{Kind: kind, Value: value})
	if err != nil {
		return
	}
	_ = as.codec.Write(ipc.Frame{Kind: ipc.KindObs, Payload: payload})
}

// Done returns a channel closed when the link tears down (peer gone, lease
// expiry on the home side, or Close).
func (d *Dialer) Done() <-chan struct{} { return d.done }

// Close tears the link down. Every actor stream EOFs, every pending emit fails.
func (d *Dialer) Close() error { return d.lc.Close() }
