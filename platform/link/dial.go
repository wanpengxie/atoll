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

// actorStream is one hosted actor's link stream + its native ipc plumbing.
type actorStream struct {
	id     actor.ActorID
	stream *stream
	codec  *ipc.Codec
	writer *ipc.RemoteWriter
}

// Dial dials the home, sends the stream-0 attach, and waits for attach_reply. It
// does NOT open actor streams or start any demux — Start does that after the
// host is built. Window-period frames sit in the kernel socket buffer.
func Dial(ctx context.Context, serverURL, apiKey, computeID string, decls []Declaration, logger *slog.Logger) (*Dialer, error) {
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
		APIKey: apiKey, ComputeID: computeID, Declarations: decls,
	}})
	if err != nil {
		_ = ws.Close()
		return nil, err
	}

	// The demux loop runs for the link's whole life. Starting it in Dial is safe
	// w.r.t. the dispatch race: an actor stream exists only after OpenStream
	// registers it AND wires its dispatch handler synchronously, so a data frame
	// for an unopened stream is dropped — no dispatch can race a half-built host.
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
// routes it into the cell's mailbox. Call after Dial, before Start consumes the
// home's first dispatch.
func (d *Dialer) OpenStream(id actor.ActorID, dispatch func(env *message.Envelope) error) (harness.Writer, func(cause string), error) {
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
	as := &actorStream{id: id, stream: s, codec: codec, writer: rw}
	d.mu.Lock()
	d.streams[id] = as
	d.mu.Unlock()

	// Per-stream read loop: KindDeliver → dispatch into the cell; KindEmitAck →
	// resolve the RemoteWriter's FIFO head; EOF → fail pending emits + close.
	go d.streamReadLoop(as, dispatch)

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
		case ipc.KindControl:
			// Control lane to a remote cell is not wired on the daemon side yet
			// (no consumer); a control frame is dropped rather than fail-closed,
			// since the link is the home↔daemon hop, not the cell-control path.
			d.logger.Debug("link.control", "actor", string(as.id))
		default:
			d.logger.Warn("link.unknown_kind", "actor", string(as.id), "kind", string(frame.Kind))
		}
	}
}

// Start begins the idle-ping keepalive (the demux loop is already running from
// Dial). Call once, after Dial + all OpenStream + host install. The dispatch
// handlers are already wired per stream, so no inbound dispatch races a
// half-built host: streams are opened explicitly by OpenStream before Start.
func (d *Dialer) Start() {
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

// Done returns a channel closed when the link tears down (peer gone, lease
// expiry on the home side, or Close).
func (d *Dialer) Done() <-chan struct{} { return d.done }

// Close tears the link down. Every actor stream EOFs, every pending emit fails.
func (d *Dialer) Close() error { return d.lc.Close() }
