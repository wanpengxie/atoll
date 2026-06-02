// Package ipc defines the PORT WIRE PROTOCOL — the length-prefixed JSON
// byte-stream contract between the substrate (host side, hosted in
// runtime/actorrt as a `port` presence) and ONE out-of-process actor.
//
// Model: ONE connection == ONE actor (the Erlang `open_port` model). The
// connection IS the actor's identity, so NO per-frame actor / worker /
// channel id. (Multiplexing many actors over one link = Erlang distribution;
// an additive future, not pre-built here.)
//
// Security boundary = the CONNECTION, authenticated ONCE at handshake
// (resolve credential → ActorID). Thereafter the point-to-point stream is
// trusted — there is no per-frame re-auth (TCP/TLS model: you authenticate the
// connection, not every packet). A zombie/reconnecting actor is handled by
// connect-in REPLACE: a new connection for the same ActorID stops + closes the
// old one (actorrt Spawn-replace). So no fence frame, no per-frame token.
//
// The wire is medium-agnostic: the Codec wraps io.Reader / io.Writer, so the
// same protocol runs over a local pipe (same-node out-of-proc actor) or a
// net.Conn (cloud / proxy-imported out-of-proc actor).
package ipc

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Kind is the closed set of port-wire frame kinds. Every kind has a real
// producer + a real state transition — nothing reserved-but-unwired.
type Kind string

const (
	// KindHandshake (remote→host): the connecting actor presents its lease
	// credential. The host resolves it to an ActorID. This is the connection's
	// one-time authentication.
	KindHandshake Kind = "handshake"
	// KindHandshakeAck (host→remote): the host returns the bound ActorID.
	KindHandshakeAck Kind = "handshake_ack"
	// KindDeliver (host→remote): one envelope into the bound actor's mailbox.
	// Fire-and-forget — the transport's own flow control IS the backpressure
	// (a full pipe/socket buffer surfaces as MailboxFull on the host's
	// non-blocking enqueue).
	KindDeliver Kind = "deliver"
	// KindEmit (remote→host): the bound actor emitted an envelope upward. The
	// host (port presence) relays it to the harness — the single channel-log
	// writer.
	KindEmit Kind = "emit"
	// KindDown (remote→host): the bound actor died. The host materialises the
	// DeathSignal (receiver_unavailable for in-flight requests). Connection EOF
	// is the equivalent terminal signal.
	KindDown Kind = "down"
)

// MaxFrameBytes caps one length-prefixed JSON frame at 16 MiB.
const MaxFrameBytes = 1 << 24

// Frame is the port-wire envelope. Length-prefixed JSON: a uint32 BE length
// header followed by the JSON-marshalled Frame. It carries only the kind + an
// opaque payload — the connection identifies + authenticates the actor, so no
// per-frame id, token, actor, worker, or channel rides along.
type Frame struct {
	Kind    Kind            `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// HandshakePayload is sent remote → host on connect.
type HandshakePayload struct {
	LeaseID string `json:"lease_id"`
}

// HandshakeAckPayload is the host's reply: the bound actor identity.
type HandshakeAckPayload struct {
	Actor actor.ActorID `json:"actor"`
}

// DeliverPayload carries one envelope into the bound actor's mailbox.
type DeliverPayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// EmitPayload carries one envelope the bound actor emitted upward.
type EmitPayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// DownPayload is the bound actor's death signal (the actor is implicit — the
// connection IS that actor).
type DownPayload struct {
	Reason string `json:"reason,omitempty"`
}

// Codec encodes / decodes port frames on length-prefixed buffered IO. It is
// medium-agnostic (any io.Reader/io.Writer: local pipe or net.Conn).
type Codec struct {
	r    *bufio.Reader
	w    io.Writer
	wmu  sync.Mutex
	hdr  [4]byte
	rbuf []byte
}

// NewCodec wraps r/w as a frame Codec.
func NewCodec(r io.Reader, w io.Writer) *Codec {
	return &Codec{r: bufio.NewReader(r), w: w}
}

// Write marshals a Frame and emits the length-prefixed bytes.
func (c *Codec) Write(f Frame) error {
	raw, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("ipc: marshal: %w", err)
	}
	if len(raw) > MaxFrameBytes {
		return errors.New("ipc: frame too large")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	binary.BigEndian.PutUint32(c.hdr[:], uint32(len(raw)))
	if _, err := c.w.Write(c.hdr[:]); err != nil {
		return err
	}
	if _, err := c.w.Write(raw); err != nil {
		return err
	}
	return nil
}

// Read pulls the next frame. Returns io.EOF when the connection closed.
func (c *Codec) Read() (Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameBytes {
		return Frame{}, fmt.Errorf("ipc: frame too large: %d > %d", n, MaxFrameBytes)
	}
	if int(n) > cap(c.rbuf) {
		c.rbuf = make([]byte, n)
	} else {
		c.rbuf = c.rbuf[:n]
	}
	if _, err := io.ReadFull(c.r, c.rbuf); err != nil {
		return Frame{}, err
	}
	var f Frame
	if err := json.Unmarshal(c.rbuf, &f); err != nil {
		return Frame{}, fmt.Errorf("ipc: unmarshal: %w", err)
	}
	return f, nil
}
