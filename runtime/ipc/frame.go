package ipc

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Kind is the closed set of port-wire frame kinds. Every kind has a real
// producer + a real state transition — nothing reserved-but-unwired.
type Kind string

const (
	// KindHandshake (remote→host): the connecting actor presents its lease
	// credential. The host resolves it to an ActorID. This is the connection's
	// one-time authentication.
	KindHandshake Kind = "handshake"
	// KindDeliver (host→remote): one envelope into the bound actor's mailbox.
	// Fire-and-forget — the transport's own flow control IS the backpressure:
	// the stream owner writes synchronously and its bounded write fails if the
	// peer stops reading. There is no application delivery queue.
	KindDeliver Kind = "deliver"
	// KindEmit (remote→host): the bound actor emitted an envelope upward. The
	// host route relays it to the harness — the single channel-log
	// writer.
	KindEmit Kind = "emit"
	// KindEmitAck (host→remote): the host's authoritative verdict for one
	// KindEmit. The harness Writer's WriteResult (MessageID + RejectReason) is
	// part of a cell's PEN: a remote cell's Respond/Emit MUST see the same
	// authoritative write verdict a local cell sees, so the writer contract may
	// not be downgraded across the wire — that requires an upward ack, not a
	// fire-and-forget emit.
	//
	// Correlation is FIFO with NO id: one connection == one actor, the stream is
	// totally ordered, and the host's read loop processes each KindEmit
	// synchronously (call EmitSink, then write its KindEmitAck) on a single
	// goroutine. So acks are returned strictly in receipt order. The contract is
	// pinned: the host acks emits in receipt order; the remote holds a FIFO wait
	// queue and may pipeline. A per-emit id would reserve a field for a
	// reordering that cannot occur — forbidden (zero reservation).
	KindEmitAck Kind = "emit_ack"
	// KindDown (remote→host): the bound actor died. The host publishes the
	// down edge (obs push); a subscriber materialises
	// receiver_unavailable for in-flight requests. Connection EOF is the
	// equivalent terminal signal.
	KindDown Kind = "down"
	// KindCancel (host→remote): the request-scope of cancel(scope) crossing the
	// wire. The home (where a request's caller lives) tells the remote host to
	// cancel the reqCtx its cell is running one request under — interrupting
	// exactly that in-flight Receive, not the actor. The remote MUST fire it
	// OFF the cell goroutine (the thing to interrupt is the goroutine's occupant;
	// queuing it behind the work it means to cancel would deadlock). It is
	// best-effort, unidirectional, no ack: a lost KindCancel only costs the
	// receiver a little wasted work — the request's ExpiresAt deadline still
	// collapses its reqCtx, and the caller's closure owns the terminal. This is
	// why it carries no ack frame and cannot reuse the on-loop KindDeliver path.
	KindCancel Kind = "cancel"
	// KindObs (remote→host): the bound actor pushed an opaque obs snapshot about
	// ITSELF (actor-source obs PUSH — operational/health state like device
	// presence, NEVER business content, NEVER truth). The host relays it into the
	// runtime's population obs fanout (publishObs) so home-side consumers
	// see it. Fire-and-forget, unidirectional, NO ack: obs is non-truth and
	// best-effort (a lost snapshot is superseded by the next one / decayed by the
	// lease) — so it cannot reuse the ack'd KindEmit path. The actor is implicit
	// (the connection IS that actor); the kind/value are OPAQUE (the substrate
	// forwards, never interprets — it governs structure, not vocabulary).
	KindObs Kind = "obs"
	// KindAccess (remote→host): the bound actor invoked its plane-2 off-log
	// capability (an access/state Invocation). The host relays the OPAQUE payload
	// to its injected access RelaySink — the substrate port does NOT decode it
	// (plane-agnostic transport: the wire payload's shape is owned by the platform
	// link layer, not by ipc/actorrt). One connection == one actor: the same
	// FIFO-no-id correlation as KindEmit (the host acks in receipt order on its
	// single read loop).
	KindAccess Kind = "access"
	// KindAccessAck (host→remote): the host's authoritative verdict for one
	// KindAccess (the opaque access response bytes + any host-side error). Same
	// receipt-order FIFO discipline and same reason as KindEmitAck — the off-log
	// capability's outcome must not downgrade across the wire, so it needs an
	// upward ack, not fire-and-forget.
	KindAccessAck Kind = "access_ack"
	// KindSchedule (remote→host): the bound actor invoked its time-axis capability
	// (schedule/cancel a self-targeted timer). Opaque payload, relayed to the host's
	// injected schedule RelaySink; the port does not decode it. FIFO-no-id, same as
	// KindEmit/KindAccess.
	KindSchedule Kind = "schedule"
	// KindScheduleAck (host→remote): the host's authoritative verdict for one
	// KindSchedule (the opaque schedule response bytes + any host-side error).
	KindScheduleAck Kind = "schedule_ack"
	// KindDetach (remote→host) is an optional graceful close for this exact
	// physical route. It does not mutate actor lifecycle truth.
	KindDetach Kind = "detach"
	// KindDeliverResult (remote→host): a pure delivery-OBSERVATION frame. After the
	// remote host's local Deliver produces a non-Delivered outcome (the addressed
	// cell is not_hosted / mailbox_full / stopped), it reports that verdict UP the
	// wire so the home logs it exactly as its own delivery tap does. Fire-and-forget,
	// unidirectional, NO ack: it is INDEPENDENT of the KindEmit FIFO waiter queue
	// (never enqueued, never correlated) — a lost one only loses an observation
	// (truth-side closure is materialised from the log, not from this frame).
	KindDeliverResult Kind = "deliver_result"
	// KindCancelRequest (remote→host): a bound actor abandons one of ITS OWN
	// in-flight OUTBOUND requests — the caller-side UPSTREAM twin of the host→remote
	// KindCancel. The direction is the substrate reason it is a distinct kind (the
	// pairing precedent = KindEmit/KindDeliver): a
	// daemon-hosted caller cannot mint the host→remote cancel signal, so the
	// substrate carries its "close my outstanding request" intent up its own stream.
	// It carries ONLY the request id (reuses CancelPayload): the actor is implicit
	// (the connection IS that actor), and the host reverse-resolves the target from
	// the request's own audience in the log — the caller self-reports neither target
	// nor its own identity (non-self-report: the host authenticates the sender ==
	// the connection's bound id against the stored request before acting). Fire-and-
	// forget, unidirectional, NO ack (same posture as the downstream KindCancel /
	// KindObs): a lost one only costs the receiver a little wasted work — the
	// request's ExpiresAt deadline still collapses its reqCtx and the caller's
	// closure already owns its own terminal. So it never rides the ack'd on-loop
	// path.
	KindCancelRequest Kind = "cancel_request"
	// Lifecycle control is carried on the actor stream. Fork and End have
	// operation results.
	KindSpawn    Kind = "spawn"
	KindSpawnAck Kind = "spawn_ack"
	KindEnd      Kind = "end"
	KindEndAck   Kind = "end_ack"
)

// MaxFrameBytes caps one length-prefixed JSON frame at 16 MiB.
const MaxFrameBytes = 1 << 24

// Frame is the port-wire envelope. Length-prefixed JSON: a uint32 BE length
// header followed by the JSON-marshalled Frame. It carries only the kind + an
// opaque payload — the connection identifies + authenticates the actor, so no
// per-frame id, token, actor id, or channel id rides along.
type Frame struct {
	Kind    Kind            `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// HandshakePayload is sent remote → host on connect.
type HandshakePayload struct {
	LeaseID    string `json:"lease_id"`
	AttemptKey string `json:"attempt_key"`
}

type SpawnPayload struct {
	RequestID     message.ID      `json:"request_id"`
	Kind          actor.Kind      `json:"kind"`
	Class         string          `json:"class"`
	NameHint      string          `json:"name_hint,omitempty"`
	Config        json.RawMessage `json:"config,omitempty"`
	PlacementKind string          `json:"placement_kind,omitempty"`
	PlacementHost string          `json:"placement_host,omitempty"`
}

type SpawnAckPayload struct {
	ChildID      actor.ActorID `json:"child_id,omitempty"`
	ErrorCode    string        `json:"error_code,omitempty"`
	ErrorMessage string        `json:"error_message,omitempty"`
}

type EndPayload struct {
	Target actor.ActorID `json:"target,omitempty"`
	Reason string        `json:"reason,omitempty"`
}

type EndAckPayload struct {
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// DeliverPayload carries one envelope into the bound actor's mailbox.
type DeliverPayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// EmitPayload carries one envelope the bound actor emitted upward.
type EmitPayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// EmitResult is the host's authoritative verdict for one KindEmit: the write
// outcome the EmitSink produced. It mirrors EVERY verdict field of the harness
// WriteResult — MessageID + Seq on the accepted path, RejectReason + RejectDetail
// on the rejected path — because the writer contract crossing the wire must not
// downgrade: a remote cell's Respond has to observe the SAME verdict a local
// cell's writer returns, not a truncated subset. It is the wire contract's own
// type (not borrowed from harness) so the wire layer owns its surface and never
// depends on the harness package.
type EmitResult struct {
	MessageID    message.ID `json:"message_id"`
	Seq          int64      `json:"seq,omitempty"`
	RejectReason string     `json:"reject_reason,omitempty"`
	RejectDetail string     `json:"reject_detail,omitempty"`
}

// EmitAckPayload is the host's reply to one KindEmit: the EmitResult verdict
// plus Err, the string form of the transport/write error the host's EmitSink
// returned (empty on success). The remote side reconstructs both the verdict
// and the error from this single frame, so a remote cell's Respond observes the
// exact outcome a local cell would.
type EmitAckPayload struct {
	EmitResult
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// RelayAckPayload is the host's reply to one KindAccess / KindSchedule: the
// OPAQUE verdict bytes the injected RelaySink produced (the platform link layer
// owns their shape — ipc stays plane-agnostic), plus Err, the string form of any
// host-side error the sink returned (empty on the ok path). It is the plane-
// agnostic twin of EmitAckPayload: the remote reconstructs both the opaque
// verdict and the error from this single frame, so a remote cell's plane-2 /
// time-axis call observes the exact outcome a local cell would. The wire
// contract is not downgraded across the port.
type RelayAckPayload struct {
	Payload      json.RawMessage `json:"payload,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
}

type errorCoder interface{ ErrorCode() string }

func EncodeError(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	var coded errorCoder
	if errors.As(err, &coded) {
		return coded.ErrorCode(), err.Error()
	}
	return "unknown", err.Error()
}

// DownPayload is the bound actor's death notification — the host turns it into a
// down edge (the actor is implicit — the connection IS that actor).
type DownPayload struct {
	Reason string `json:"reason,omitempty"`
}

// ObsPayload carries one actor-source obs snapshot the bound actor pushed (the
// actor is implicit — the connection IS that actor). Kind + Value are OPAQUE: the
// wire forwards them, never interprets. Value is raw opaque bytes (the adapter's
// vocabulary; the substrate stays type-agnostic).
type ObsPayload struct {
	Kind  string `json:"kind"`
	Value []byte `json:"value,omitempty"`
}

// DeliverResultPayload carries one remote-host delivery observation up the wire
// (KindDeliverResult). The actor is implicit (the connection IS that actor), so
// only the envelope + outcome ride along — never an actor id. It is non-truth,
// best-effort: the home turns it into a structured Warn, never a closure terminal.
type DeliverResultPayload struct {
	EnvelopeID message.ID `json:"envelope_id"`
	Outcome    string     `json:"outcome"` // not_hosted / mailbox_full / stopped
	Detail     string     `json:"detail,omitempty"`
}

// CancelPayload names the in-flight request to cancel on the bound actor (the
// actor is implicit — the connection IS that actor). It carries ONLY the request
// id: no reason rides along (deadline-driven cancel never crosses the wire — each
// end builds its own deadline from ExpiresAt — so KindCancel carries the single
// case of a caller actively abandoning; a diagnostic reason is additive when a
// real consumer wants it).
type CancelPayload struct {
	RequestID message.ID `json:"request_id"`
}

// Codec encodes / decodes port frames on length-prefixed buffered IO. It is
// medium-agnostic (any io.Reader/io.Writer: local pipe or net.Conn).
type Codec struct {
	r   *bufio.Reader
	w   io.Writer
	wmu sync.Mutex
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
	wire := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(wire[:4], uint32(len(raw)))
	copy(wire[4:], raw)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	n, err := c.w.Write(wire)
	if err != nil {
		return err
	}
	if n != len(wire) {
		return io.ErrShortWrite
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
	// Read-local buffer: no shared Codec field, so concurrent readers (or a
	// future second reader) cannot corrupt each other. Frames are infrequent
	// control/deliver messages — a per-frame alloc is not a hot path.
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return Frame{}, err
	}
	var f Frame
	if err := json.Unmarshal(buf, &f); err != nil {
		return Frame{}, fmt.Errorf("ipc: unmarshal: %w", err)
	}
	return f, nil
}
