package link

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// RemoteWriter is the OUT-OF-PROCESS end of the write contract: a relay-only
// PROXY PEN (harness.Pen) a daemon-hosted remote cell uses to emit upward over
// the port wire and observe the host's authoritative write verdict.
//
// It lives in the platform ASSEMBLY layer, NOT in the substrate. A remote pen is
// the COMPOSITION of two substrate mechanisms — the harness.Pen contract and the
// ipc wire protocol — and composing them into an end-to-end pen is an assembly
// concern, not a kernel-native one (unlike harness.boundPen, whose dependencies
// close entirely inside the truth engine, this one straddles harness + ipc). Its
// SYMMETRIC counterpart is the host-side emitSink (accept.go), which composes the
// same two mechanisms the other way — Mint a pen, write truth, mirror the verdict
// back onto the wire. Both halves of the emit/ack protocol therefore live
// together here in link, beside their consumers (dial.go opens the remote end,
// accept.go serves the host end). This is why moving it out of runtime/ipc lets
// ipc fall back to a protocol-only wire leaf and frees actorrt's compile closure
// of harness.
//
// INVARIANT: the proxy pen NEVER injects identity and NEVER fail-fasts. It only
// relays the envelope up the wire; the identity weld + fail-fast live on the
// HOST side, where the link's emitSink Mints a Pen for the connection's
// authenticated bound id. A daemon cell's behavior leaves Sender.ID/ChannelID
// empty (it has no Minter), the proxy pen relays that empty envelope, and the
// host pen welds the bound identity. So this proxy must stay a pure relay — see
// the platform emit-identity test.
//
// A remote cell has no local truth: its Respond/EmitEvent drive this pen,
// which sends a KindEmit and BLOCKS until the matching KindEmitAck returns. The
// returned harness.WriteResult is reconstructed from that ack, so a remote
// cell's Respond observes the EXACT outcome a local cell's Respond would — the
// write contract is not downgraded across the wire.
//
// Correlation is FIFO with no id (the wire contract, pinned in ipc/frame.go): the
// host acks emits in receipt order, so a pending queue resolved head-first is
// the complete and only mechanism needed. Concurrent Write calls pipeline:
// their emits are written in mutex order and their waiters enqueued in the same
// order, matching the order the host receives and therefore acks them.
type RemoteWriter struct {
	codec *ipc.Codec

	// writeMu serializes "enqueue waiter + write emit to the wire" as one atomic
	// step, so on-wire emit order == FIFO waiter order. It is HELD ACROSS the
	// (possibly blocking) codec.Write — but DeliverAck/Close take only mu, never
	// writeMu, so an incoming ack can always be delivered even while a Write is
	// blocked on a synchronous transport. Holding both locks across the wire
	// write would deadlock: the host cannot ack until it reads the emit, and the
	// remote reader cannot deliver that ack if it needs the same lock.
	writeMu sync.Mutex

	mu      sync.Mutex
	pending []chan ackResult // FIFO wait queue; head is the oldest unacked emit
	closed  bool
}

// ackResult carries one resolved KindEmitAck back to the blocked Write.
type ackResult struct {
	res harness.WriteResult
	err error
}

// errRemoteWriterClosed is returned to a blocked or new Write once the writer is
// torn down (the connection died with emits still in flight).
var errRemoteWriterClosed = errors.New("link: remote writer closed")

// NewRemoteWriter binds a remote writer to codec (the actor's port connection).
// The codec's write side is mutex-guarded, so emits may share it with the
// actor's other outbound frames.
func NewRemoteWriter(codec *ipc.Codec) *RemoteWriter {
	return &RemoteWriter{codec: codec}
}

// Write sends env upward as a KindEmit and blocks until the host returns the
// matching KindEmitAck (FIFO) or ctx is cancelled. It satisfies harness.Pen
// (relay-only): a remote cell's pen seam is this method, so its behavior.Respond
// / behavior.EmitEvent flow to the host harness (truth owner) and observe the
// authoritative verdict. It does NOT inject identity here — the host emitSink's
// Mint welds the bound id (see the type doc invariant).
//
// On ctx cancellation the waiter is abandoned in place: the host still acks in
// receipt order, and DeliverAck resolving an abandoned (already-closed-context)
// waiter is harmless — the FIFO head is still consumed, keeping the queue
// aligned with the host's ack order.
func (w *RemoteWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if env == nil {
		return harness.WriteResult{}, errors.New("link: remote writer nil envelope")
	}
	payload, err := json.Marshal(ipc.EmitPayload{Envelope: *env})
	if err != nil {
		return harness.WriteResult{}, err
	}
	waiter := make(chan ackResult, 1)

	// writeMu makes "enqueue waiter + write emit" atomic with respect to other
	// Writes, so on-wire emit order == FIFO waiter order. mu is taken only for
	// the brief pending-slice mutations; the blocking codec.Write happens under
	// writeMu but NOT under mu, so DeliverAck/Close stay unblocked.
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return harness.WriteResult{}, errRemoteWriterClosed
	}
	w.pending = append(w.pending, waiter)
	w.mu.Unlock()

	if err := w.codec.Write(ipc.Frame{Kind: ipc.KindEmit, Payload: payload}); err != nil {
		// Nothing reached the host: drop this waiter. writeMu guarantees no other
		// Write appended after it, so it is the tail (head pops by DeliverAck only
		// shrink from the front).
		w.removeTailWaiter(waiter)
		return harness.WriteResult{}, err
	}

	select {
	case <-ctx.Done():
		return harness.WriteResult{}, ctx.Err()
	case r := <-waiter:
		return r.res, r.err
	}
}

// removeTailWaiter drops waiter from the pending queue after a failed wire
// write. It is the tail (writeMu held since enqueue), but DeliverAck may have
// popped the head meanwhile, so it locates the waiter by identity rather than
// assuming a fixed index.
func (w *RemoteWriter) removeTailWaiter(waiter chan ackResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.pending) - 1; i >= 0; i-- {
		if w.pending[i] == waiter {
			w.pending = append(w.pending[:i], w.pending[i+1:]...)
			return
		}
	}
}

// DeliverAck routes one inbound KindEmitAck into the FIFO head waiter. The
// remote's single read loop calls this when it decodes a KindEmitAck frame
// (the wire contract pins acks to receipt order, so the head waiter is always
// the correct target). It reconstructs the harness.WriteResult verdict and the
// transport error from the ack payload.
func (w *RemoteWriter) DeliverAck(ack ipc.EmitAckPayload) {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return // a stray ack with no waiter (protocol violation upstream); ignore
	}
	waiter := w.pending[0]
	w.pending = w.pending[1:]
	w.mu.Unlock()

	res := harness.WriteResult{
		MessageID:    ack.MessageID,
		Seq:          ack.Seq,
		RejectReason: harness.HarnessRejectReason(ack.RejectReason),
		RejectDetail: ack.RejectDetail,
	}
	var err error
	if ack.Err != "" {
		err = errors.New(ack.Err)
	}
	waiter <- ackResult{res: res, err: err}
}

// Close fails every pending waiter with errRemoteWriterClosed and rejects
// subsequent Writes. The connection died with emits in flight: those cells must
// see a transport error, not block forever.
func (w *RemoteWriter) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()
	for _, waiter := range pending {
		waiter <- ackResult{err: errRemoteWriterClosed}
	}
}

// Verify the remote writer satisfies the harness Pen contract at compile time —
// the whole point is that a remote cell's pen is indistinguishable from a local
// one. It is a relay-only proxy pen (never injects identity / never fail-fasts);
// the host emitSink's Mint welds the bound identity.
var _ harness.Pen = (*RemoteWriter)(nil)
