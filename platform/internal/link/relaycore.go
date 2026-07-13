package link

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/wanpengxie/atoll/runtime/ipc"
)

// relayCore is the ONE "FIFO no-id synchronous round-trip" machine both
// out-of-process wire ends are built on: RemoteWriter (the emit dialect,
// remotewriter.go) and relayClient (the plane-2 / time-axis dialect,
// relaywire.go). It sends a request frame of its fixed kind over the port codec
// and blocks until the matching ack returns, correlated purely by receipt order.
// Both ends were once hand-copied twins of this machine; the mechanism now lives
// here exactly once and each end is a thin dialect adapter over it (its wire
// frame kind, its ack type, its close sentinel).
//
// SIX AXIOMS (the machine, the whole machine):
//  1. FIFO head dissolves correlation. The wire carries NO per-request id (pinned
//     in ipc/frame.go): the host acks on its single read loop in receipt order,
//     so a pending queue resolved head-first is the complete and only mechanism.
//  2. "enqueue waiter + write frame" is ONE atomic step under writeMu. That makes
//     on-wire request order == FIFO waiter order: concurrent round-trips pipeline,
//     their frames written in mutex order and their waiters enqueued in the same
//     order, matching the order the host receives and therefore acks them.
//  3. Split locks to avoid deadlock. writeMu is HELD ACROSS the (possibly
//     blocking) codec.Write, but deliverAck/close take only mu, never writeMu —
//     so an inbound ack can always be delivered even while a round-trip is blocked
//     on a synchronous transport. Holding both across the wire write would
//     deadlock: the host cannot ack until it reads the frame, and the reader
//     cannot deliver that ack if it needs the write lock.
//  4. Abandon the slot, do not remove it. On ctx cancellation the waiter is left
//     in place: the host still acks in receipt order and deliverAck consuming an
//     abandoned (buffered, reader-gone) waiter is harmless — the FIFO head is
//     still consumed, keeping the queue aligned with the host's ack order.
//  5. Teardown is settlement as UNCONFIRMED, not failure. close() resolves every
//     in-flight round-trip with the transport flag set (a connection death with
//     the request in flight is not a host verdict) — each adapter surfaces that as
//     its own "unconfirmed" outcome (access → outcome_unknown, writer/schedule →
//     error), carrying closedErr as the sentinel.
//  6. Pre- vs post-send cancellation is boxed. A ctx already cancelled BEFORE the
//     frame leaves is a DEFINITE non-execution (definiteErr) — the op provably
//     never reached the home. A cancellation AFTER the frame is on the wire leaves
//     the op genuinely in flight, its result UNCONFIRMED (transportErr).
//
// The core is unexported and its two instantiations both live in this package —
// the generic does not cross the package boundary.
type relayCore[Ack any] struct {
	codec       *ipc.Codec
	requestKind ipc.Kind
	// closedErr is the sentinel surfaced (as transportErr) to a round-trip torn
	// down at enqueue or in flight. Each dialect supplies its own identity
	// (errRemoteWriterClosed / errRelayClosed) so its consumers can still judge it.
	closedErr error

	// writeMu serialises axiom 2's "enqueue waiter + write frame" atomic step, so
	// on-wire order == FIFO waiter order. It is held across the blocking
	// codec.Write; mu (axiom 3) is taken only for the brief pending-slice
	// mutations, so deliverAck/close stay unblocked while a round-trip is parked.
	writeMu sync.Mutex

	mu      sync.Mutex
	pending []chan coreResult[Ack] // FIFO wait queue; head is the oldest unacked request
	closed  bool
}

// coreResult carries one resolved round-trip back to the blocked caller. transport
// marks axiom 5's teardown settlement (connection died with the request in flight)
// — NOT a host ack — so roundTrip surfaces it as transportErr (unconfirmed) with
// closedErr, never as a definite host verdict. On a normal deliverAck it is false
// and ack holds the host's reconstructed verdict.
type coreResult[Ack any] struct {
	ack       Ack
	transport bool
}

func newRelayCore[Ack any](codec *ipc.Codec, requestKind ipc.Kind, closedErr error) *relayCore[Ack] {
	return &relayCore[Ack]{codec: codec, requestKind: requestKind, closedErr: closedErr}
}

// roundTrip sends payload as a frame of requestKind and blocks for the FIFO head
// ack. It reports three DISJOINT outcomes (axiom 6's pre/post-send split + axiom 5):
//   - definiteErr != nil: the ctx was ALREADY cancelled before the frame left, so
//     the op provably did not reach the home — a DEFINITE non-execution;
//   - transportErr != nil: the op's result is UNCONFIRMED — a pre-send wire-write
//     failure (nothing crossed, nothing to confirm), a POST-send ctx cancel (in
//     flight), or a teardown at enqueue / in flight (closedErr);
//   - both nil: ack is the host's resolved verdict.
//
// Each dialect adapter maps this triple onto its own contract (a plain error, an
// outcome_unknown verdict, etc.) — the core stays contract-agnostic.
func (c *relayCore[Ack]) roundTrip(ctx context.Context, payload []byte) (ack Ack, transportErr error, definiteErr error) {
	var zero Ack
	// Axiom 6 pre-send half: an already-cancelled ctx means the frame never leaves,
	// so the op provably did not execute — surfaced as definiteErr, never transport.
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}

	waiter := make(chan coreResult[Ack], 1)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return zero, c.closedErr, nil // axiom 5: torn down before enqueue → unconfirmed
	}
	c.pending = append(c.pending, waiter)
	c.mu.Unlock()

	if err := c.codec.Write(ipc.Frame{Kind: c.requestKind, Payload: json.RawMessage(payload)}); err != nil {
		// Nothing reached the host: drop this waiter. writeMu guarantees no other
		// round-trip appended after it, so it is the tail (the head only shrinks via
		// deliverAck). Nothing crossed the wire → unconfirmed transportErr.
		c.removeTailWaiter(waiter)
		return zero, err, nil
	}

	select {
	case <-ctx.Done():
		// Axiom 6 post-send half: the frame is already on the wire, so the op is
		// GENUINELY in flight and its result unconfirmed → transportErr (NOT the
		// pre-send definite path). Waiter abandoned in place (axiom 4).
		return zero, ctx.Err(), nil
	case r := <-waiter:
		if r.transport {
			// Axiom 5: the arm was torn down with this request in flight — nothing
			// confirmed, surface the closed sentinel as transportErr, not a verdict.
			return zero, c.closedErr, nil
		}
		return r.ack, nil, nil
	}
}

// removeTailWaiter drops waiter from the pending queue after a failed wire write.
// It is the tail (writeMu held since enqueue), but deliverAck may have popped the
// head meanwhile, so it locates the waiter by identity rather than a fixed index.
func (c *relayCore[Ack]) removeTailWaiter(waiter chan coreResult[Ack]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.pending) - 1; i >= 0; i-- {
		if c.pending[i] == waiter {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return
		}
	}
}

// deliverAck routes one inbound ack into the FIFO head waiter (axiom 1: the wire
// pins acks to receipt order, so the head is always the correct target). The
// adapter reconstructs the dialect Ack from its frame payload before calling this.
func (c *relayCore[Ack]) deliverAck(ack Ack) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return // a stray ack with no waiter (upstream protocol violation); ignore
	}
	waiter := c.pending[0]
	c.pending = c.pending[1:]
	c.mu.Unlock()
	waiter <- coreResult[Ack]{ack: ack}
}

// close settles every pending round-trip as UNCONFIRMED (axiom 5: the transport
// flag distinguishes teardown from a host ack) and rejects subsequent round-trips.
func (c *relayCore[Ack]) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	for _, waiter := range pending {
		waiter <- coreResult[Ack]{transport: true}
	}
}
