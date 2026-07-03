package link

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// The wire payloads for the plane-2 (access/state) and time-axis (schedule) arms.
// They live HERE in link — the composition layer — not in ipc: ipc is a protocol-
// only wire leaf and must not import runtime siblings (schedule imports actorrt,
// which imports ipc — an ipc→schedule edge would cycle). The substrate port
// forwards these bytes OPAQUELY (RelaySink), so only the two link ends (home
// emitSink-side and daemon proxy-side) ever decode them.

// accessScope selects which minter face the home welds the caller onto — the
// channel-scoped tree (Mint) or the actor-scoped collapsed branch (MintState).
// State is not a separate frame family: it is Access's collapsed branch, carried
// on the same KindAccess arm distinguished only by this field.
type accessScope string

const (
	accessScopeChannel accessScope = "channel"
	accessScopeState   accessScope = "state"
)

// accessRequest is the daemon→home KindAccess payload: the scope discriminator
// plus the proto Invocation. Invocation.Caller MUST be empty on the wire — the
// home welds the connection's authenticated id (fail-fast on a non-empty caller,
// exactly like the pen rejecting a pre-filled Sender).
type accessRequest struct {
	Scope accessScope       `json:"scope"`
	Inv   access.Invocation `json:"inv"`
}

// accessResponse is the home→daemon KindAccess verdict: the accessdoor.Outcome
// fields. A host-side Go error (structural malformed / not-live fence) rides the
// ipc RelayAckPayload.Err instead, so a remote cell observes the same (Outcome,
// error) split a local cell does.
type accessResponse struct {
	Value        []byte               `json:"value,omitempty"`
	Found        bool                 `json:"found,omitempty"`
	RejectReason access.FailureReason `json:"reject_reason,omitempty"`
}

// scheduleMethod names which ScheduleHandle call the frame carries.
type scheduleMethod string

const (
	scheduleMethodSchedule scheduleMethod = "schedule"
	scheduleMethodCancel   scheduleMethod = "cancel"
)

// scheduleRequest is the daemon→home KindSchedule payload. The whole ScheduleReq
// is carried, so CorrelationID (the caller's causal coordinate) crosses the wire
// intact — dropping it would lose the domain session semantics the substrate is
// obliged to relay. No author field: author is welded at the home Mint, never
// self-reported (mirrors access's empty Caller).
type scheduleRequest struct {
	Method scheduleMethod       `json:"method"`
	Req    schedule.ScheduleReq `json:"req,omitempty"`
	ID     schedule.TimerID     `json:"id,omitempty"`
}

// scheduleResponse is the home→daemon KindSchedule verdict (the minted TimerID on
// the schedule path; empty on cancel). Errors ride RelayAckPayload.Err.
type scheduleResponse struct {
	ID schedule.TimerID `json:"id,omitempty"`
}

// ---------------------------------------------------------------------------
// relayClient — the daemon-side FIFO round-trip over one opaque capability arm.
// ---------------------------------------------------------------------------

// errRelayClosed is returned to a blocked or new round-trip once the arm is torn
// down (the connection died with an invocation in flight).
var errRelayClosed = errors.New("link: relay arm closed")

// relayClient is the out-of-process end of one opaque capability arm (KindAccess
// or KindSchedule) — the plane-agnostic twin of RemoteWriter. It sends a request
// frame of its fixed kind and blocks until the matching ack returns (FIFO, no id,
// receipt-order — the host acks on its single read loop). Concurrent round-trips
// pipeline: emits are written in mutex order and waiters enqueued in the same
// order, matching the order the host receives and acks them.
type relayClient struct {
	codec       *ipc.Codec
	requestKind ipc.Kind

	// writeMu serialises "enqueue waiter + write request" as one atomic step, so
	// on-wire order == FIFO waiter order (same discipline as RemoteWriter.writeMu).
	writeMu sync.Mutex

	mu      sync.Mutex
	pending []chan relayAck
	closed  bool
}

// relayAck carries one resolved ack back to the blocked round-trip: the opaque
// response bytes and the host-side error (reconstructed from RelayAckPayload.Err).
// transport marks the arm-closed sentinel — a teardown with the request in flight,
// NOT a host verdict — so roundTrip surfaces it as transportErr (unconfirmed →
// outcome_unknown on access) rather than as a definite ackErr.
type relayAck struct {
	payload   json.RawMessage
	err       error
	transport bool
}

func newRelayClient(codec *ipc.Codec, requestKind ipc.Kind) *relayClient {
	return &relayClient{codec: codec, requestKind: requestKind}
}

// roundTrip sends payload as a request frame and blocks for the ack. It reports
// three distinct outcomes:
//   - transportErr != nil: the op GENUINELY crossed the wire and its result is now
//     UNCONFIRMED (the ctx was cancelled AFTER the frame was sent, or the arm died
//     with the request in flight) — the caller surfaces this as an in-flight
//     unknown (access → outcome_unknown, schedule → error). A pre-send wire-write
//     failure also lands here (nothing to confirm either way);
//   - transportErr == nil, ackErr != nil: a DEFINITE, op-did-not-produce-an-unknown
//     failure — either the host returned a definite error verdict (structural /
//     not-live), OR the ctx was ALREADY cancelled before the frame left (a pre-send
//     abort: the op provably never reached the home). Both are relayed as a plain
//     error on both arms — NEVER outcome_unknown, because the op verifiably did not
//     execute-with-unknown-result;
//   - both nil: ackPayload is the host's opaque verdict bytes.
func (c *relayClient) roundTrip(ctx context.Context, payload []byte) (ackPayload json.RawMessage, ackErr error, transportErr error) {
	// Pre-send ctx check: an already-cancelled ctx means the frame never leaves the
	// wire, so the op provably did not reach the home — a DEFINITE non-execution,
	// surfaced through the ackErr slot (a plain error), never transportErr. This is
	// the pre/post-send split: outcome_unknown is reserved for an op that actually
	// crossed the wire (post-send cancel / in-flight arm death below), never one
	// that never sent.
	if err := ctx.Err(); err != nil {
		return nil, err, nil
	}

	waiter := make(chan relayAck, 1)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errRelayClosed
	}
	c.pending = append(c.pending, waiter)
	c.mu.Unlock()

	if err := c.codec.Write(ipc.Frame{Kind: c.requestKind, Payload: json.RawMessage(payload)}); err != nil {
		c.removeTailWaiter(waiter)
		return nil, nil, err
	}

	select {
	case <-ctx.Done():
		// POST-SEND cancel: the frame is already on the wire, so the op is
		// GENUINELY in flight and its result unconfirmed → transportErr (→
		// outcome_unknown on access), NOT the pre-send definite-error path above.
		// Waiter abandoned in place: the host still acks in receipt order and
		// deliverAck consuming an abandoned waiter is harmless (buffered chan, no
		// reader) — the FIFO head is still consumed, keeping the queue aligned.
		return nil, nil, ctx.Err()
	case r := <-waiter:
		if r.transport {
			// The arm was torn down with this request in flight (connection died):
			// nothing was confirmed — surface it as transportErr, not a host verdict.
			return nil, nil, r.err
		}
		return r.payload, r.err, nil
	}
}

// removeTailWaiter drops waiter after a failed wire write (it is the tail —
// writeMu has been held since enqueue — but deliverAck may have popped the head
// meanwhile, so it locates by identity).
func (c *relayClient) removeTailWaiter(waiter chan relayAck) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.pending) - 1; i >= 0; i-- {
		if c.pending[i] == waiter {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return
		}
	}
}

// deliverAck routes one inbound ack into the FIFO head waiter (the wire pins acks
// to receipt order, so the head is always the correct target).
func (c *relayClient) deliverAck(ack ipc.RelayAckPayload) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return // stray ack with no waiter (upstream protocol violation); ignore
	}
	waiter := c.pending[0]
	c.pending = c.pending[1:]
	c.mu.Unlock()

	var err error
	if ack.Err != "" {
		err = errors.New(ack.Err)
	}
	waiter <- relayAck{payload: ack.Payload, err: err}
}

// close fails every pending round-trip with errRelayClosed and rejects new ones.
func (c *relayClient) close() {
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
		waiter <- relayAck{err: errRelayClosed, transport: true}
	}
}

// ---------------------------------------------------------------------------
// remote handles — the daemon-side capability faces an out-of-process cell holds.
// ---------------------------------------------------------------------------

// remoteAccessHandle is the out-of-process end of accessdoor.AccessHandle: a
// relay-only proxy over one access arm. Like RemoteWriter it injects NO identity
// (Caller stays empty — the home welds the authenticated bound id) and holds no
// authorization state. One relayClient backs BOTH the channel-scoped and actor-
// scoped (State) faces, distinguished by scope — they share the arm's FIFO queue.
type remoteAccessHandle struct {
	relay *relayClient
	scope accessScope
}

// Invoke satisfies accessdoor.AccessHandle over the wire. A transport failure
// (unconfirmed in-flight) yields the outcome_unknown VERDICT — the one reason
// only the wire path can produce (an in-proc invoke is synchronous and never
// does). A definite host error (malformed / not-live) is returned as-is.
func (h *remoteAccessHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	payload, err := json.Marshal(accessRequest{
		Scope: h.scope,
		Inv:   access.Invocation{Resource: id, Operation: op, Args: args, Grant: grant},
	})
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if ackErr != nil {
		return accessdoor.Outcome{}, ackErr
	}
	var resp accessResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{Value: resp.Value, Found: resp.Found, RejectReason: resp.RejectReason}, nil
}

// remoteScheduleHandle is the out-of-process end of schedule.ScheduleHandle: a
// relay-only proxy over the schedule arm. No author injection (the home welds it).
type remoteScheduleHandle struct {
	relay *relayClient
}

// Schedule satisfies schedule.ScheduleHandle over the wire. A transport failure is
// a plain error (the time axis has no unknown-verdict slot — the timer may or may
// not exist, and the caller sees an error, current at-least-once semantics).
func (h *remoteScheduleHandle) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodSchedule, Req: req})
	if err != nil {
		return "", err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return "", txErr
	}
	if ackErr != nil {
		return "", ackErr
	}
	var resp scheduleResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Cancel satisfies schedule.ScheduleHandle over the wire.
func (h *remoteScheduleHandle) Cancel(ctx context.Context, id schedule.TimerID) error {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodCancel, ID: id})
	if err != nil {
		return err
	}
	_, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return txErr
	}
	return ackErr
}

// Compile-time proof the relay proxies satisfy the substrate capability contracts
// — an out-of-process cell's plane-2 / time-axis handle is indistinguishable from
// a local one (the whole point of the arms).
var (
	_ accessdoor.AccessHandle = (*remoteAccessHandle)(nil)
	_ schedule.ScheduleHandle = (*remoteScheduleHandle)(nil)
)
