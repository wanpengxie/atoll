package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// call.go is the CALL face — closure author#2, the half of gen_server:call the
// substrate was missing.
//
// First-principles grounding: kernel terminal_reason.go fixes
// unanswered_timeout as author#2 — produced by the CALLER's OWN timer, not a
// scanner. The harness callerSelfClose gate already exists, but the producer
// was never wired (a half-built seam). actorrt has no self-send and the mailbox
// is the sole ingress (actorrt/doc.go) — so the timeout terminal is committed
// to truth (audience = caller) and flows BACK to the caller's mailbox; the
// caller learns of its own timeout by RECEIVING that terminal, never by an
// out-of-band callback. No runtime self-timer / Tick is needed.
//
// TWO TIMER LAYERS, not one (P15): this file's time.AfterFunc is the DURABLE
// terminal deadline — closure author#2's guarantee that every request it Arms
// gets a terminal written to truth, fired once, surviving no matter whether
// anyone is still watching. It is NOT the caller-experience wait bound: that is
// metatool's Shell.Await window (lib/metatool/shell.go), a VOLATILE per-call
// UX timer that can elapse (degrading to an ack) while author#2's timer keeps
// running underneath, still owing its terminal write. Neither is the batch
// table-scan form forward's write-door audit criticised (each is a single
// per-request AfterFunc, not a poll loop over a live table).

// RequestSpec is the caller-supplied shape of a kind=request envelope —
// the call-face mirror of serve's ResponseSpec.
type RequestSpec struct {
	// ID is optional: empty = a fresh uuid. Callers with their own id scheme
	// (e.g. deterministic per-worker ids) override it.
	ID            message.ID
	Type          string // required
	Payload       json.RawMessage
	Audience      message.Audience // required
	Visibility    message.Visibility
	ParentID      message.ID
	CorrelationID message.ID
	// ExpiresAt is the caller's deadline (drives the author#2 timer).
	ExpiresAt *int64
}

// BuildRequest assembles a kind=request envelope — the ONE home for request
// construction defaults, mirroring serve's BuildResponseFromRequest. Bindings
// stamp transport-edge fields (TSReceived) after build; this builder never
// writes.
//
// Sender / ChannelID are left ZERO: identity is substrate-injected by the pen
// at write time (sealed-pen). The calling actor's id is welded onto the pen, so
// the builder neither knows nor fills it.
func BuildRequest(
	clock func() time.Time,
	spec RequestSpec,
) (*message.Envelope, error) {
	if strings.TrimSpace(spec.Type) == "" {
		return nil, fmt.Errorf("behavior: BuildRequest type required")
	}
	if len(spec.Audience) == 0 {
		return nil, fmt.Errorf("behavior: BuildRequest audience required")
	}
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	return &message.Envelope{
		ID:            id,
		TS:            clock().UnixMilli(),
		Kind:          message.KindRequest,
		Type:          strings.TrimSpace(spec.Type),
		Audience:      spec.Audience,
		Payload:       spec.Payload,
		Visibility:    spec.Visibility,
		ParentID:      spec.ParentID,
		CorrelationID: spec.CorrelationID,
		ExpiresAt:     spec.ExpiresAt,
	}, nil
}

// Caller is an actor-private, caller-scoped closure manager (the timeout half
// of gen_server:call). It is owned by one actor.
//
// CONCURRENCY PROOF: `pending` is touched ONLY on the cell goroutine — Arm,
// Match, and Stop are all invoked from the actor's Receive/Stop, which the host
// runs serially on the single cell goroutine. So `pending` needs no lock. A
// timer's fireTimeout runs OFF the cell goroutine (the timer's own goroutine)
// but NEVER touches `pending`: it holds only an immutable request snapshot and
// the pen (concurrency-safe per harness.Pen contract; identity is welded onto
// the pen, not carried alongside). The race between a timer fire and a late real
// terminal from the receiver is resolved by the harness one-terminal-per-request
// UNIQUE INDEX — the loser gets RejectReason=HarnessTerminalDuplicate, which is
// benign.
type Caller struct {
	pen     harness.Pen
	clock   func() time.Time
	pending map[message.ID]*time.Timer
	onFault func(reqID message.ID, err error)
}

// NewCaller builds a caller-scoped closure manager. The calling actor's identity
// is welded onto the pen (sealed-pen), never a separate parameter. onFault is the
// per-request fault face — symmetric with death.go's author#3 onFault.
// fireTimeout is author#2's terminal-write executor; when its write FAILS (a real
// store/reject error, NOT a benign duplicate) the liveness guarantee has a hole
// that must be observable, so the base reports it through onFault. A duplicate
// (the receiver's real terminal already landed) is the happy race and is never a
// fault. nil onFault = faults ignored (the base holds no logger).
func NewCaller(pen harness.Pen, clock func() time.Time, onFault func(reqID message.ID, err error)) *Caller {
	return &Caller{
		pen:     pen,
		clock:   clock,
		pending: map[message.ID]*time.Timer{},
		onFault: onFault,
	}
}

// Arm registers a just-sent request as in-flight and, if req.ExpiresAt != nil,
// starts a caller-scoped timer. Idempotent: a re-Arm of an already-pending
// request is a no-op. CALL SITE: the cell goroutine (the actor armed it right
// after sending the request from Receive).
func (c *Caller) Arm(req *message.Envelope) {
	if req == nil || req.ID == "" {
		return
	}
	if _, exists := c.pending[req.ID]; exists {
		return // idempotent
	}
	if req.ExpiresAt == nil {
		c.pending[req.ID] = nil // in-flight, no deadline
		return
	}
	now := c.clock()
	d := time.Duration(*req.ExpiresAt-now.UnixMilli()) * time.Millisecond
	if d < 0 {
		d = 0
	}
	// Capture an IMMUTABLE snapshot of the request — fireTimeout runs off the
	// cell goroutine and must not alias cell-mutable state.
	snap := *req
	t := time.AfterFunc(d, func() { c.fireTimeout(&snap) })
	c.pending[req.ID] = t
}

// fireTimeout runs OFF the cell goroutine (the timer's goroutine). It holds
// ONLY the immutable snapshot, the pen, and onFault — it NEVER touches
// `pending`. It commits an unanswered_timeout terminal to truth (audience =
// caller). The caller does NOT learn of the timeout here: that terminal fans
// back to the caller's mailbox and Match clears `pending` on the cell goroutine
// (mailbox is the sole ingress).
//
// Identity is welded onto the pen (sealed-pen): the terminal's sender/channel_id
// are left zero by the builder and the pen injects the caller's own welded id at
// write time. There is no caller-context plumbing here.
//
// The write result is no longer swallowed: a duplicate (the receiver's real
// terminal already landed — the happy race) is benign and silent; ANY other
// reject or a non-nil error is a liveness break (this request may now be closed
// by no path) and MUST be observable, so it is reported through onFault — the
// SAME fault face author#3 (death.go) already has. A liveness-guarantee author
// must never fail silently.
func (c *Caller) fireTimeout(req *message.Envelope) {
	term, err := BuildResponseFromRequest(req, c.clock, ResponseSpec{
		Status: message.StatusFailed,
		Reason: string(message.TerminalUnansweredTimeout),
	})
	if err != nil {
		c.fault(req.ID, fmt.Errorf("behavior: caller timeout build: %w", err))
		return
	}
	out, werr := c.pen.Write(context.Background(), term)
	if werr != nil {
		c.fault(req.ID, fmt.Errorf("behavior: caller timeout write: %w", werr))
		return
	}
	if out.RejectReason == harness.HarnessTerminalDuplicate {
		return // the receiver's real terminal won the race — benign.
	}
	if out.RejectReason != "" {
		c.fault(req.ID, fmt.Errorf("behavior: caller timeout rejected: %s (%s)", out.RejectReason, out.RejectDetail))
	}
}

// fault reports a per-request closure fault if onFault is wired.
func (c *Caller) fault(reqID message.ID, err error) {
	if c.onFault != nil {
		c.onFault(reqID, err)
	}
}

// Match reports whether env is a response to one of this caller's in-flight
// requests, and (only when env is a FINAL response) closes that request. CALL
// SITE: the cell goroutine (the actor received env in Receive).
//
// A non-response, or a response whose parent is not pending, returns false — it
// is not a reply to one of our calls. A provisional response of ours returns
// true but does NOT close the request (it stays in flight).
func (c *Caller) Match(env *message.Envelope) bool {
	if env == nil || env.Kind != message.KindResponse {
		return false
	}
	t, ok := c.pending[env.ParentID]
	if !ok {
		return false
	}
	if isFinalResponse(env) {
		if t != nil {
			t.Stop()
		}
		delete(c.pending, env.ParentID)
	}
	return true
}

// Stop stops all timers and clears pending. CALL SITE: the cell goroutine
// (actor teardown).
func (c *Caller) Stop() {
	for id, t := range c.pending {
		if t != nil {
			t.Stop()
		}
		delete(c.pending, id)
	}
}

// IsEnvFinal reports whether env is a final (terminal) response. It parses
// env.payload.status and defers to message.IsFinalStatus. A non-response or
// unparseable payload is not final.
func IsEnvFinal(env *message.Envelope) bool {
	if env == nil || env.Kind != message.KindResponse {
		return false
	}
	var p struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return false
	}
	return message.IsFinalStatus(p.Status)
}

// isFinalResponse is the internal alias used by Caller.Match.
func isFinalResponse(env *message.Envelope) bool {
	return IsEnvFinal(env)
}

// ParseResponseStatus is a defensive payload.status extractor.
func ParseResponseStatus(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	return strings.TrimSpace(obj.Status)
}

// ParseFinalStatus extracts payload.status and reports whether it is a
// Layer 1 final (completed/failed).
func ParseFinalStatus(raw []byte) (string, bool) {
	status := ParseResponseStatus(raw)
	return status, message.IsFinalStatus(strings.TrimSpace(status))
}
