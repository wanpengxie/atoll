package behavior

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
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

// Caller is an actor-private, caller-scoped closure manager (the timeout half
// of gen_server:call). It is owned by one actor.
//
// CONCURRENCY PROOF: `pending` is touched ONLY on the cell goroutine — Arm,
// Match, and Stop are all invoked from the actor's Receive/Stop, which the host
// runs serially on the single cell goroutine. So `pending` needs no lock. A
// timer's fireTimeout runs OFF the cell goroutine (the timer's own goroutine)
// but NEVER touches `pending`: it holds only an immutable request snapshot, the
// caller's sender, and the writer (the writer is concurrency-safe per the
// ResponseWriter contract). The race between a timer fire and a late real
// terminal from the receiver is resolved by the harness one-terminal-per-request
// UNIQUE INDEX — the loser gets WriteOutcome.Duplicate=true, which is benign.
type Caller struct {
	sender  message.Sender
	writer  ResponseWriter
	clock   func() time.Time
	pending map[message.ID]*time.Timer // key=request id; value=timer (nil = in-flight, no deadline)
}

// NewCaller constructs a Caller. sender = the owning actor's own identity (the
// sender stamped on author#2 timeout terminals it produces).
func NewCaller(sender message.Sender, writer ResponseWriter, clock func() time.Time) *Caller {
	return &Caller{
		sender:  sender,
		writer:  writer,
		clock:   clock,
		pending: map[message.ID]*time.Timer{},
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
// ONLY the immutable snapshot, the caller's sender, and the writer — it NEVER
// touches `pending`. It commits an unanswered_timeout terminal to truth
// (audience = caller); the result is ignored (a duplicate = benign loss to a
// real late terminal; an error leaves the caller covered by no other path here,
// which is the accepted guardrail — caller-scoped timeout is the last resort).
// The caller does NOT learn of the timeout here: that terminal fans back to the
// caller's mailbox and Match clears `pending` on the cell goroutine (mailbox is
// the sole ingress).
func (c *Caller) fireTimeout(req *message.Envelope) {
	term, err := BuildResponseFromRequest(req, c.clock, c.sender, CorrelationKey(req.ID), ResponseSpec{
		Status: "failed",
		Reason: string(message.TerminalUnansweredTimeout),
	})
	if err != nil {
		return
	}
	_, _ = c.writer.Write(context.Background(), term)
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

// isFinalResponse reports whether env is a final (terminal) response. It parses
// env.payload.status and defers to message.IsFinalStatus. Internal helper used
// by Caller.Match to decide closure. A non-response or unparseable payload is
// not final.
func isFinalResponse(env *message.Envelope) bool {
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
