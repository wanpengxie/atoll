// Package closure implements the sender-side, caller-scoped closure
// behaviour for the actor runtime: a request's closure is the SENDER's
// responsibility, not a global substrate scanner.
//
// Model (actor-runtime-construction-spec.md §0.5 / §1, closure author #2):
// when an actor emits a kind=request expecting a final response, it Tracks
// that request here. If no final response is observed before the caller's
// deadline (R5 default, INVARIANT-14), the per-request timer fires and the
// sender writes its OWN caller-scoped terminal — status=failed,
// reason=unanswered_timeout — meaning "I, the caller, stopped waiting"
// (NOT "the receiver failed"; the sender has no authority over receiver
// state). The terminal's parent_id pairs it to the request (proto §2.8
// strong-protocol closure pairing); audience is the request sender itself.
//
// State is in-process and volatile: on sender crash the pending set is lost
// (let-it-crash, decision D2). There is deliberately NO global scanner and
// NO persistence — the only durable record is the channel log, from which a
// future version may rebuild pending by parent_id (additive; not built).
//
// kernel placement: this is the closure CONTRACT behaviour shared by every
// request sender (agent / SDK / worker / in-daemon actor); the only layer
// all of them may import is kernel. It uses stdlib timers only — no backend
// dependency — so it stays within the kernel-purity invariant (INVARIANT-1
// forbids concrete backends, not stdlib mechanism). The actual write path is
// injected as a SubmitFn callback so kernel never imports runtime/harness.
package closure

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// DefaultTimeout is the R5 caller-deadline default (INVARIANT-14): a sender
// waits this long for a final response before closing its own request with a
// caller-scoped unanswered_timeout. A long-running request type overrides it
// per Tracker.
const DefaultTimeout = 30 * time.Second

// SubmitFn writes a sender-authored terminal envelope through whatever write
// path the sender uses (daemon harness chain, worker IPC, SDK HTTP). The
// Tracker builds the envelope; the sender's transport appends it. A
// HarnessTerminalDuplicate outcome (the receiver answered first, concurrently)
// is benign and MUST NOT be treated as an error by the submitter.
type SubmitFn func(ctx context.Context, terminal *message.Envelope) error

// Tracker is one sending actor's caller-scoped pending set and timers. One
// Tracker per sender. All methods are safe for concurrent use, but a Tracker
// is owned by a single actor whose mailbox already serialises its logic — the
// internal mutex only guards against the timer goroutine racing Resolve.
type Tracker struct {
	self    message.Sender
	timeout time.Duration
	submit  SubmitFn
	baseCtx context.Context
	nowFn   func() int64

	mu      sync.Mutex
	pending map[message.ID]*time.Timer
	stopped bool
}

// Config configures a Tracker.
type Config struct {
	// Self is the sending actor (becomes the terminal's sender). Required.
	Self message.Sender
	// Timeout is the caller deadline; <=0 → DefaultTimeout.
	Timeout time.Duration
	// Submit appends the caller-scoped terminal. Required.
	Submit SubmitFn
	// BaseCtx is the context handed to Submit when a timer fires (the original
	// request context is gone by then). Defaults to context.Background().
	BaseCtx context.Context
	// NowFn returns unix-ms for terminal timestamps. Defaults to time.Now.
	NowFn func() int64
}

// New constructs a Tracker.
func New(cfg Config) *Tracker {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	base := cfg.BaseCtx
	if base == nil {
		base = context.Background()
	}
	nowFn := cfg.NowFn
	if nowFn == nil {
		nowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &Tracker{
		self:    cfg.Self,
		timeout: timeout,
		submit:  cfg.Submit,
		baseCtx: base,
		nowFn:   nowFn,
		pending: make(map[message.ID]*time.Timer),
	}
}

// Track registers an outbound request this sender emitted and arms its
// caller deadline. If req is not a kind=request, or is already tracked, or
// the tracker is stopped, Track is a no-op. The captured fields needed to
// build the terminal (id, channel, sender, type, visibility, correlation)
// are snapshotted so the timer goroutine never touches req later.
func (t *Tracker) Track(req *message.Envelope) {
	if req == nil || req.Kind != message.KindRequest || req.ID == "" {
		return
	}
	terminal := t.buildTerminal(req)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	if _, exists := t.pending[req.ID]; exists {
		return
	}
	reqID := req.ID
	timer := time.AfterFunc(t.timeout, func() { t.fire(reqID, terminal) })
	t.pending[reqID] = timer
}

// Resolve cancels tracking for a request whose final response (from any
// author — receiver, or substrate death) the sender has observed. Idempotent.
func (t *Tracker) Resolve(reqID message.ID) {
	t.mu.Lock()
	timer, ok := t.pending[reqID]
	if ok {
		delete(t.pending, reqID)
	}
	t.mu.Unlock()
	if ok {
		timer.Stop()
	}
}

// Stop cancels all timers and marks the tracker stopped. In-flight requests
// are simply dropped (let-it-crash, D2) — their log entries may dangle, an
// accepted v1 gap. Call on actor teardown.
func (t *Tracker) Stop() {
	t.mu.Lock()
	timers := make([]*time.Timer, 0, len(t.pending))
	for id, timer := range t.pending {
		timers = append(timers, timer)
		delete(t.pending, id)
	}
	t.stopped = true
	t.mu.Unlock()
	for _, timer := range timers {
		timer.Stop()
	}
}

// Pending reports how many requests are currently awaiting closure. Test/
// observability aid.
func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// fire is the timer callback: if the request is still pending, write the
// caller-scoped terminal and drop it. Runs on the timer's own goroutine.
func (t *Tracker) fire(reqID message.ID, terminal *message.Envelope) {
	t.mu.Lock()
	_, ok := t.pending[reqID]
	if ok {
		delete(t.pending, reqID)
	}
	t.mu.Unlock()
	if !ok {
		return // resolved or stopped between fire and lock
	}
	// Stamp the firing timestamp; the submit path assigns ts_received/seq.
	terminal.TS = t.nowFn()
	_ = t.submit(t.baseCtx, terminal)
}

// callerTimeoutPayload is the caller-scoped unanswered_timeout response body.
type callerTimeoutPayload struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

// buildTerminal constructs the caller-scoped unanswered_timeout final
// response for req, mirroring the canonical response shape (Type echoes the
// request; audience is the request sender; correlation falls back to the
// request id). The deterministic ID lets the store's terminal unique index
// dedupe against a concurrent receiver final.
func (t *Tracker) buildTerminal(req *message.Envelope) *message.Envelope {
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}
	payload, _ := json.Marshal(callerTimeoutPayload{
		Status: "failed",
		Reason: string(message.TerminalUnansweredTimeout),
		Detail: "caller deadline elapsed; sender stopped waiting (caller-scoped)",
	})
	return &message.Envelope{
		ID:            message.ID("closure:" + string(req.ID) + ":unanswered_timeout"),
		ChannelID:     req.ChannelID,
		Sender:        t.self,
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    req.Visibility,
		Audience:      message.Audience{req.Sender.ID},
	}
}
