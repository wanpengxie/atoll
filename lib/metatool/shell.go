package metatool

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// shell.go is the channel's actor-invocation SHELL (bash positioning): the
// ONE shared home for the correlation + sync/async machinery a client edge
// (an LLM brain, a UI, a gateway) needs to use the channel's actors. No
// single caller owns this — it is "how you call an actor in this channel",
// the universal entry. job control (& / wait / jobs / kill %) is the shell's,
// implemented once and shared by every program; likewise correlation +
// sync/async is the shell's, not each agent's private re-implementation.
//
// The Shell OWNS the complete outbound request lifecycle: it builds the
// request envelope (behavior.BuildRequest), Arms closure author#2
// (behavior.Caller — "every request I send is guaranteed to close"), emits it
// through the harness write door, and tracks it in-flight. The holder feeds
// inbound responses back through Deliver, which Matches author#2 (disarms the
// timeout) and wakes any bounded-window waiter. author#2 IS the responsibility
// of sending a request — it belongs to the shell, exactly as bash owns "the
// job I started, I reap".
//
// behavior.Caller (author#2) stays a BEHAVIOR primitive (the actor-side
// call-face). The Shell HOLDS and DRIVES it — it does not re-implement the
// timeout terminal. Arm fires on the client-edge goroutine (a tool call) and
// Match on the holder's mailbox goroutine (Deliver); the Shell guards every
// caller touch with callerMu so the lock-free single-goroutine base contract
// is honoured by a two-goroutine holder.

type awaitState int

const (
	awaitNotStarted awaitState = iota
	awaitActive
	awaitDone
)

type pendingReq struct {
	ch           chan *message.Envelope
	expectsAwait bool
	state        awaitState
}

// ShellConfig carries the deps the Shell needs to build, emit, and time
// outbound requests. The holder (the client-edge program) supplies its
// identity + write seam at construction.
type ShellConfig struct {
	// Writer is the harness write door: it emits requests and (via author#2)
	// commits timeout terminals to truth. Required.
	Writer harness.Writer

	// ChannelID scopes every emitted request. Required.
	ChannelID channel.ID

	// Sender is the caller identity stamped on outbound requests and on the
	// author#2 timeout terminals. Required.
	Sender message.Sender

	// Clock returns wall time for build + author#2 timers. Required.
	Clock func() time.Time

	// EnvelopeID mints the id for one outbound request given unix-ms. The
	// holder owns its id scheme (e.g. deterministic per-actor ids). Required.
	EnvelopeID func(nowMs int64) message.ID

	// FastPathWindow caps the inline wait of a default (non-unbounded) call
	// before it degrades to an ack — the sync EXPERIENCE for the model.
	// Zero means "use metatool's 15s default".
	FastPathWindow time.Duration

	// OnFault is the per-request closure-fault face for author#2 (symmetric
	// with author#3). nil = faults ignored.
	OnFault func(reqID message.ID, err error)
}

// Shell is the channel's actor-invocation shell. It holds the in-flight
// correlator + closure author#2 and exposes the sync/async call ops the
// meta-tools drive. One Shell is owned by one client-edge holder.
type Shell struct {
	cfg ShellConfig

	mu      sync.Mutex
	pending map[message.ID]*pendingReq

	// callerMu serializes author#2 touches across the holder's two
	// goroutines: Arm on the client-edge (a tool call), Match on the
	// mailbox (Deliver). The behavior.Caller base stays lock-free for
	// single-goroutine actors; this two-goroutine holder adapts.
	callerMu sync.Mutex
	caller   *behavior.Caller
}

// NewShell builds a Shell bound to its holder's identity + write seam. The
// author#2 caller is constructed here (the Shell owns request closure).
func NewShell(cfg ShellConfig) *Shell {
	if cfg.FastPathWindow <= 0 {
		cfg.FastPathWindow = FastPathWindow
	}
	s := &Shell{
		cfg:     cfg,
		pending: make(map[message.ID]*pendingReq),
	}
	s.caller = behavior.NewCaller(cfg.Sender, cfg.Writer, cfg.Clock, cfg.OnFault)
	return s
}

// Stop disarms all author#2 timers and clears in-flight state. CALL SITE:
// the holder's teardown (cell goroutine).
func (s *Shell) Stop() {
	s.callerMu.Lock()
	if s.caller != nil {
		s.caller.Stop()
	}
	s.callerMu.Unlock()
}

// --- correlator core (in-flight futures) ---

// register records an in-flight request. If expectsAwait is true, a final
// that arrives before Await parks is buffered for it.
func (s *Shell) register(id message.ID, expectsAwait bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pending[id]; ok {
		return
	}
	s.pending[id] = &pendingReq{
		ch:           make(chan *message.Envelope, 1),
		expectsAwait: expectsAwait,
		state:        awaitNotStarted,
	}
}

// InFlight reports whether a future exists for id (await_result's guard).
func (s *Shell) InFlight(id message.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[id]
	return ok
}

// cancel removes the future for id.
func (s *Shell) cancel(id message.ID) {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
}

// Pending returns the in-flight request ids (list_pending).
func (s *Shell) Pending() []message.ID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]message.ID, 0, len(s.pending))
	for id := range s.pending {
		ids = append(ids, id)
	}
	return ids
}

// Abandon drops the local waiter for id (abandon semantics — no downstream
// cancel; the result still returns as a new message).
func (s *Shell) Abandon(id message.ID) { s.cancel(id) }

// Deliver routes an inbound response envelope to its registered waiter and
// Matches author#2 (disarms the timeout) on a final. CALL SITE: the holder's
// mailbox goroutine (Receive). Returns whether a waiter CONSUMED it — the
// holder turns a final nobody consumed into a new turn.
func (s *Shell) Deliver(env *message.Envelope) (consumed bool) {
	if env == nil {
		return false
	}
	// author#2: a final disarms the per-request timeout. callerMu guards the
	// lock-free base against the concurrent Arm on the client-edge goroutine.
	s.matchCaller(env)

	final := behavior.IsEnvFinal(env)

	s.mu.Lock()
	p, ok := s.pending[env.ParentID]
	if !ok {
		s.mu.Unlock()
		return false
	}

	switch {
	case p.state == awaitActive:
		if !final {
			// Provisional — swallow; don't wake the Await goroutine.
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return true

	case p.state == awaitNotStarted && p.expectsAwait:
		if !final {
			s.mu.Unlock()
			return true
		}
		// Buffer the final for a future Await.
		s.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return true

	default:
		// awaitDone (timed out/cancelled) OR !expectsAwait — no active waiter.
		if final {
			delete(s.pending, env.ParentID)
		}
		s.mu.Unlock()
		return false
	}
}

// Await blocks until the final for id arrives, the window elapses, or ctx is
// done. window <= 0 means "do not wait at all".
func (s *Shell) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if window <= 0 {
		return nil, false, nil
	}
	s.mu.Lock()
	p, ok := s.pending[id]
	if !ok {
		s.mu.Unlock()
		return nil, false, nil
	}
	p.state = awaitActive
	s.mu.Unlock()

	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case env := <-p.ch:
		s.cancel(id)
		return env, true, nil
	case <-timer.C:
		if env, ok := s.reconcileAfterWait(id); ok {
			return env, true, nil
		}
		return nil, false, nil
	case <-ctx.Done():
		if env, ok := s.reconcileAfterWait(id); ok {
			return env, true, nil
		}
		return nil, false, ctx.Err()
	}
}

// reconcileAfterWait resolves the timeout/cancel vs buffered-final race: a
// final that landed in the buffer just before the timer fired must not be
// stranded (a ghost in-flight entry that list_pending reports forever). If a
// final is buffered, consume it and close the future; otherwise mark the
// await done (a later retry re-arms it).
func (s *Shell) reconcileAfterWait(id message.ID) (*message.Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[id]
	if !ok {
		return nil, false
	}
	select {
	case env := <-p.ch:
		delete(s.pending, id)
		return env, true
	default:
		p.state = awaitDone
		return nil, false
	}
}

// --- author#2 lifecycle (held + driven, not re-implemented) ---

// armCaller / matchCaller serialize author#2 touches across the holder's two
// goroutines (Arm on the client edge, Match on the mailbox).
func (s *Shell) armCaller(env *message.Envelope) {
	s.callerMu.Lock()
	defer s.callerMu.Unlock()
	if s.caller != nil {
		s.caller.Arm(env)
	}
}

func (s *Shell) matchCaller(env *message.Envelope) {
	s.callerMu.Lock()
	defer s.callerMu.Unlock()
	if s.caller != nil {
		s.caller.Match(env)
	}
}

// --- outbound request lifecycle ---

// buildRequest assembles the kind=request envelope through the behavior
// call-face builder (ONE home for request defaults), then stamps the
// binding-edge fields the shell owns (deterministic id, TSReceived). ExpiresAt
// carries the caller's deadline so author#2's Arm has a timer to set.
func (s *Shell) buildRequest(rc RuntimeContext, spec RequestSpec) (message.Envelope, error) {
	now := s.cfg.Clock().UnixMilli()
	expiresAt := now + int64(spec.Timeout/time.Millisecond)
	env, err := behavior.BuildRequest(s.cfg.ChannelID, s.cfg.Sender,
		func() time.Time { return time.UnixMilli(now) },
		behavior.RequestSpec{
			ID:            s.cfg.EnvelopeID(now),
			Type:          spec.EnvelopeType,
			Payload:       spec.Payload,
			Audience:      message.Audience{actor.ActorID(spec.HandlerActorID)},
			Visibility:    message.VisibilityPublic,
			ParentID:      rc.Trigger.Envelope.ID,
			// Correlation root falls back to the trigger's own id when the
			// trigger carries no correlation_id (a non-normalised trigger) —
			// the same defensive derivation behavior.CorrelationID gives the
			// closure, so a request never roots correlation at itself by accident.
			CorrelationID: behavior.CorrelationID("", rc.Trigger.CorrelationID, rc.Trigger.Envelope.ID),
			ExpiresAt:     &expiresAt,
		})
	if err != nil {
		return message.Envelope{}, err
	}
	env.TSReceived = now
	return *env, nil
}

// submit is the three-step call: register the future (subscribe-before-send),
// commit the request through the harness, and Arm closure author#2. Any
// failure unwinds the future registration.
func (s *Shell) submit(ctx context.Context, env message.Envelope, expectsAwait bool) error {
	s.register(env.ID, expectsAwait)
	if err := s.write(ctx, env); err != nil {
		s.cancel(env.ID)
		return err
	}
	s.armCaller(&env)
	return nil
}

// write commits one envelope through the harness chain; a reject is an error
// (the caller must know its emit did not land).
func (s *Shell) write(ctx context.Context, env message.Envelope) error {
	res, err := s.cfg.Writer.Write(ctx, &env)
	if err != nil {
		return err
	}
	if !res.Accepted() {
		return &emitRejectedError{reason: string(res.RejectReason), detail: res.RejectDetail}
	}
	return nil
}

type emitRejectedError struct {
	reason string
	detail string
}

func (e *emitRejectedError) Error() string {
	return "metatool: emit rejected: " + e.reason + " (" + e.detail + ")"
}

// --- meta-tool dispatch ops ---

// ExecuteRequest emits an envelope request and waits the resolved fast-path
// window: a final within the window returns inline, otherwise an ack. This is
// the generic dispatch the call_actor / describe_actor / describe_type
// meta-tools drive.
func (s *Shell) ExecuteRequest(ctx context.Context, rc RuntimeContext, spec RequestSpec) ResultValue {
	if spec.Timeout <= 0 {
		spec.Timeout = DefaultTimeout
	}
	env, buildErr := s.buildRequest(rc, spec)
	if buildErr != nil {
		return NewError(spec.ToolName, InternalError,
			"build channel request "+spec.EnvelopeType+": "+buildErr.Error(),
			"Inspect the request spec and retry", nil)
	}

	expectsAwait := spec.WaitMode != WaitNone
	if err := s.submit(ctx, env, expectsAwait); err != nil {
		return NewError(spec.ToolName, InternalError,
			"emit channel request "+spec.EnvelopeType+": "+err.Error(),
			"Inspect adapter/link status and retry", nil)
	}
	ack := AckDescriptor{
		RequestID: env.ID,
		Accepted:  true,
		Status:    "accepted",
		EstWaitMs: int64(spec.Timeout / time.Millisecond),
	}

	window := s.resolveWindow(spec)
	if window <= 0 {
		return s.ackResult(spec.ToolName, ack)
	}

	finalEnv, ok, awaitErr := s.Await(ctx, env.ID, window)
	if awaitErr != nil {
		s.cancel(env.ID)
		return NewError(spec.ToolName, InternalError,
			"channel request "+spec.EnvelopeType+" wait failed: "+awaitErr.Error(),
			"Inspect adapter logs; the request may have been abandoned", nil)
	}
	if ok {
		rv, _ := ResultFromResponse(spec.ToolName, *finalEnv)
		return rv
	}
	return s.ackResult(spec.ToolName, ack)
}

// ExecuteReservedRaw emits a reserved-type request and returns the FINAL
// response payload as raw JSON (list_actors' live-catalog path).
func (s *Shell) ExecuteReservedRaw(ctx context.Context, rc RuntimeContext, spec RequestSpec) (rawPayload []byte, ok bool) {
	if spec.Timeout <= 0 {
		spec.Timeout = DefaultTimeout
	}
	env, buildErr := s.buildRequest(rc, spec)
	if buildErr != nil {
		return nil, false
	}
	if err := s.submit(ctx, env, true); err != nil {
		return nil, false
	}
	window := ResolveFastPathWindow(spec.Timeout, DefaultTimeout, true)
	finalEnv, ok, awaitErr := s.Await(ctx, env.ID, window)
	if awaitErr != nil {
		s.cancel(env.ID)
		return nil, false
	}
	if !ok || finalEnv == nil || finalEnv.Kind != message.KindResponse {
		return nil, false
	}
	if ResponseFailureReason(finalEnv.Payload) != "" {
		return nil, false
	}
	return finalEnv.Payload, true
}

// resolveWindow computes the inline wait window from the spec's wait mode,
// capping the default fast path at the holder's configured window.
func (s *Shell) resolveWindow(spec RequestSpec) time.Duration {
	switch spec.WaitMode {
	case WaitNone:
		return 0
	case WaitUnbounded:
		return ResolveFastPathWindow(spec.Timeout, DefaultTimeout, true)
	default: // WaitFastPath
		window := ResolveFastPathWindow(spec.Timeout, DefaultTimeout, false)
		if window > s.cfg.FastPathWindow {
			window = s.cfg.FastPathWindow
		}
		return window
	}
}

// ackResult renders the immediate ack with the standard collect-it guidance.
func (s *Shell) ackResult(toolName string, ack AckDescriptor) ResultValue {
	id := ack.RequestID.String()
	ack.Guidance = "Accepted. To wait, call await_result(request_id=" + id +
		"). If you do not wait, the result returns as a new message (parent_id=" + id + ")."
	ack.ToWait = ToWaitHint{
		Tool:   "await_result",
		Params: map[string]any{"request_id": id},
	}
	ack.NotWaitng = "result returns as kind=response, parent_id=" + id + " new turn trigger"
	return AckResult(toolName, ack)
}
