package metatool

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// exec.go is the七工具 execution face (期10 S5): the SAME out-station account
// two caller classes touch (spec §1.5). It replaces the historical metatool.Shell
// — which held its OWN correlator + a private author#2 timer (the since-拆删
// behavior.Caller) alongside the
// engine's callLedger. The spec collapses those "two historical fragments" into
// lib/actorbase's JobTable: the机器 moved house, it is not a second one. So the
// seven meta-tools no longer drive a private Shell; they drive the substrate's
// JobTable directly (the async invoke/collect/cancel tools) plus a synchronous
// sys.Call face (the introspection queries) — both are the ONE engine ledger.
//
// Exec bundles those two faces with the wait-policy config the tool convention
// needs (the fast-path window is a UX cap, not the substrate deadline). It is
// built once per incarnation from Sys (drivers/agents/base.ExecFace) and shared by every
// turn; the per-turn RuntimeContext is threaded separately (the curTurn/RC 合一).
type Exec struct {
	// Jobs is the cross-turn out-station account (the engine's JobTable — the
	// SAME machine sys.Call/Pending touch). Drives call_actor / await_result /
	// cancel / list_pending. Required.
	Jobs actorbase.JobTable

	// Call is the synchronous request+await-final face for introspection
	// queries (describe_actor / describe_type / list_actors) — sys.Call+Wait,
	// TRANSIENT (never a cross-turn job in list_pending), so an introspection
	// round-trip never litters the tool-managed job table. Required for those
	// three tools; nil = "tool not configured".
	Call CallFunc

	// Clock returns wall time for the caller's closure deadline (ExpiresAt).
	// Required.
	Clock func() time.Time

	// FastPathWindow caps the inline wait of a default (non-unbounded) call
	// before it degrades to an ack — the sync EXPERIENCE for the model. This is
	// the VOLATILE half平移 from Shell.Await's window: a per-call UX bound, NOT
	// the durable closure deadline. Zero means metatool's 15s default.
	FastPathWindow time.Duration
}

// CallFunc is the synchronous request+await-final face the introspection
// queries drive: build+emit spec as a kind=request, block up to window for its
// final response, return it (ok=false = no final within the window / no
// answer). The one implementation wraps sys.Call+Pending.Wait (drivers/agents/base).
type CallFunc func(ctx context.Context, spec behavior.RequestSpec, window time.Duration) (*message.Envelope, bool, error)

type awaitOutcome struct {
	env *message.Envelope
	ok  bool
	err error
}

// awaitWithProgress is the A-side observation point for provisional rows. It
// drains the same ordered queue that the A ledger receives and returns those
// observations alongside the terminal/ack so the agent model sees them in the
// identical order. A timed-out wait stops consuming; a later await resumes the
// same queue.
func awaitWithProgress(ctx context.Context, jobs actorbase.JobTable, id message.ID, window time.Duration) (*message.Envelope, bool, []any, error) {
	progress := jobs.ProgressEvents(id)
	done := make(chan awaitOutcome, 1)
	go func() {
		env, ok, err := jobs.Await(ctx, id, window)
		done <- awaitOutcome{env: env, ok: ok, err: err}
	}()
	observed := []any{}
	for {
		select {
		case env, open := <-progress:
			if open {
				observed = append(observed, payloadValue(env.Payload))
				continue
			}
			progress = nil
		case outcome := <-done:
			if outcome.ok && progress != nil {
				for env := range progress {
					observed = append(observed, payloadValue(env.Payload))
				}
			}
			return outcome.env, outcome.ok, observed, outcome.err
		}
	}
}

func attachProgress(result ResultValue, observed []any) ResultValue {
	if len(observed) == 0 {
		return result
	}
	if result.Value == nil {
		result.Value = map[string]any{}
	}
	result.Value["progress_events"] = observed
	return result
}

// now returns wall time, defaulting to time.Now if the Clock is unset (a test
// double may leave it nil).
func (x *Exec) now() time.Time {
	if x.Clock != nil {
		return x.Clock()
	}
	return time.Now()
}

// buildRequestSpec is the ONE adapter (复审 P2-2) from a metatool RequestSpec +
// the turn's RuntimeContext to a behavior.RequestSpec — the four elements the
// seven tools share (parent request id, the closure deadline resolved from
// spec.Timeout or DefaultTimeout, and — returned alongside for the ack/window —
// the deadline the wait mode and fast-path window are derived from) are resolved
// here once, never re-拼 per tool. It returns the behavior spec plus the resolved
// deadline.
func (x *Exec) buildRequestSpec(rc RuntimeContext, spec RequestSpec) (behavior.RequestSpec, time.Duration) {
	deadline := spec.Timeout
	if deadline <= 0 {
		deadline = DefaultTimeout
	}
	expiresAt := x.now().Add(deadline).UnixMilli()
	return behavior.RequestSpec{
		Type:       spec.EnvelopeType,
		Payload:    spec.Payload,
		Audience:   message.Audience{actor.ActorID(spec.HandlerActorID)},
		Visibility: message.VisibilityPublic,
		ParentID:   rc.Trigger.Envelope.ID,
		// Correlation root falls back to the trigger's own id when the trigger
		// carries no correlation_id (the same defensive derivation the closure
		// gives — a request never roots correlation at itself by accident).
		CorrelationID: behavior.CorrelationID(rc.Trigger.CorrelationID, rc.Trigger.Envelope.ID),
		ExpiresAt:     &expiresAt,
	}, deadline
}

// resolveWindow computes the inline wait window from the wait mode + the
// resolved deadline, capping the default fast path at the holder's window.
func (x *Exec) resolveWindow(mode WaitMode, deadline time.Duration) time.Duration {
	fpw := x.FastPathWindow
	if fpw <= 0 {
		fpw = FastPathWindow
	}
	switch mode {
	case WaitNone:
		return 0
	case WaitUnbounded:
		return ResolveFastPathWindow(deadline, DefaultTimeout, true)
	default: // WaitFastPath
		window := ResolveFastPathWindow(deadline, DefaultTimeout, false)
		if window > fpw {
			window = fpw
		}
		return window
	}
}

// ExecuteRequest submits a kind=request through the JobTable and waits the
// resolved fast-path window: a final within the window returns inline,
// otherwise an ack. This is the generic dispatch call_actor drives.
func (x *Exec) ExecuteRequest(ctx context.Context, rc RuntimeContext, spec RequestSpec) ResultValue {
	bspec, deadline := x.buildRequestSpec(rc, spec)
	id, err := x.Jobs.Submit(bspec)
	if err != nil {
		return NewError(spec.ToolName, InternalError,
			"emit channel request "+spec.EnvelopeType+": "+err.Error(),
			"Inspect adapter/link status and retry", nil)
	}
	ack := AckDescriptor{
		RequestID: id,
		Accepted:  true,
		Status:    "accepted",
		EstWaitMs: int64(deadline / time.Millisecond),
	}
	window := x.resolveWindow(spec.WaitMode, deadline)
	if window <= 0 {
		return x.ackResult(spec.ToolName, ack)
	}
	finalEnv, ok, observed, awaitErr := awaitWithProgress(ctx, x.Jobs, id, window)
	if awaitErr != nil {
		// The wait was released (ctx / account close) but — unlike the old
		// Shell path — nothing here drops the ledger entry: an in-flight
		// request stays in-flight (author#2 owns its terminal), awaitable
		// again later. A closed account (ErrCallClosed) already deleted itself.
		return attachProgress(NewError(spec.ToolName, InternalError,
			"channel request "+spec.EnvelopeType+" wait failed: "+awaitErr.Error(),
			"Inspect adapter logs; the wait was released but the call keeps running", nil), observed)
	}
	if ok {
		rv, _ := ResultFromResponse(spec.ToolName, *finalEnv)
		return attachProgress(rv, observed)
	}
	return attachProgress(x.ackResult(spec.ToolName, ack), observed)
}

// callSyncFinal is the shared head of the two CallSync* faces: drive a
// synchronous query through the Call face (transient sys.Call, never a
// cross-turn job) and hand back the FINAL envelope, or the machinery failure
// already rendered in the actor-CLI closed error shape. timeoutHint is
// caller-supplied because it is CONTEXT, not drift: describe-class tools
// point the LLM at list_actors, while list_actors itself cannot
// self-reference and points at the system actor instead.
func (x *Exec) callSyncFinal(ctx context.Context, rc RuntimeContext, spec RequestSpec, timeoutHint string) (*message.Envelope, *ResultValue) {
	bspec, deadline := x.buildRequestSpec(rc, spec)
	window := ResolveFastPathWindow(deadline, DefaultTimeout, true)
	finalEnv, ok, err := x.Call(ctx, bspec, window)
	if err != nil {
		rv := NewError(spec.ToolName, InternalError,
			"channel request "+spec.EnvelopeType+" failed: "+err.Error(),
			"Inspect adapter/link status and retry", nil)
		return nil, &rv
	}
	if !ok || finalEnv == nil {
		rv := NewError(spec.ToolName, Timeout,
			spec.EnvelopeType+" did not return a result in time",
			timeoutHint, nil)
		return nil, &rv
	}
	return finalEnv, nil
}

// CallSyncResult drives a synchronous introspection query and renders its
// FINAL response as a ResultValue. Used by describe_actor / describe_type
// (both wrap the result in NormalizeCallActorResult — stage two of the
// render→normalize pipeline).
func (x *Exec) CallSyncResult(ctx context.Context, rc RuntimeContext, spec RequestSpec) ResultValue {
	finalEnv, failure := x.callSyncFinal(ctx, rc, spec,
		"Retry, or call list_actors to confirm the actor is present")
	if failure != nil {
		return *failure
	}
	rv, _ := ResultFromResponse(spec.ToolName, *finalEnv)
	return rv
}

// CallSyncRaw drives a synchronous introspection query and returns the FINAL
// response payload as raw JSON (list_actors' live-catalog path). On failure it
// returns a ready actor-CLI closed-set error carrying the failure's REAL
// category (CallSyncResult's own distinctions, never one collapsed bucket): a
// call/wire error or an illegal non-response final → internal_error; no final
// within the window → timeout; an actor-returned failure → the terminal-reason
// mapping (NormalizeCallActorResult's law). nil failure = rawPayload is the
// live final payload.
func (x *Exec) CallSyncRaw(ctx context.Context, rc RuntimeContext, spec RequestSpec) (rawPayload []byte, failure *ResultValue) {
	// Timeout hint deliberately differs from CallSyncResult's: this face's one
	// caller IS list_actors, which cannot tell the LLM to call list_actors.
	finalEnv, headFailure := x.callSyncFinal(ctx, rc, spec,
		"Retry, or check that the system actor is present")
	if headFailure != nil {
		return nil, headFailure
	}
	if finalEnv.Kind != message.KindResponse {
		rv := NewError(spec.ToolName, InternalError,
			spec.EnvelopeType+" final envelope is not a response (kind="+string(finalEnv.Kind)+")",
			"Inspect adapter logs and retry", nil)
		return nil, &rv
	}
	if reason := ResponseFailureReason(finalEnv.Payload); reason != "" {
		rv := TerminalFailureToActorCLI(spec.ToolName, spec.HandlerActorID, spec.EnvelopeType, reason, nil)
		return nil, &rv
	}
	return finalEnv.Payload, nil
}

// ackResult renders the immediate ack with the standard collect-it guidance.
func (x *Exec) ackResult(toolName string, ack AckDescriptor) ResultValue {
	id := ack.RequestID.String()
	ack.Guidance = "Accepted. Claim the final explicitly with await_result(request_id=" + id + ")."
	ack.ToWait, ack.NotWaiting = newCollectHint(id)
	return AckResult(toolName, ack)
}
