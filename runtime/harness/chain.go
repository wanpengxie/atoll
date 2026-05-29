package harness

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

// Chain is the concrete runtime implementation of kernel/harness.Chain.
// It assembles the 9 numbered steps declared in proto-layer1 §2.0
// (Step 0 entry gate + Step 1-9 main pipeline) and runs them in stable
// ascending-ID order, short-circuiting on the first reject (or on the
// first idempotent dedupe hit).
//
// Construct with New; Write is safe for concurrent use as long as Deps
// implementations are concurrent-safe (the standard sqlite-backed store
// / actor registry satisfy this).
type Chain struct {
	deps  Deps
	steps []khar.Step
}

// New assembles a Chain from Deps. Returns an error when Deps is
// incomplete or any step refuses to construct.
func New(deps Deps) (*Chain, error) {
	if err := deps.Validate(); err != nil {
		return nil, err
	}
	if deps.NowMs == nil {
		deps.NowMs = func() int64 { return time.Now().UnixMilli() }
	}
	if deps.Logger == nil {
		deps.Logger = NoopLogger{}
	}
	if deps.Metrics == nil {
		deps.Metrics = NoopMetrics{}
	}

	steps := []khar.Step{
		newStepCallerAuth(deps),
		newStepEnvelopeShape(deps),
		newStepDedupe(deps),
		newStepNormalize(deps),
		newStepSenderConsistent(deps),
		newStepTypeRegistered(deps),
		newStepAudienceResolve(deps),
		newStepKindAndAudience(deps),
		newStepResponsePairing(deps),
		// StepEngineAppend (step 9) is fused into Chain.Write so the Step
		// interface can stay pure (no side-effects beyond envelope
		// mutation). Keeping engine append out of the Step slice also
		// lets unit tests run steps 0..8 in isolation with a stub Log.
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].ID() < steps[j].ID() })

	return &Chain{deps: deps, steps: steps}, nil
}

// Write runs the chain against env per proto-layer1 §2.0. The envelope
// is mutated in place during StepNormalize so the caller observes
// default-filled fields when the call returns.
func (c *Chain) Write(ctx context.Context, env *message.Envelope) (res khar.WriteResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("harness: panic: %v\n%s", r, debug.Stack())
		}
	}()
	if env == nil {
		return khar.WriteResult{}, errors.New("harness: nil envelope")
	}

	// Single linear loop: steps run in strict ascending-ID order.
	// StepDedupe is a normal step that short-circuits via Outcome.Deduped
	// when the incoming envelope is an idempotent retry.
	for _, s := range c.steps {
		out, err := s.Run(ctx, env)
		if err != nil {
			c.observeError(ctx, env, s.ID(), err)
			return khar.WriteResult{}, err
		}
		if out.Deduped {
			env.Seq = out.ExistingSeq
			env.IsTerminal = out.ExistingIsTerminal
			env.TSReceived = out.ExistingTSReceived
			c.deps.Logger.Debug("harness.write.dedupe",
				"step", int(s.ID()),
				"step_name", stepName(s.ID()),
				"channel_id", string(env.ChannelID),
				"message_id", string(env.ID),
				"correlation_id", string(env.CorrelationID),
				"request_id", requestctx.RequestID(ctx),
				"seq", out.ExistingSeq,
			)
			return khar.WriteResult{
				MessageID: env.ID,
				Seq:       out.ExistingSeq,
				Deduped:   true,
			}, nil
		}
		if !out.Continue() {
			c.observeReject(ctx, env, s.ID(), out.RejectReason, out.Detail)
			return rejectFromOutcome(out, env), nil
		}
		c.observePass(ctx, env, s.ID())
	}

	// StepEngineAppend — canonical sink. The chain has by this point
	// set env.IsTerminal (StepResponsePairing for responses) and computed
	// every other field; the store implementation is responsible for
	// outbox / sequence allocation per L1 §8.6 / L2 §1.4.1.
	env.TSReceived = c.deps.NowMs()
	appendRes, err := c.deps.Log.Append(ctx, env, c.deps.Fencing)
	if err != nil {
		// Map the typed AppendError to a closed-set reject when possible.
		var appErr *khlog.AppendError
		if errors.As(err, &appErr) {
			c.observeReject(ctx, env, khar.StepEngineAppend, appErr.Reason, appErr.Detail)
			return khar.WriteResult{
				MessageID:        appErr.PartialMessageID,
				RejectReason:     appErr.Reason,
				RejectDetail:     appErr.Detail,
				PartialMessageID: appErr.PartialMessageID,
			}, nil
		}
		c.observeError(ctx, env, khar.StepEngineAppend, err)
		return khar.WriteResult{}, fmt.Errorf("harness: engine append: %w", err)
	}

	c.observePass(ctx, env, khar.StepEngineAppend)
	return khar.WriteResult{
		MessageID: env.ID,
		Seq:       int64(appendRes.Seq),
		Deduped:   appendRes.Deduped,
	}, nil
}

func (c *Chain) observePass(ctx context.Context, env *message.Envelope, step khar.StepID) {
	c.deps.Logger.Debug("harness.write.step_ok",
		"step", int(step),
		"step_name", stepName(step),
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestctx.RequestID(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func (c *Chain) observeReject(ctx context.Context, env *message.Envelope, step khar.StepID, reason message.HarnessRejectReason, detail string) {
	if reason == "" {
		return
	}
	c.deps.Metrics.IncCounter("harness.reject", "reason", string(reason), "step", stepName(step))
	c.deps.Logger.Warn("harness.write.reject",
		"step", int(step),
		"step_name", stepName(step),
		"reason", string(reason),
		"detail", detail,
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestctx.RequestID(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func (c *Chain) observeError(ctx context.Context, env *message.Envelope, step khar.StepID, err error) {
	c.deps.Metrics.IncCounter("harness.error", "step", stepName(step))
	c.deps.Logger.Error("harness.write.error",
		"step", int(step),
		"step_name", stepName(step),
		"error", err.Error(),
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestctx.RequestID(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func stepName(step khar.StepID) string {
	switch step {
	case khar.StepCallerAuth:
		return "caller_auth"
	case khar.StepEnvelopeShape:
		return "envelope_shape"
	case khar.StepDedupe:
		return "dedupe"
	case khar.StepNormalize:
		return "normalize"
	case khar.StepSenderConsistent:
		return "sender_consistent"
	case khar.StepTypeRegistered:
		return "type_registered"
	case khar.StepAudienceResolve:
		return "audience_resolve"
	case khar.StepKindAndAudience:
		return "kind_and_audience"
	case khar.StepResponsePairing:
		return "response_pairing"
	case khar.StepEngineAppend:
		return "engine_append"
	default:
		return fmt.Sprintf("step_%d", step)
	}
}

// rejectFromOutcome packages a step outcome into a WriteResult.
func rejectFromOutcome(out khar.Outcome, env *message.Envelope) khar.WriteResult {
	r := khar.WriteResult{
		RejectReason:     out.RejectReason,
		RejectDetail:     out.Detail,
		PartialMessageID: message.ID(out.PartialMessageID),
	}
	if env != nil && env.ID != "" {
		r.MessageID = env.ID
	}
	return r
}
