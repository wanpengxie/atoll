package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
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

	steps := []khar.Step{
		newStepCallerAuth(deps),
		newStepEnvelopeShape(deps),
		newStepDedupe(deps),
		newStepNormalize(deps),
		newStepSenderConsistent(deps),
		newStepTypeRegistered(deps),
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
func (c *Chain) Write(ctx context.Context, env *message.Envelope) (khar.WriteResult, error) {
	if env == nil {
		return khar.WriteResult{}, errors.New("harness: nil envelope")
	}

	// Single linear loop: steps run in strict ascending-ID order.
	// StepDedupe is a normal step that short-circuits via Outcome.Deduped
	// when the incoming envelope is an idempotent retry.
	for _, s := range c.steps {
		out, err := s.Run(ctx, env)
		if err != nil {
			return khar.WriteResult{}, err
		}
		if out.Deduped {
			env.Seq = out.ExistingSeq
			env.IsTerminal = out.ExistingIsTerminal
			env.TSReceived = out.ExistingTSReceived
			return khar.WriteResult{
				MessageID: env.ID,
				Seq:       out.ExistingSeq,
				Deduped:   true,
			}, nil
		}
		if !out.Continue() {
			return rejectFromOutcome(out, env), nil
		}
	}

	// StepEngineAppend — canonical sink. The chain has by this point
	// set env.IsTerminal (StepResponsePairing for responses) and computed
	// every other field; the store implementation is responsible for
	// outbox / sequence allocation per L1 §8.6 / L2 §1.4.1.
	env.TSReceived = c.deps.NowMs()
	res, err := c.deps.Log.Append(ctx, env, c.deps.Fencing)
	if err != nil {
		// Map the typed AppendError to a closed-set reject when possible.
		var appErr *khlog.AppendError
		if errors.As(err, &appErr) {
			return khar.WriteResult{
				MessageID:        appErr.PartialMessageID,
				RejectReason:     appErr.Reason,
				RejectDetail:     appErr.Detail,
				PartialMessageID: appErr.PartialMessageID,
			}, nil
		}
		return khar.WriteResult{}, fmt.Errorf("harness: engine append: %w", err)
	}

	return khar.WriteResult{
		MessageID: env.ID,
		Seq:       int64(res.Seq),
		Deduped:   res.Deduped,
	}, nil
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
