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
// It assembles the 10 numbered steps (plus the out-of-band StepDedupe)
// declared in proto-layer1 §2.0 / kernel/harness.StepID and runs them in
// stable order, short-circuiting on the first reject.
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
		newStepSenderConsistent(deps),
		newStepNormalize(deps),
		newStepTypeRegistered(deps),
		newStepKindAndAudience(deps),
		newStepPayloadSchema(deps),
		newStepDocRefs(deps),
		newStepResponsePairing(deps),
		// StepEngineAppend (step 10) is fused into Chain.Write so the Step
		// interface can stay pure (no side-effects beyond envelope
		// mutation). Keeping engine append out of the Step slice also
		// lets unit tests run steps 0..9 in isolation with a stub Log.
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

	// Run StepCallerAuth(0), StepEnvelopeShape(1), StepSenderConsistent(2)
	// BEFORE the StepDedupe pre-check.
	//
	// Rationale: StepSenderConsistent forces envelope.sender.kind to the
	// registry truth, so the canonical hash of the stored row (which
	// reflects post-sender-consistent state) only matches a retry hash
	// after the same overwrite has been applied. The proto-layer1 §2.3
	// dedupe spec calls for sender-provided fields, but engineering
	// correctness requires sender-kind normalization first; the practical
	// behaviour is equivalent because StepSenderConsistent only forces
	// kind/name, never the wire id.
	for _, s := range c.steps {
		if s.ID() > khar.StepSenderConsistent {
			break
		}
		out, err := s.Run(ctx, env)
		if err != nil {
			return khar.WriteResult{}, err
		}
		if !out.Continue() {
			return rejectFromOutcome(out, env), nil
		}
	}

	// StepDedupe — id-conflict pre-check (post-sender-consistent,
	// pre-normalize).
	if dedupeRes, err := c.runDedupe(ctx, env); err != nil {
		return khar.WriteResult{}, err
	} else if dedupeRes != nil {
		return *dedupeRes, nil
	}

	// Remaining steps: StepNormalize → StepResponsePairing.
	for _, s := range c.steps {
		if s.ID() <= khar.StepSenderConsistent {
			continue
		}
		out, err := s.Run(ctx, env)
		if err != nil {
			return khar.WriteResult{}, err
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

// runDedupe is StepDedupe — universal id-conflict pre-check per
// proto-layer1 §2.3.
//
// Returns (nil, nil) when no existing row matches (caller proceeds to
// the rest of the chain). Returns a non-nil WriteResult when the chain
// should short-circuit (dedupe hit / id conflict).
func (c *Chain) runDedupe(ctx context.Context, env *message.Envelope) (*khar.WriteResult, error) {
	if env.ID == "" {
		// StepEnvelopeShape rejects empty id — leave StepDedupe a no-op
		// when the chain entered with no id (defensive).
		return nil, nil
	}
	existing, ok, err := c.deps.Log.FindByID(ctx, c.deps.ChannelID, env.ID)
	if err != nil {
		return nil, fmt.Errorf("harness: dedupe find: %w", err)
	}
	if !ok {
		return nil, nil
	}
	existingHash, err := message.CanonicalHash(existing)
	if err != nil {
		return nil, fmt.Errorf("harness: dedupe hash existing: %w", err)
	}
	incomingHash, err := message.CanonicalHash(*env)
	if err != nil {
		return nil, fmt.Errorf("harness: dedupe hash incoming: %w", err)
	}
	if existingHash == incomingHash {
		// Idempotent retry — surface the stored seq + Deduped flag.
		env.Seq = existing.Seq
		env.IsTerminal = existing.IsTerminal
		env.TSReceived = existing.TSReceived
		return &khar.WriteResult{
			MessageID: existing.ID,
			Seq:       existing.Seq,
			Deduped:   true,
		}, nil
	}
	return &khar.WriteResult{
		MessageID:        env.ID,
		RejectReason:     message.HarnessIDDuplicateConflict,
		RejectDetail:     "envelope.id reused with different content",
		PartialMessageID: env.ID,
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
