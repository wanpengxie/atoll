package harness

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepDedupe implements proto-layer1 §2.3 Step 3 — Id Dedupe.
//
// The step runs after StepEnvelopeShape and BEFORE StepNormalize so the
// canonical_hash sees the sender-provided envelope (the same shape a
// retrying sender resubmits). Normalize default-fills (visibility=public
// / not_before=ts / expires_at=<type default>) MUST NOT enter the hash
// input domain — those values are runtime-derived and would diverge
// between Step 3's first-time path (filled by normalize) and the retry's
// pre-normalize input.
//
// Mechanics:
//
//   - Compute the canonical hash on the incoming (pre-normalize) envelope
//     and stash it on env.CanonicalHash so StepEngineAppend can persist
//     it alongside the row.
//   - On an id collision, fetch the stored hash via
//     MessageLog.LookupCanonicalHash and compare.
//
// Outcome:
//
//   - new id (no row found): Continue (hash stashed).
//   - existing id, hashes match: Outcome.Deduped=true plus the existing
//     row's seq / is_terminal / ts_received so the chain runner can
//     short-circuit and surface the original write result.
//   - existing id, hashes differ: REJECT harness_id_duplicate_conflict.
type stepDedupe struct {
	deps Deps
}

func newStepDedupe(d Deps) Step { return &stepDedupe{deps: d} }

func (s *stepDedupe) ID() StepID { return StepDedupe }

func (s *stepDedupe) Run(ctx context.Context, env *message.Envelope) (Outcome, error) {
	if env.ID == "" {
		// StepEnvelopeShape already rejects empty id — defensive no-op.
		return Outcome{}, nil
	}

	// Hash the sender-provided envelope. Stash on env so the engine
	// append step persists it; on retries the lookup below replays the
	// same hash because StepNormalize / StepSenderConsistent run AFTER
	// this step.
	incomingHash, err := message.CanonicalHash(*env)
	if err != nil {
		return Outcome{}, fmt.Errorf("harness: dedupe hash incoming: %w", err)
	}
	env.CanonicalHash = incomingHash

	storedHash, found, err := s.deps.Log.LookupCanonicalHash(ctx, s.deps.ChannelID, env.ID)
	if err != nil {
		return Outcome{}, fmt.Errorf("harness: dedupe lookup: %w", err)
	}
	if !found {
		return Outcome{}, nil
	}

	if storedHash == incomingHash {
		// Idempotent retry — fetch the row for its seq / is_terminal /
		// ts_received so the chain runner can surface the original write
		// result.
		existing, ok, err := s.deps.Log.FindByID(ctx, s.deps.ChannelID, env.ID)
		if err != nil {
			return Outcome{}, fmt.Errorf("harness: dedupe find existing: %w", err)
		}
		if !ok {
			// Race window: hash existed but row vanished. Treat as fresh.
			return Outcome{}, nil
		}
		return Outcome{
			Deduped:            true,
			ExistingSeq:        existing.Seq,
			ExistingIsTerminal: existing.IsTerminal,
			ExistingTSReceived: existing.TSReceived,
		}, nil
	}
	return Outcome{
		RejectReason:     HarnessIDDuplicateConflict,
		Detail:           "envelope.id reused with different content",
		PartialMessageID: env.ID,
	}, nil
}
