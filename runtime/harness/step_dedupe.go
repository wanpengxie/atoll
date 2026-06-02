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
//     and return it on Outcome.CanonicalHash (the envelope itself no longer
//     carries the field — kernel purified it) so the chain captures it and
//     hands it to MessageLog.Append for StepEngineAppend to persist.
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

func newStepDedupe(d Deps) step { return &stepDedupe{deps: d} }

func (s *stepDedupe) ID() stepID { return StepDedupe }

func (s *stepDedupe) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	if env.ID == "" {
		// StepEnvelopeShape already rejects empty id — defensive no-op.
		return outcome{}, nil
	}

	// Hash the sender-provided envelope. Stash on env so the engine
	// append step persists it; on retries the lookup below replays the
	// same hash because StepNormalize / StepSenderConsistent run AFTER
	// this step.
	incomingHash, err := message.CanonicalHash(*env)
	if err != nil {
		return outcome{}, fmt.Errorf("harness: dedupe hash incoming: %w", err)
	}

	storedHash, found, err := s.deps.Log.LookupCanonicalHash(ctx, s.deps.ChannelID, env.ID)
	if err != nil {
		return outcome{}, fmt.Errorf("harness: dedupe lookup: %w", err)
	}
	if !found {
		return outcome{CanonicalHash: incomingHash}, nil
	}

	if storedHash == incomingHash {
		// Idempotent retry — fetch the row for its seq / is_terminal /
		// ts_received so the chain runner can surface the original write
		// result.
		existing, ok, err := s.deps.Log.FindByID(ctx, s.deps.ChannelID, env.ID)
		if err != nil {
			return outcome{}, fmt.Errorf("harness: dedupe find existing: %w", err)
		}
		if !ok {
			// Race window: hash existed but row vanished. Treat as fresh.
			return outcome{CanonicalHash: incomingHash}, nil
		}
		return outcome{
			Deduped:            true,
			ExistingSeq:        existing.Seq,
			ExistingTSReceived: existing.Envelope.TSReceived,
		}, nil
	}
	return outcome{
		RejectReason:     HarnessIDDuplicateConflict,
		Detail:           "envelope.id reused with different content",
		PartialMessageID: env.ID,
	}, nil
}
