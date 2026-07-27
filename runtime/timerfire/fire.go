package timerfire

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ErrInvalidInput is the assembly fail-fast: a sink missing an organ would
// fail at the first fire instead of at construction.
var ErrInvalidInput = errors.New("timerfire: invalid construction input")

// sink appends the already-durable timer message through the ordinary
// author-welded harness path. It does not revive, redeliver, or otherwise
// participate in actor lifecycle; the delivery adapter independently attempts
// the current actor endpoint when this committed message reaches its receiver.
//
// The admission here is the exit guard of §7.4: a dead author's durable timer
// is refused (author_not_member) and the row annihilated by the engine — the
// reason the schedule arm needs no entry-side classification gate.
type sink struct {
	minter    harness.AdmittedMinter
	authority storespec.CollaborationAuthority
}

// New builds the fire sink from the two organs it bridges. The authority is
// the value ledger's own membership face — no Platform forwarding layer.
func New(
	authority storespec.CollaborationAuthority,
	minter harness.AdmittedMinter,
) (schedule.FireSink, error) {
	if authority == nil || minter == nil {
		return nil, ErrInvalidInput
	}
	return sink{minter: minter, authority: authority}, nil
}

func (s sink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	admission, ok, err := s.authority.AdmitIdentity(ctx, author)
	if err != nil {
		return err
	}
	if !ok || !admission.Valid() {
		return schedule.FireRejected{Reason: "author_not_member", Detail: string(author)}
	}
	result, err := s.minter.MintAdmitted(admission).Write(ctx, env)
	if err != nil {
		return err
	}
	if result.Accepted() {
		return nil
	}
	if result.RejectReason == harness.HarnessIDDuplicateConflict && result.MessageID == env.ID {
		return schedule.ErrDuplicateFire
	}
	return schedule.FireRejected{
		Reason: string(result.RejectReason), Detail: result.RejectDetail,
	}
}
