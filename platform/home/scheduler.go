package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fireSink appends the already-durable timer message through the ordinary
// author-welded harness path. It does not revive, redeliver, or otherwise
// participate in actor lifecycle; the delivery adapter independently attempts
// the current actor endpoint when this committed message reaches its receiver.
type fireSink struct {
	minter    harness.AdmittedMinter
	authority storespec.CollaborationAuthority
	chID      channelpkg.ID
}

func (s fireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	admission, ok, err := s.authority.AdmitIdentity(ctx, author)
	if err != nil {
		return err
	}
	if !ok || !admission.Valid() {
		return schedule.FireRejected{Reason: "author_not_member", Detail: string(author)}
	}
	result, err := s.minter.MintAdmitted(admission, s.chID).Write(ctx, env)
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

var _ schedule.FireSink = fireSink{}
