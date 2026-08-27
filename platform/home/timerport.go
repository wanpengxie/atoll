package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// timerPort is the composition root's half of the timer words: it mints ONE
// schedule handle per call, welded to the subject the door already
// authenticated, and does nothing else.
//
// Minting for another coordinate is a kernel privilege and this is where it
// belongs — the same seam the remote ingress uses
// (`MintAuthority(IdentityAuthorityFor(id))`), whose whole point is that the
// coordinate arrives authenticated by an endpoint rather than chosen by a
// caller. The handle still runs the Controller's live verdict on every call, so
// a subject that stopped being a member between the door's check and this line
// is refused by the authority, not by a stale snapshot.
//
// The system actor holds the MINTER, never a free-author method: keeping the
// mint here means the gate can route and authenticate without ever being able
// to name an author the substrate did not authenticate.
type timerPort struct {
	minter schedule.Minter
	// authority turns an actor id into a LIVE identity capability. It is a
	// function, not a Controller reference, so this port can hold nothing
	// broader than the one privilege it needs.
	authority func(actor.ActorID) capauth.Authority
}

func (p timerPort) handleFor(subject actor.ActorID) schedule.ScheduleHandle {
	return p.minter.MintAuthority(p.authority(subject))
}

// timerFault translates the scheduler's own verdicts into codes a caller can
// act on. Without this a malformed request (a reserved type, an impossible
// instant) and a full quota both reach the agent as internal_error — the code
// that says "nothing you can do", which is exactly wrong for both. The mapping
// lives HERE rather than in the door because the door deliberately knows
// nothing of runtime/schedule; this is the one place that already does.
func timerFault(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, schedule.ErrBadSchedule):
		return &sysactor.OperateError{Code: "bad_payload", Detail: err.Error()}
	case errors.Is(err, schedule.ErrScheduleQuota):
		return &sysactor.OperateError{Code: "limit_exceeded", Detail: err.Error()}
	default:
		return err
	}
}

func (p timerPort) Set(ctx context.Context, subject actor.ActorID, req sysactor.TimerSet) (sysactor.TimerHandle, error) {
	home := schedule.TimerHomeDurable
	if req.Home == string(schedule.TimerHomeMemory) {
		home = schedule.TimerHomeMemory
	}
	id, err := p.handleFor(subject).Schedule(ctx, schedule.ScheduleReq{
		Home:    home,
		FireAt:  req.FireAt,
		Type:    req.Type,
		Payload: req.Payload,
	})
	if err != nil {
		return sysactor.TimerHandle{}, timerFault(err)
	}
	return sysactor.TimerHandle{ID: string(id), FireAt: req.FireAt}, nil
}

// Cancel reports existed=false for an id that already fired, never existed, or
// belongs to someone else. The engine's Cancel is ack-less (a cancel racing an
// in-flight fire may still see it ring), so "did it exist" is answered by a
// follow-up read of the pending set rather than invented here.
func (p timerPort) Cancel(ctx context.Context, subject actor.ActorID, id string) (bool, error) {
	handle := p.handleFor(subject)
	before, err := handle.List(ctx)
	if err != nil {
		return false, timerFault(err)
	}
	found := false
	for _, t := range before {
		if string(t.ID) == id {
			found = true
			break
		}
	}
	if !found {
		return false, nil
	}
	if err := handle.Cancel(ctx, schedule.TimerID(id)); err != nil {
		return false, timerFault(err)
	}
	return true, nil
}

func (p timerPort) List(ctx context.Context, subject actor.ActorID) ([]sysactor.TimerInfo, error) {
	timers, err := p.handleFor(subject).List(ctx)
	if err != nil {
		return nil, timerFault(err)
	}
	out := make([]sysactor.TimerInfo, 0, len(timers))
	for _, t := range timers {
		out = append(out, sysactor.TimerInfo{
			ID: string(t.ID), Home: string(t.Home), FireAt: t.FireAt,
			Type: t.Type, CreatedAt: t.CreatedAt,
		})
	}
	return out, nil
}
