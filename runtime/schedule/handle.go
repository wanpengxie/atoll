package schedule

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// boundScheduleHandle is a ScheduleHandle welded to one author (the caps-
// injected instance an actor cell holds). The author is a struct field, not
// a wire/request field — structurally there is nowhere for a caller to
// self-report a different one (mirrors accessdoor.boundHandle).
type boundScheduleHandle struct {
	engine    *Engine
	author    actor.ActorID
	authority capauth.Authority
	admitted  bool
}

func (h boundScheduleHandle) authorize(ctx context.Context) error {
	if h.admitted {
		return nil
	}
	if h.authority != nil {
		return h.authority.Admit()
	}
	return errors.New("schedule: incomplete author authority")
}

func (h boundScheduleHandle) Schedule(ctx context.Context, req ScheduleReq) (TimerID, error) {
	if err := h.authorize(ctx); err != nil {
		return "", err
	}
	return h.engine.schedule(ctx, h.author, req)
}

func (h boundScheduleHandle) Cancel(ctx context.Context, id TimerID) error {
	if err := h.authorize(ctx); err != nil {
		return err
	}
	return h.engine.cancel(ctx, h.author, id)
}

func (h boundScheduleHandle) Ack(ctx context.Context, id TimerID) error {
	if err := h.authorize(ctx); err != nil {
		return err
	}
	_, err := h.engine.deps.Store.AckOwned(ctx, id, h.author)
	return err
}

// minter is Minter's sole implementation, sealed inside the package — New
// hands out the interface only, never the concrete type (mirrors
// harness.minter / accessdoor.minter).
type minter struct {
	engine *Engine
}

func (m *minter) MintAdmitted(
	admission storespec.IdentityAdmission,
) ScheduleHandle {
	if !admission.Valid() {
		return rejectedScheduleHandle{err: errors.New("schedule: invalid identity admission")}
	}
	return boundScheduleHandle{
		engine: m.engine, author: admission.ID, admitted: true,
	}
}

func (m *minter) MintAuthority(authority capauth.Authority) ScheduleHandle {
	if authority == nil || authority.ActorID() == "" {
		return rejectedScheduleHandle{err: errors.New("schedule: invalid authority")}
	}
	return boundScheduleHandle{
		engine:    m.engine,
		author:    authority.ActorID(),
		authority: authority,
	}
}

type rejectedScheduleHandle struct{ err error }

func (h rejectedScheduleHandle) Schedule(context.Context, ScheduleReq) (TimerID, error) {
	return "", h.err
}
func (h rejectedScheduleHandle) Cancel(context.Context, TimerID) error { return h.err }
func (h rejectedScheduleHandle) Ack(context.Context, TimerID) error    { return h.err }
