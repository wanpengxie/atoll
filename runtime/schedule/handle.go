package schedule

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/runtime/storespec"
)

// boundScheduleHandle is a ScheduleHandle welded to one author (the caps-
// injected instance an actor cell holds). The author is a struct field, not
// a wire/request field — structurally there is nowhere for a caller to
// self-report a different one (mirrors accessdoor.boundHandle).
type boundScheduleHandle struct {
	engine *Engine
	author storespec.AuthorStamp
	auth   storespec.ActorAuthority
}

func (h boundScheduleHandle) authorize(ctx context.Context) error {
	verdict, err := h.auth.CheckAuthor(ctx, h.author)
	if err != nil {
		return err
	}
	if verdict != storespec.AuthorOK {
		return ErrAuthorInactive
	}
	return nil
}

func (h boundScheduleHandle) Schedule(ctx context.Context, req ScheduleReq) (TimerID, error) {
	if err := h.authorize(ctx); err != nil {
		return "", err
	}
	if req.Bind == BindIdentity {
		world, ok, err := h.auth.WorldOf(ctx, h.author.ID)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", ErrAuthorInactive
		}
		if world == storespec.WorldRun {
			return "", ErrDurableScheduleForbidden
		}
	}
	return h.engine.schedule(ctx, h.author.ID, req)
}

func (h boundScheduleHandle) Cancel(ctx context.Context, id TimerID) error {
	if err := h.authorize(ctx); err != nil {
		return err
	}
	return h.engine.cancel(ctx, h.author.ID, id)
}

func (h boundScheduleHandle) Ack(ctx context.Context, id TimerID) error {
	if err := h.authorize(ctx); err != nil {
		return err
	}
	_, err := h.engine.deps.Store.AckOwned(ctx, id, h.author.ID)
	return err
}

// minter is Minter's sole implementation, sealed inside the package — New
// hands out the interface only, never the concrete type (mirrors
// harness.minter / accessdoor.minter).
type minter struct {
	engine    *Engine
	authority storespec.ActorAuthority
}

// Mint welds author onto the engine and returns a handle. Deterministic and
// cheap (no per-handle state beyond the welded author), so admission points
// may Mint per-caller freely.
func (m *minter) Mint(author storespec.AuthorStamp) ScheduleHandle {
	if author.ID == "" || author.BirthVersion <= 0 {
		return rejectedScheduleHandle{err: errors.New("schedule: invalid author stamp")}
	}
	return boundScheduleHandle{engine: m.engine, author: author, auth: m.authority}
}

type rejectedScheduleHandle struct{ err error }

func (h rejectedScheduleHandle) Schedule(context.Context, ScheduleReq) (TimerID, error) {
	return "", h.err
}
func (h rejectedScheduleHandle) Cancel(context.Context, TimerID) error { return h.err }
func (h rejectedScheduleHandle) Ack(context.Context, TimerID) error    { return h.err }
