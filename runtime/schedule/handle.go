package schedule

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
)

// boundScheduleHandle is a ScheduleHandle welded to one author (the caps-
// injected instance an actor cell holds). The author is a struct field, not
// a wire/request field — structurally there is nowhere for a caller to
// self-report a different one (mirrors accessdoor.boundHandle).
type boundScheduleHandle struct {
	engine *Engine
	author actor.ActorID
}

func (h boundScheduleHandle) Schedule(ctx context.Context, req ScheduleReq) (TimerID, error) {
	return h.engine.schedule(ctx, h.author, req)
}

func (h boundScheduleHandle) Cancel(ctx context.Context, id TimerID) error {
	return h.engine.cancel(ctx, h.author, id)
}

// minter is Minter's sole implementation, sealed inside the package — New
// hands out the interface only, never the concrete type (mirrors
// harness.minter / accessdoor.minter).
type minter struct{ engine *Engine }

// Mint welds author onto the engine and returns a handle. Deterministic and
// cheap (no per-handle state beyond the welded author), so admission points
// may Mint per-caller freely.
func (m *minter) Mint(author actor.ActorID) ScheduleHandle {
	return boundScheduleHandle{engine: m.engine, author: author}
}
