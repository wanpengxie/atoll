package home

import (
	"context"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// buildCaps assembles the caps bundle — the five-capability bundle welded to (id,
// inc). Handing out the handle and wrapping the live membrane happen in the same
// step (invariant: no bare handle escapes). It is the SINGLE
// caps assembler, shared by activation and by fork (spawnHandle.Fork
// holds this method value as its capsAssembler and re-runs it against each child's
// incarnation) — so a fork child is born with the IDENTICAL membrane set as a
// top-level admission (recursive assembly), never a raw un-membraned closure.
//
// Wired this period: Pen (livePen over the harness pen), Access + State
// (liveAccess over the channel-scoped Mint and actor-scoped MintState handles —
// cs.Access is already assembled by storeopen, drawn directly), Spawn (the
// by-incarnation fork/despawn handle; builder may be nil → Fork fail-fasts).
//
// Schedule is welded over the schedule engine's per-author ScheduleHandle
// (h.schedMinter, assembled by OpenScheduler at Open step 10) inside the same
// liveSchedule membrane the other caps wear — self-targeted timers gated on this
// incarnation still being live. schedMinter is always set before any participant
// admission (the system cell does not pass through buildCaps).
func (h *Home) buildCaps(id actor.ActorID, kind actor.Kind, birthVersion int64, inc actorrt.Incarnation) actorcaps.Caps {
	rt := h.channel.Cells()
	state, err := h.stateHandles.Resolve(context.Background(), id)
	if err != nil {
		panic(err)
	}
	return actorcaps.Caps{
		Pen:      link.NewLivePen(h.minter.Mint(id, kind, h.channelID, birthVersion), inc, rt),
		Access:   link.NewLiveResourceAccess(h.cs.Access.Mint(storespec.AuthorStamp{ID: id, BirthVersion: birthVersion}), inc, rt),
		State:    link.NewLiveAccess(state, inc, rt),
		Schedule: link.NewLiveSchedule(h.schedMinter.Mint(storespec.AuthorStamp{ID: id, BirthVersion: birthVersion}), inc, rt),
		// Lifecycle admission recurses through this same assembler. State resolution
		// is world-aware: declared identities receive the durable var layer, forked
		// identities the Home-session run layer, both behind the same handle shape.
		Lifecycle: newSpawnHandle(h, inc, birthVersion, rt),
	}
}
