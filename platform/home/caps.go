package home

import (
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
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
func (h *Home) buildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	rt := h.channel.Cells()
	return actorcaps.Caps{
		Pen:      link.NewLivePen(h.minter.Mint(id, kind, h.channelID), inc, rt),
		Access:   link.NewLiveResourceAccess(h.cs.Access.Mint(id), inc, rt),
		State:    link.NewLiveAccess(h.cs.Access.MintState(id), inc, rt),
		Schedule: link.NewLiveSchedule(h.schedMinter.Mint(id), inc, rt),
		// The child assembler is buildChildCaps, NOT buildCaps: every fork
		// descendant is an incarnation-level citizen (spec §4.1), so its private
		// state must be per-incarnation memory, not this durable MintState arm. Any
		// actor's fork children — top-level or itself a child — take that path.
		Spawn: newSpawnHandle(inc, rt, h.builder, h.buildChildCaps, h.hooks()),
	}
}

// buildChildCaps is the FORK-CHILD caps assembler (spec §4.1 户籍轴): identical to
// buildCaps except the State arm is a per-incarnation in-memory backend instead of
// the durable actor_state MintState. substrate-本质: a fork child holds no durable
// name分, so it holds no durable state — a fresh empty memStateStore is minted per
// Fork and welded to THIS incarnation, so it evaporates with the incarnation and a
// same-named reincarnation inherits nothing (EH2 root-cure, spec P1-2). The other
// four arms (Pen / Access / Schedule / Spawn) are byte-for-byte buildCaps's — a
// child writes truth, reads/writes父授 workspace resources, arms incarnation timers,
// and forks its own (memory-state) children exactly like any actor.
func (h *Home) buildChildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	caps := h.buildCaps(id, kind, inc)
	caps.State = link.NewLiveAccess(accessdoor.NewMemoryStateHandle(id), inc, h.channel.Cells())
	return caps
}
