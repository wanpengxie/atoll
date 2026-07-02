package platform

import (
	"errors"

	"github.com/wanpengxie/ActOS/lib/actorcaps"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// ErrNoBuilder is returned by spawnHandle.Fork when no builder is wired into the
// home yet — the注入点契约 is in place but its implementation (the domain's
// class→factory table) has not been injected. A fork with no builder is a
// structural refusal, never a silent no-op: the parent gets a clear error rather
// than a phantom child id. The builder wiring lands with the factory-migration棒
// (线A/线B); until then this is the honest answer.
var ErrNoBuilder = errors.New("platform: no builder wired — fork unavailable")

// ErrClassNotFound is returned by spawnHandle.Fork when the builder has no
// factory registered under spec.Class — a structural reject (not silent),
// mirroring §3.4's `if !ok { return "", ErrClassNotFound }`.
var ErrClassNotFound = errors.New("platform: fork class not found in builder")

// CapsFactoryBuilder is the PLATFORM-layer class→factory table for fork (and,
// later, activation). It resolves an opaque implementation-selection Class to the
// actor's caps-taking factory `func(actorcaps.Caps) actorrt.Actor` — the SAME
// factory shape a top-level admission (Home.Spawn) consumes.
//
// It is deliberately NOT actorrt.Builder (whose LookupByClass hands back
// `func(Incarnation) Actor`): the caps缝 — the livePen/liveAccess/liveState
// membranes — must be woven at the platform assembly seam (buildCaps), where the
// live-membrane constructors live, and NEVER inside actorrt (which must not import
// runtime/harness · runtime/link). So the table hands back the RAW factory and
// spawnHandle wraps it in the shared caps assembler per fork; a child is therefore
// born with the identical membrane set as a top-level admission. Welding caps at
// the domain-populated table instead would require the domain to reach the
// platform membrane constructors — exactly the leak the archtest wall forbids.
type CapsFactoryBuilder interface {
	// LookupByClass resolves a caller-declared, opaque implementation-selection key
	// to its caps-taking factory — fork's entry (ForkSpec.Class). Kind is NOT
	// re-answered here (it is caller-held on ForkSpec.Kind, §9.1).
	LookupByClass(class string) (factory func(actorcaps.Caps) actorrt.Actor, ok bool)
}

// capsAssembler is the platform's single caps缝 assembler (Home.buildCaps): it
// welds the whole five-capability bundle to (id, kind, inc) — 发 handle 与 live 膜
// wrap 同一步. Injected into spawnHandle so a fork child is assembled through the
// EXACT same seam as a top-level admission (recursive assembly, §3.A).
type capsAssembler func(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps

// spawnHandle is the platform-side concrete implementation of
// actorrt.SpawnHandle (fork.go declares the vocabulary; the concrete impl must
// live here because it welds a parent Incarnation + drives *actorrt.Runtime, AND
// it reaches the platform caps assembler / membrane constructors — actorrt itself
// must not import runtime/harness · runtime/link — §10.5 / fork.go:117).
//
// It is welded to ONE parent incarnation at construction (inc): every Fork mints
// a child owned by THAT incarnation, and every Despawn is authority-checked
// against it inside the substrate (childID's owner must equal inc). The child's
// truth-handle never leaves substrate — Fork returns only the child's NAME.
type spawnHandle struct {
	inc       actorrt.Incarnation
	rt        *actorrt.Runtime
	builder   CapsFactoryBuilder // nil until the class→factory table is injected (线A/线B).
	assemble  capsAssembler      // the shared caps缝 assembler (Home.buildCaps); never nil.
	placement actorrt.Placement  // single-home identity this period (SinglePlacement).
}

// newSpawnHandle welds a SpawnHandle to parent incarnation inc. rt/assemble/
// placement are always present; builder may be nil (see ErrNoBuilder).
func newSpawnHandle(inc actorrt.Incarnation, rt *actorrt.Runtime, builder CapsFactoryBuilder, assemble capsAssembler, placement actorrt.Placement) actorrt.SpawnHandle {
	return spawnHandle{inc: inc, rt: rt, builder: builder, assemble: assemble, placement: placement}
}

// Fork mints a child owned by this handle's parent incarnation (§3.1/§10.2).
//
// The child's caps缝 is woven at the SAME platform assembler as a top-level
// admission (h.assemble = Home.buildCaps): Fork resolves the raw caps-taking
// factory from the builder table, then wraps it in a build closure that runs the
// assembler against the CHILD's incarnation, so the child is born with its own
// livePen/liveAccess/liveState membranes welded to itself — recursive assembly,
// not a raw closure handed straight through. This handle derives the child name,
// asks placement, and drives the substrate fork primitive.
func (h spawnHandle) Fork(spec actorrt.ForkSpec) (actor.ActorID, error) {
	if h.builder == nil {
		return "", ErrNoBuilder
	}
	factory, ok := h.builder.LookupByClass(spec.Class)
	if !ok {
		return "", ErrClassNotFound
	}
	// childID = parentID + "/" + NameHint (namespace derivation, §10.2 — no
	// substrate id allocator).
	childID := h.inc.ID() + "/" + actor.ActorID(spec.NameHint)
	// Placement answers the physical host (single-home identity this period);
	// call it so multi-home can additively swap the implementation later without
	// touching this call site (§3.5 Placement shape).
	if _, err := h.placement.Place(childID, spec.Kind); err != nil {
		return "", err
	}
	// Weld the child's caps缝 at the platform assembler (same seam as admission):
	// the build closure runs INSIDE rt.Fork (pre-go-live, IsLive(childInc)==false),
	// so a construction-time write is fenced exactly as for a top-level Spawn.
	build := func(childInc actorrt.Incarnation) actorrt.Actor {
		return factory(h.assemble(childID, spec.Kind, childInc))
	}
	if _, err := h.rt.Fork(h.inc, childID, build); err != nil {
		return "", err
	}
	return childID, nil
}

// Despawn requests termination of one of this handle's own children by name.
// The by-id authority-check (childID's owner incarnation == h.inc) is performed
// inside the substrate (DespawnChild), so the caller never holds a bare kill —
// §10.5 句柄不出.
func (h spawnHandle) Despawn(childID actor.ActorID) error {
	return h.rt.DespawnChild(h.inc, childID)
}

// singleHostRef is the fixed host of the single-home deployment. Opaque to
// actorrt (it never interprets a HostRef); the value only needs to be stable and
// non-empty. Multi-home selection is the split-brain wall (§5.2) — deferred.
const singleHostRef actorrt.HostRef = "local"

// SinglePlacement is the single fixed-home Placement implementation (期2 §3.5:
// ships shape only). Every id/kind resolves to the same host — a placement never
// touches membership, and multi-home selection is deferred until a second home
// exists.
type SinglePlacement struct{}

// Place implements actorrt.Placement — always the single home, never an error.
func (SinglePlacement) Place(actor.ActorID, actor.Kind) (actorrt.HostRef, error) {
	return singleHostRef, nil
}
