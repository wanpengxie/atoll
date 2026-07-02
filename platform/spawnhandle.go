package platform

import (
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// ErrNoBuilder is returned by spawnHandle.Fork when no builder is wired into the
// home yet — the injection point contract is in place but its implementation (the
// domain's class→factory table) has not been injected. A fork with no builder is
// a structural refusal, never a silent no-op: the parent gets a clear error
// rather than a phantom child id.
var ErrNoBuilder = errors.New("platform: no builder wired — fork unavailable")

// ErrClassNotFound is returned by spawnHandle.Fork when the builder has no
// factory registered under spec.Class — a structural reject, not silent.
var ErrClassNotFound = errors.New("platform: fork class not found in builder")

// CapsFactoryBuilder is the PLATFORM-layer factory table for the two mint
// triggers that resolve a factory AFTER the original admission closure is gone —
// fork (by Class) and activation (by id). It resolves either key to the actor's
// caps-taking factory `func(actorcaps.Caps) actorrt.Actor` — the SAME factory
// shape a top-level admission (Home.Spawn) consumes.
//
// It is deliberately NOT actorrt.Builder (whose Lookup/LookupByClass hand back
// `func(Incarnation) Actor`): the caps seam — the livePen/liveAccess/liveState/
// liveSchedule membranes — must be woven at the platform assembly seam
// (buildCaps), where the live-membrane constructors live, and NEVER inside actorrt
// (which must not import runtime/harness · runtime/link). So the table hands back
// the RAW factory and the platform wraps it in the shared caps assembler per mint;
// the child/revived actor is therefore born with the identical membrane set as a
// top-level admission. Welding caps at the domain-populated table instead would
// require the domain to reach the platform membrane constructors — exactly the
// leak the archtest wall forbids. This is the "platform承接 the raw factory"
// posture the wiring spec pins for BOTH actorrt.Builder entries: the runtime
// contract is not bent to carry caps; the platform seam owns the weld.
type CapsFactoryBuilder interface {
	// LookupByClass resolves a caller-declared, opaque implementation-selection key
	// to its caps-taking factory — fork's entry (ForkSpec.Class). Kind is NOT
	// re-answered here (it is caller-held on ForkSpec.Kind).
	LookupByClass(class string) (factory func(actorcaps.Caps) actorrt.Actor, ok bool)
	// Lookup resolves a durable member id to its caps-taking factory — activation's
	// entry (the reconcile ring's eager revival + the schedule engine's identity-
	// timer Reviver both hold only an id, since the original admission factory died
	// with the previous incarnation). Kind is NOT re-answered here either (it is
	// caller-held: DesiredMember.Kind for reconcile, the registry row for the
	// Reviver). The domain implementation maps id→class→factory over the same
	// underlying table LookupByClass reads.
	Lookup(id actor.ActorID) (factory func(actorcaps.Caps) actorrt.Actor, ok bool)
}

// capsAssembler is the platform's single caps seam assembler (Home.buildCaps):
// it welds the whole five-capability bundle to (id, kind, inc) — minting the
// handle and wrapping the live membranes happen in the same step. Injected into
// spawnHandle so a fork child is assembled through the EXACT same seam as a
// top-level admission (recursive assembly).
type capsAssembler func(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps

// spawnHandle is the platform-side concrete implementation of
// actorrt.SpawnHandle (fork.go declares the vocabulary; the concrete impl must
// live here because it welds a parent Incarnation + drives *actorrt.Runtime, AND
// it reaches the platform caps assembler / membrane constructors — actorrt itself
// must not import runtime/harness · runtime/link).
//
// It is welded to ONE parent incarnation at construction (inc): every Fork mints
// a child owned by THAT incarnation, and every Despawn is authority-checked
// against it inside the substrate (childID's owner must equal inc). The child's
// truth-handle never leaves substrate — Fork returns only the child's NAME.
type spawnHandle struct {
	inc       actorrt.Incarnation
	rt        *actorrt.Runtime
	builder   CapsFactoryBuilder // nil until the class→factory table is injected.
	assemble  capsAssembler      // the shared caps seam assembler (Home.buildCaps); never nil.
	placement actorrt.Placement  // single-home identity this period (SinglePlacement).
}

// newSpawnHandle welds a SpawnHandle to parent incarnation inc. rt/assemble/
// placement are always present; builder may be nil (see ErrNoBuilder).
func newSpawnHandle(inc actorrt.Incarnation, rt *actorrt.Runtime, builder CapsFactoryBuilder, assemble capsAssembler, placement actorrt.Placement) actorrt.SpawnHandle {
	return spawnHandle{inc: inc, rt: rt, builder: builder, assemble: assemble, placement: placement}
}

// Fork mints a child owned by this handle's parent incarnation.
//
// The child's caps seam is woven at the SAME platform assembler as a top-level
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
	// childID = parentID + "/" + NameHint (namespace derivation — no substrate id
	// allocator).
	childID := h.inc.ID() + "/" + actor.ActorID(spec.NameHint)
	// Placement answers the physical host (single-home identity this period);
	// call it so multi-home can additively swap the implementation later without
	// touching this call site.
	if _, err := h.placement.Place(childID, spec.Kind); err != nil {
		return "", err
	}
	// Weld the child's caps seam at the platform assembler (same seam as
	// admission): the build closure runs INSIDE rt.Fork (pre-go-live,
	// IsLive(childInc)==false), so a construction-time write is fenced exactly as
	// for a top-level Spawn.
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
// the handle itself never leaves the substrate.
func (h spawnHandle) Despawn(childID actor.ActorID) error {
	return h.rt.DespawnChild(h.inc, childID)
}

// singleHostRef is the fixed host of the single-home deployment. Opaque to
// actorrt (it never interprets a HostRef); the value only needs to be stable and
// non-empty. Multi-home selection is the split-brain wall — deferred.
const singleHostRef actorrt.HostRef = "local"

// SinglePlacement is the single fixed-home Placement implementation — ships
// shape only. Every id/kind resolves to the same host — a placement never
// touches membership, and multi-home selection is deferred until a second home
// exists.
type SinglePlacement struct{}

// Place implements actorrt.Placement — always the single home, never an error.
func (SinglePlacement) Place(actor.ActorID, actor.Kind) (actorrt.HostRef, error) {
	return singleHostRef, nil
}
