package platform

import (
	"errors"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// ErrNoBuilder is returned by spawnHandle.Fork when no Builder is wired into the
// home yet — the注入点契约 (actorrt.Builder) is in place but its implementation
// (the domain's class→factory table) has not been injected. A fork with no
// builder is a structural refusal, never a silent no-op: the parent gets a clear
// error rather than a phantom child id. The builder wiring lands with the
// factory-migration棒 (线A/线B); until then this is the honest answer.
var ErrNoBuilder = errors.New("platform: no builder wired — fork unavailable")

// ErrClassNotFound is returned by spawnHandle.Fork when the Builder has no
// implementation registered under spec.Class — a structural reject (not silent),
// mirroring §3.4's `if !ok { return "", ErrClassNotFound }`.
var ErrClassNotFound = errors.New("platform: fork class not found in builder")

// spawnHandle is the platform-side concrete implementation of
// actorrt.SpawnHandle (fork.go declares the vocabulary; the concrete impl must
// live here because it welds a parent Incarnation + drives *actorrt.Runtime,
// and actorrt itself must not import runtime/harness — §10.5 / fork.go:117).
//
// It is welded to ONE parent incarnation at construction (inc): every Fork mints
// a child owned by THAT incarnation, and every Despawn is authority-checked
// against it inside the substrate (childID's owner must equal inc). The child's
// truth-handle never leaves substrate — Fork returns only the child's NAME.
type spawnHandle struct {
	inc       actorrt.Incarnation
	rt        *actorrt.Runtime
	builder   actorrt.Builder   // nil until the class→factory table is injected (线A/线B).
	placement actorrt.Placement // single-home identity this period (SinglePlacement).
}

// newSpawnHandle welds a SpawnHandle to parent incarnation inc. rt/placement are
// always present; builder may be nil (see ErrNoBuilder).
func newSpawnHandle(inc actorrt.Incarnation, rt *actorrt.Runtime, builder actorrt.Builder, placement actorrt.Placement) actorrt.SpawnHandle {
	return spawnHandle{inc: inc, rt: rt, builder: builder, placement: placement}
}

// Fork mints a child owned by this handle's parent incarnation (§3.1/§10.2).
//
// The Builder returns the child's build closure `func(Incarnation) actorrt.Actor`
// with its own caps (livePen/liveAccess/…) ALREADY welded in — the caps缝 is
// baked into the closure by whoever populates the Builder table (the shared caps
// assembler, reused by admission and by the table populator), NOT re-applied
// here (the committed actorrt.Builder contract hands back a fully-wrapped
// `func(Incarnation) Actor`, so a child is born with the same membrane set as a
// top-level admission). This handle only derives the child name, asks placement,
// and drives the substrate fork primitive.
func (h spawnHandle) Fork(spec actorrt.ForkSpec) (actor.ActorID, error) {
	if h.builder == nil {
		return "", ErrNoBuilder
	}
	build, ok := h.builder.LookupByClass(spec.Class)
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
