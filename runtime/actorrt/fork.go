package actorrt

import (
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// ErrParentNotLive is returned by Fork when the parent incarnation is not (or
// is no longer) live — either the lock-free fast-path check at entry, or the
// re-check inside the same critical section as the child's go-live (a parent
// that died during the build window). No child embodiment is ever inserted or
// started on this path.
var ErrParentNotLive = errors.New("actorrt: parent not live")

// ErrChildIDCollision is returned by Fork when childID already names a live
// embodiment. Unlike Attach (whose wire re-bind stops and replaces, last-go-live-wins), a fork collision is a
// HARD failure — fork always mints a fresh identity; a colliding
// childID is a caller bug, not a legitimate replace scenario.
var ErrChildIDCollision = errors.New("actorrt: child id collision")

// Fork mints a child cell owned by parent's incarnation.
// Ownership binds the PARENT'S INCARNATION (not identity): when parent dies,
// every still-live child is signal-cascaded to death too (removeIf) —
// ownership lives only in memory (r.owned), never persisted.
//
// Two-phase construction (mirrors Spawn) with TWO liveness checks bracketing
// the build window:
//  1. entry, lock-free (IsLive): parent already dead → fail fast, no wasted
//     build.
//  2. go-live, inside r.mu, in the SAME critical section as the embodiment
//     insert + ownership-edge append: re-check r.embodiments[parent.id]==parent.p.
//     A parent dying during the (lock-free) build window is caught HERE — its
//     own death path (removeIf) also takes r.mu, so the two are totally
//     ordered by r.mu and this check can never miss it. If it fails, the
//     child's shell is discarded — never inserted into embodiments, never
//     started — so Fork never produces an orphan needing a "revert" step.
//
// childID colliding with an existing embodiment is a HARD failure
// (ErrChildIDCollision) — NOT Spawn's replace semantics.
//
// kind is welded onto the child's embodiment (G11: the caller — ForkSpec.Kind —
// always holds it; the child's incarnation household must carry it, same as an
// admission Spawn's), read back later via Runtime.Stat.
func (r *Runtime) Fork(parent Incarnation, childID actor.ActorID, kind actor.Kind, build func(Incarnation) Actor) (Incarnation, error) {
	r.mu.RLock()
	sealed := r.sealed
	r.mu.RUnlock()
	if sealed {
		return Incarnation{}, ErrRuntimeSealed
	}
	if !r.IsLive(parent) { // ① fast-path, lock-free
		return Incarnation{}, ErrParentNotLive
	}
	c := allocShell(r.parent, childID, kind, r.mailbox, r.publishDown, r.publishObs, r.removeIf, r.reapZombie, r.clock(), r.logger)
	var err error
	c.impl, err = buildActor(build, Incarnation{id: childID, p: c}) // outside the lock, same discipline as Spawn.
	if err != nil {
		c.cancel()
		return Incarnation{}, err
	}

	r.mu.Lock()
	if r.sealed {
		r.mu.Unlock()
		abortBuild(c)
		return Incarnation{}, ErrRuntimeSealed
	}
	if _, exists := r.embodiments[childID]; exists { // collision = hard fail
		r.mu.Unlock()
		// Release the discarded shell's ctx node: allocShell derived it from
		// r.parent, so an uncancelled discard would pin a child-context entry
		// in the parent's tree for the whole channel lifetime (the shell never
		// started; cancel is idempotent and unobserved).
		abortBuild(c)
		return Incarnation{}, ErrChildIDCollision
	}
	if r.embodiments[parent.id] != parent.p { // ② same critical section re-check
		r.mu.Unlock() // not passed: do not insert, do not go-live.
		abortBuild(c) // same discard-release as the collision arm above
		return Incarnation{}, ErrParentNotLive
	}
	r.embodiments[childID] = c
	// Prune already-not-live entries out of r.owned[parent.p] BEFORE appending
	// the new child: a long-lived parent that forks many
	// short-lived children would otherwise grow this slice without bound —
	// each already-dead child's OWN initiateStop/death path already unhooked it
	// from r.embodiments, but the stale pointer would linger here until the
	// parent itself dies. Amortised into the regular Fork path; no separate
	// sweep/GC.
	live := r.owned[parent.p][:0]
	for _, ch := range r.owned[parent.p] {
		if ch.isLive() {
			live = append(live, ch)
		}
	}
	r.owned[parent.p] = append(live, c) // ownership edge recorded in the same critical section.
	c.live.Store(true)
	r.mu.Unlock()
	c.start()
	return Incarnation{id: childID, p: c}, nil
}

// Builder is the injection point contract for activation/fork: a queryable table from
// (id) or (class) to a build closure, shared by BOTH triggers underneath the
// same domain-owned table (fork/activation are one mechanism) —
// runtime defines the shape, domain fills the table (fat daemon registry.Build).
// Neither entry returns a Kind: Kind is caller-held (ForkSpec.Kind /
// DesiredMember.Kind), never re-answered by Builder — Kind (protocol
// classification) and "which implementation" are orthogonal fields, not one
// selecting the other.
type Builder interface {
	// Lookup resolves an already-durable member's id to its build closure —
	// activation's entry (the triggering party knows only the id; the original
	// factory closure died with the prior incarnation).
	Lookup(id actor.ActorID) (build func(Incarnation) Actor, ok bool)
	// LookupByClass resolves a caller-declared, opaque implementation-selection
	// key to its build closure — fork's entry (ForkSpec.Class; there is no
	// durable-membership row to look an id up in).
	LookupByClass(class string) (build func(Incarnation) Actor, ok bool)
}

// ForkSpec is the caller-declared, wire-serialisable description of a child to
// fork. It carries no Go closure — fork and activation share
// one Builder-backed mint path so an out-of-process
// parent (a daemon-hosted actor) can fork too, over a wire that can only carry
// serialisable fields.
type ForkSpec struct {
	// Kind is the substrate's own protocol classification (the closed set
	// welded into Mint(id, kind, chID)) — it does NOT select which
	// implementation runs (every tool class shares Kind=KindTool; Kind cannot
	// distinguish one tool implementation from another). Orthogonal to Class.
	Kind actor.Kind
	// Class is the opaque implementation-selection key the Builder looks up
	// (LookupByClass) — substrate does not interpret this value, it is passed
	// through verbatim to the domain's registry.Build(class, ...) table.
	Class string
	// NameHint derives childID as parentID + "/" + NameHint (namespace
	// derivation) — no substrate id allocator needed.
	NameHint string
	// Config is the parent's opaque per-instance委托 for the child — the fork
	// counterpart of admission's InstanceSpec.Config (the Unix fork+exec argv /
	// Erlang spawn(M,F,Args) Args). Substrate does not interpret it: it is passed
	// through verbatim to the domain's Build(class, InstanceSpec{Config}, ...)
	// table, so an out-of-process parent could carry it too (it rides the
	// wire-serialisable ForkSpec, never a Go closure). It is the child's incarnation
	// intent —随 incarnation, never persisted.
	Config json.RawMessage
}

// SpawnHandle is the capability a parent incarnation holds to fork/despawn ITS
// OWN children. Fork returns only the child's NAME
// (childID), never its incarnation truth-handle — the handle never leaves
// substrate (the handle is never exposed outside it); a holder that could pass around a live
// incarnation handle could bypass the by-id authority-check Despawn performs.
// The concrete implementation (welding a parent Incarnation + Minter for
// pen-welding) lives in platform, not here — this is pure actorrt vocabulary
// (Kind/Class/NameHint/ActorID are all actorrt-level types), actorrt itself
// must not import runtime/harness.
type SpawnHandle interface {
	// Fork mints a new child owned by the incarnation this handle is welded to,
	// returning the child's name.
	Fork(spec ForkSpec) (actor.ActorID, error)
	// Despawn requests termination of one of this handle's own children by
	// name. The implementation performs the by-id authority-check internally
	// (childID's owner must equal the incarnation this handle is welded to)
	// before executing — the caller never gets a bare, unchecked kill.
	Despawn(childID actor.ActorID) error
}

// ErrNotOwner is returned by DespawnChild when childID does not name a
// embodiment owned by the given parent incarnation — either no such embodiment
// exists, or it exists but is owned by a different parent (or not fork-owned
// at all). Both cases collapse to the same rejection: a by-id caller gets no
// information about WHY an id it does not own is refused — since the handle
// is never exposed outside substrate, a name-only request only ever resolves
// through this authority gate, it never gets to distinguish "doesn't exist"
// from "not yours".
var ErrNotOwner = errors.New("actorrt: child not owned by this incarnation")

// DespawnChild is the by-id, authority-checked termination entry backing
// SpawnHandle.Despawn: it confirms childID names an embodiment currently
// listed in r.owned[parent.p] BEFORE despawning it — so a SpawnHandle can
// request termination of its own fork children BY NAME ONLY, without ever
// holding the child's Incarnation truth-handle. A childID that is
// absent, or present but not owned by parent, is ErrNotOwner.
func (r *Runtime) DespawnChild(parent Incarnation, childID actor.ActorID) error {
	r.mu.Lock()
	child, ok := r.embodiments[childID]
	if ok {
		ok = false
		for _, ch := range r.owned[parent.p] {
			if ch == child {
				ok = true
				break
			}
		}
	}
	if !ok {
		r.mu.Unlock()
		return ErrNotOwner
	}
	// REPLACEMENT-LIVE-FLIP INVARIANT (same as Despawn/DespawnID): flip dead in the
	// SAME critical section as the map delete — no live-but-unmapped window for a
	// stale welded cap to pass IsLive. markDead is an idempotent atomic and never
	// re-enters r.mu, so it is deadlock-safe in-lock. Enrol as a zombie in the SAME
	// critical section (P0-1).
	retirement := r.retireCurrentLocked(childID, child, flavorDespawn)
	r.mu.Unlock()
	// Mirrors Despawn's own guarded teardown: the escort drives signalDespawn (a
	// by-name termination — a port emits KindDespawn before closing; a cell child,
	// the only kind Fork mints today, collapses it to the quiet teardown) then joins
	// bounded by grace. DespawnChild returns in O(judge-dead), never waiting on the
	// child goroutine. The stale r.owned[parent.p] entry is left in place — it is
	// !isLive() from here on and gets pruned on the parent's next Fork.
	runRetirement(retirement)
	return nil
}
