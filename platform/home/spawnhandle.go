package home

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ActorFactoryResolver is the platform-layer factory table for the two mint
// triggers that resolve a factory AFTER the original admission closure is gone —
// fork (by Class) and activation (by id). It resolves either key to the actor's
// ActorFactory (spec §4 S3's "def") — the SAME shape activation consumes.
//
// It is deliberately NOT actorrt.Builder (whose Lookup/LookupByClass hand back
// `func(Incarnation) Actor`): the caps seam — the livePen/liveAccess/liveState/
// liveSchedule membranes — must be woven at the platform assembly seam
// (buildCaps), where the live-membrane constructors live, and NEVER inside actorrt
// (which must not import runtime/harness · runtime/link). So the table hands back
// the ActorFactory and the platform wraps it in the shared caps assembler +
// build() per mint; the child/revived actor is therefore born with the identical
// membrane set as a top-level admission. Welding caps at the domain-populated
// table instead would require the domain to reach the platform membrane
// constructors — exactly the leak the archtest wall forbids. This is the
// "platform承接 the construction" posture the wiring spec pins for BOTH
// actorrt.Builder entries: the runtime contract is not bent to carry caps; the
// platform seam owns the weld.
type ActorFactoryResolver interface {
	// LookupByClass resolves a caller-declared, opaque implementation-selection key
	// to its ActorFactory — fork's entry (ForkSpec.Class). Kind is NOT
	// re-answered here (it is caller-held on ForkSpec.Kind). It takes the
	// already-derived childID and the parent's opaque config because the domain's
	// Build(class, InstanceSpec{ID, Config}, ...) table needs both to construct the
	// instance (a provider constructor rejects an empty ID) — the platform only
	// passes them through, it never touches the registry.
	LookupByClass(childID actor.ActorID, class string, config json.RawMessage) (def platform.ActorFactory, ok bool)
	// Lookup resolves an active identity id to its ActorFactory — activation's
	// entry (the reconcile ring's eager revival + the schedule engine's identity-
	// timer Reviver both hold only an id, since the original admission factory died
	// with the previous incarnation). Kind is NOT re-answered here either (it is
	// caller-held: DesiredMember.Kind for reconcile, the registry row for the
	// Reviver). The domain implementation maps id→class→factory over the same
	// underlying table LookupByClass reads.
	Lookup(id actor.ActorID) (def platform.ActorFactory, ok bool)
}

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
	home         *Home
	inc          actorrt.Incarnation
	birthVersion int64
	rt           *actorrt.Runtime
	logger       *slog.Logger
}

// newSpawnHandle welds a SpawnHandle to parent incarnation inc. rt/assemble are
// All construction dependencies are required. logger is variadic and
// nil-safe (falls back to a discard logger) so every EXISTING call site
// (caps.go/sysanchorcaps.go, out of this cluster's file scope) keeps compiling
// unchanged with no oplog wired yet; a future caller opts in by passing one.
func newSpawnHandle(home *Home, inc actorrt.Incarnation, birthVersion int64, rt *actorrt.Runtime, logger ...*slog.Logger) actorrt.LifecycleHandle {
	var lg *slog.Logger
	if len(logger) > 0 {
		lg = logger[0]
	}
	if lg == nil {
		lg = slog.New(slog.DiscardHandler)
	}
	return spawnHandle{home: home, inc: inc, birthVersion: birthVersion, rt: rt, logger: lg}
}

func (h spawnHandle) EndSelf(ctx context.Context) error {
	return h.home.EndIdentity(ctx, storespec.AuthorStamp{
		ID: h.inc.ID(), BirthVersion: h.birthVersion,
	}, h.inc.ID(), "self_end")
}

// Fork admits first, then makes one best-effort same-server activation attempt.
// Admission is the result contract: every accelerator miss is observation-only
// and the first normal request still drives the level reconcile path.
func (h spawnHandle) Fork(ctx context.Context, spec actorrt.ForkSpec) (actor.ActorID, error) {
	childID, err := h.home.forkAdmission(ctx, h.inc.ID(), h.birthVersion, spec, uuid.NewString())
	if err != nil {
		return "", err
	}
	h.logger.Info("actorrt.fork.admitted", "parent", string(h.inc.ID()), "child", string(childID))
	row, active, lookupErr := h.home.controlIndex.LookupActive(ctx, childID)
	if lookupErr == nil && active && row.Placement.Kind == storespec.PlacementServer {
		verdict := h.home.activateOne(ctx, row)
		switch verdict.kind {
		case actEmbodied, actAlreadyLive:
			h.home.clearReviveBackoff(childID)
		default:
			h.logger.Warn("fork_accelerator_miss", "parent", string(h.inc.ID()), "child", string(childID),
				"outcome", int(verdict.kind), "err", verdict.err)
		}
	} else if lookupErr != nil || !active {
		h.logger.Warn("fork_accelerator_miss", "parent", string(h.inc.ID()), "child", string(childID),
			"outcome", "authority_lookup", "err", lookupErr)
	}
	return childID, nil
}

// Despawn requests termination of one of this handle's own children by name.
// The by-id authority-check (childID's owner incarnation == h.inc) is performed
// inside the substrate (DespawnChild), so the caller never holds a bare kill —
// the handle itself never leaves the substrate.
func (h spawnHandle) DespawnChild(ctx context.Context, childID actor.ActorID, reason string) error {
	h.logger.Info("actorrt.despawn.requested", "parent", string(h.inc.ID()), "child", string(childID))
	if !h.rt.IsLive(h.inc) {
		return actorrt.ErrParentNotLive
	}
	err := h.home.endForkChild(ctx, h.inc.ID(), childID, reason)
	if err != nil {
		h.logger.Warn("actorrt.despawn.rejected", "parent", string(h.inc.ID()),
			"child", string(childID), "error", err)
		return err
	}
	h.logger.Info("actorrt.despawn.removed", "parent", string(h.inc.ID()), "child", string(childID))
	return nil
}
