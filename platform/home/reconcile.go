package home

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// reconcileSweep contains only level-derived maintenance. Actor desired→actual
// convergence belongs to HostSupervisor; this loop never spawns, retries, or
// replays a collaboration request.
func (h *Home) reconcileSweep(ctx context.Context) {
	h.reconcileDeclarations(ctx)
	if ctx.Err() != nil {
		return
	}
	h.reconcileDaemonTombstones(ctx)
	if ctx.Err() != nil {
		return
	}
	h.reconcileClosure(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepExpired(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepPresence(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepSubjectSlots(ctx)
}

// reconcileDeclarations pulls the current declaration for every declared
// instance. It eats the Controller's DeclaredReconcileList question-shaped
// projection — the comparison inputs, not a whole-record face.
func (h *Home) reconcileDeclarations(ctx context.Context) {
	if h.resolver == nil || h.actors == nil {
		return
	}
	instances, err := h.controller.DeclaredReconcileList()
	if err != nil {
		h.logger.Warn("platform.declaration_pull.list_failed", "error", err)
		return
	}
	for _, instance := range instances {
		if ctx.Err() != nil {
			return
		}
		if instance.Kind != actor.KindAgent && instance.Kind != actor.KindTool {
			continue
		}
		resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
		facts, resolveErr := h.resolver.ResolveDeclaration(
			resolveCtx, h.channelID, instance.SourceDeclID,
		)
		cancel()
		if resolveErr != nil {
			if !errors.Is(resolveErr, channelspec.ErrDeclarationNotFound) {
				h.logger.Warn("platform.declaration_pull.resolve_failed",
					"actor", instance.ID, "declaration", instance.SourceDeclID, "error", resolveErr)
			}
			continue
		}
		kindCtx, kindCancel := context.WithTimeout(ctx, introductionResolveTimeout)
		kind, found, kindErr := h.resolver.ClassKind(kindCtx, facts.Class)
		kindCancel()
		if kindErr != nil || !found || kind != instance.Kind {
			continue
		}
		candidate := storespec.ActorDefinition{
			Class: facts.Class, Config: append([]byte(nil), facts.Config...),
		}
		// Skipping an equal definition saves one call; correctness rests on the
		// verb's own equal-value no-op, never on this comparison.
		if instance.Definition.Equal(candidate) {
			continue
		}
		if err := h.actors.ApplyDeclaration(ctx, actorctl.DeclarationChange{
			ActorID: instance.ID, Definition: candidate,
		}); err != nil {
			h.logger.Warn("platform.declaration_pull.apply_failed",
				"actor", instance.ID, "declaration", instance.SourceDeclID, "error", err)
		}
	}
}

// reconcileDaemonTombstones detaches daemons the realm has deleted. Detach is a
// wiring-domain action: it removes the binding row and kills no actor. Actors
// left placed on the gone daemon dangle legally.
func (h *Home) reconcileDaemonTombstones(ctx context.Context) {
	if h.resolver == nil || h.actors == nil || h.opEntry == nil {
		return
	}
	ids, err := h.bindings.ListBoundDaemons(ctx)
	if err != nil {
		h.logger.Warn("platform.daemon_pull.list_failed", "error", err)
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
		facts, resolveErr := h.resolver.DaemonFacts(resolveCtx, string(id))
		cancel()
		if resolveErr != nil || !facts.Deleted {
			continue
		}
		if _, err := h.opEntry.DetachDaemon(ctx, channelspec.DaemonRequest{
			Ref: "daemon-pull:v1:" + uuid.NewString(), DaemonID: string(id),
		}); err != nil {
			h.logger.Warn("platform.daemon_pull.detach_failed", "daemon", id, "error", err)
		}
	}
}

func (h *Home) reconcileClosure(ctx context.Context) {
	if h.systemPen == nil || h.query == nil || h.actors == nil {
		return
	}
	onFault := func(requestID message.ID, err error) {
		h.logger.Error("platform.closure.reconcile_fault",
			"channel", h.channelID, "request", requestID, "err", err)
	}
	err := behavior.ReconcileReceiverUnavailable(
		ctx,
		h.systemPen,
		h.query,
		func(ctx context.Context, id actor.ActorID) (bool, error) {
			active, err := h.actors.IsActive(ctx, id)
			return !active, err
		},
		func() time.Time { return time.UnixMilli(h.nowMs()) },
		onFault,
	)
	if err != nil {
		h.logger.Error("platform.closure.reconcile_scan_failed", "err", err)
	}
}

// sweepSubjectSlots is the sole physical delete owner. Desired slots are the
// current active human membership projection. A candidate is re-read at the
// delete edge so a concurrent Admit after the list snapshot cannot lose its
// newly ensured slot.
func (h *Home) sweepSubjectSlots(ctx context.Context) {
	if h.subjectgate == nil || h.actors == nil {
		return
	}
	sweepSubjectSlots(ctx, h.logger, h.actors, h.subjectgate)
}

// subjectSlotAuthority is the connection-slot sweep's whole question: who is a
// member right now, and (at the delete edge) is this one still a human member.
type subjectSlotAuthority interface {
	storespec.IdentityRoster
	storespec.ActorFactsAuthority
}

func sweepSubjectSlots(
	ctx context.Context,
	logger *slog.Logger,
	authority subjectSlotAuthority,
	slots *subjectgate.Registry,
) {
	identities, err := authority.ActiveIdentities()
	if err != nil {
		logger.Warn("platform.subject_slot.list_failed", "error", err)
		return
	}
	desired := make(map[actor.ActorID]struct{})
	for _, identity := range identities {
		if identity.Kind == actor.KindHuman {
			desired[identity.ID] = struct{}{}
			slots.EnsureSlot(identity.ID)
		}
	}
	keys := slots.Keys()
	removed := 0
	for _, id := range keys {
		if _, keep := desired[id]; keep {
			continue
		}
		facts, active, lookupErr := authority.ActorFacts(ctx, id)
		if lookupErr != nil {
			logger.Warn("platform.subject_slot.lookup_failed", "actor", id, "error", lookupErr)
			continue
		}
		if active && facts.Kind == actor.KindHuman {
			continue
		}
		slots.Remove(id)
		removed++
	}
	if removed > 0 {
		logger.Debug("platform.subject_slot.swept", "rows", removed)
	}
}

func (h *Home) sweepPresence(ctx context.Context) {
	if h.presenceFold == nil || h.actors == nil {
		return
	}
	identities, err := h.actors.ActiveIdentities()
	if err != nil {
		return
	}
	keep := make(map[actor.ActorID]struct{}, len(identities))
	for _, identity := range identities {
		keep[identity.ID] = struct{}{}
	}
	for _, id := range h.actors.HostedIDs() {
		keep[id] = struct{}{}
	}
	h.presenceFold.Sweep(func(id actor.ActorID) bool {
		_, ok := keep[id]
		return ok
	})
}

func (h *Home) pokeReconcile() {
	if h == nil || h.disablePoke() {
		return
	}
	select {
	case h.pokeCh <- struct{}{}:
	default:
	}
}

func (h *Home) disablePoke() bool { return h.closed.Load() }
