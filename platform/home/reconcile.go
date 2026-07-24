package home

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
)

// reconcileSweep contains only level-derived maintenance. Actor desired→actual
// convergence belongs to HostSupervisor and message delivery invokes the sole
// ensureRun transition directly; this loop never spawns, retries, or replays a
// collaboration request.
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
	h.sweepSubjectSlots()
	h.sweepPresence(ctx)
}

func (h *Home) reconcileDeclarations(ctx context.Context) {
	if h.actorStore == nil || h.actorStore.resolver == nil || h.actors == nil {
		return
	}
	rows, err := h.actors.ListActive(ctx)
	if err != nil {
		h.logger.Warn("platform.declaration_pull.list_failed", "error", err)
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if row.SourceDeclID == "" || (row.Kind != actor.KindAgent && row.Kind != actor.KindTool) {
			continue
		}
		resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
		facts, resolveErr := h.actorStore.resolver.ResolveDeclaration(
			resolveCtx, h.channelID, row.SourceDeclID,
		)
		cancel()
		if resolveErr != nil {
			if !errors.Is(resolveErr, channel.ErrDeclarationNotFound) {
				h.logger.Warn("platform.declaration_pull.resolve_failed",
					"actor", row.ID, "declaration", row.SourceDeclID, "error", resolveErr)
			}
			continue
		}
		kindCtx, kindCancel := context.WithTimeout(ctx, introductionResolveTimeout)
		kind, found, kindErr := h.actorStore.resolver.ClassKind(kindCtx, facts.Class)
		kindCancel()
		if kindErr != nil || !found || kind != row.Kind {
			continue
		}
		if string(row.Config) == string(facts.Config) && row.Class == facts.Class {
			continue
		}
		if err := h.actors.ApplyDeclaration(ctx, actorctl.DeclarationChange{
			ActorID: row.ID, Class: facts.Class,
			Config:    append([]byte(nil), facts.Config...),
			RequestID: message.ID("declaration-pull:v1:" + uuid.NewString()),
		}); err != nil {
			h.logger.Warn("platform.declaration_pull.apply_failed",
				"actor", row.ID, "declaration", row.SourceDeclID, "error", err)
		}
	}
}

func (h *Home) reconcileDaemonTombstones(ctx context.Context) {
	if h.actorStore == nil || h.actorStore.resolver == nil || h.actors == nil {
		return
	}
	ids, err := h.cs.Bindings.ListBoundDaemons(ctx)
	if err != nil {
		h.logger.Warn("platform.daemon_pull.list_failed", "error", err)
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
		facts, resolveErr := h.actorStore.resolver.DaemonFacts(resolveCtx, string(id))
		cancel()
		if resolveErr != nil || !facts.Deleted {
			continue
		}
		_, err := h.actors.DetachDaemon(ctx, channel.DaemonRequest{
			Ref: "daemon-pull:v1:" + uuid.NewString(), DaemonID: string(id),
		})
		if err != nil {
			h.logger.Warn("platform.daemon_pull.detach_failed", "daemon", id, "error", err)
		}
	}
}

func (h *Home) reconcileClosure(ctx context.Context) {
	if h.systemPen == nil || h.cs == nil || h.actors == nil {
		return
	}
	onFault := func(requestID message.ID, err error) {
		h.logger.Error("platform.closure.reconcile_fault",
			"channel", h.channelID, "request", requestID, "err", err)
	}
	err := behavior.ReconcileReceiverUnavailable(
		ctx,
		h.systemPen,
		h.cs.Query,
		func(ctx context.Context, id actor.ActorID) (bool, error) {
			_, active, err := h.actors.LookupActive(ctx, id)
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
// current active human membership projection.
func (h *Home) sweepSubjectSlots() {
	if h.subjectgate == nil || h.actors == nil {
		return
	}
	rows, err := h.actors.ListActive(context.Background())
	if err != nil {
		return
	}
	desired := make(map[actor.ActorID]struct{})
	for _, row := range rows {
		if row.Kind == actor.KindHuman {
			desired[row.ID] = struct{}{}
			h.subjectgate.EnsureSlot(row.ID)
		}
	}
	for _, id := range h.subjectgate.IDs() {
		if _, keep := desired[id]; !keep {
			h.subjectgate.Remove(id)
		}
	}
}

func (h *Home) sweepPresence(ctx context.Context) {
	if h.presenceFold == nil || h.actors == nil {
		return
	}
	rows, err := h.actors.ListActive(ctx)
	if err != nil {
		return
	}
	keep := make(map[actor.ActorID]struct{}, len(rows))
	for _, row := range rows {
		keep[row.ID] = struct{}{}
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
