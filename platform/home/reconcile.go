package home

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
// current active human membership projection. A candidate is re-read at the
// delete edge so a concurrent Admit after the list snapshot cannot lose its
// newly ensured slot.
func (h *Home) sweepSubjectSlots(ctx context.Context) {
	if h.subjectgate == nil || h.actors == nil {
		return
	}
	sweepSubjectSlots(ctx, h.logger, h.actors, h.subjectgate)
}

type subjectSlotAuthority interface {
	ListActive(context.Context) ([]storespec.ActorControlRow, error)
	LookupActive(context.Context, actor.ActorID) (storespec.ActorControlRow, bool, error)
}

func sweepSubjectSlots(
	ctx context.Context,
	logger *slog.Logger,
	authority subjectSlotAuthority,
	slots *subjectgate.Registry,
) {
	rows, err := authority.ListActive(ctx)
	if err != nil {
		logger.Warn("platform.subject_slot.list_failed", "error", err)
		return
	}
	desired := make(map[actor.ActorID]struct{})
	for _, row := range rows {
		if row.Kind == actor.KindHuman {
			desired[row.ID] = struct{}{}
			slots.EnsureSlot(row.ID)
		}
	}
	keys := slots.Keys()
	removed := 0
	for _, id := range keys {
		if _, keep := desired[id]; keep {
			continue
		}
		row, active, lookupErr := authority.LookupActive(ctx, id)
		if lookupErr != nil {
			logger.Warn("platform.subject_slot.lookup_failed", "actor", id, "error", lookupErr)
			continue
		}
		if active && row.Kind == actor.KindHuman {
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
