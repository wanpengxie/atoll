package home

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelspec"
)

func (h *Home) emitRelations(deltas ...channelspec.RelationDelta) {
	if h == nil || h.onRelationChange == nil || len(deltas) == 0 {
		return
	}
	batch := make([]channelspec.RelationDelta, len(deltas))
	copy(batch, deltas)
	for i := range batch {
		batch[i].ChannelID = h.channelID
	}
	h.onRelationChange(h.channelID, batch)
}

// emitRelationSnapshot announces the membrane's complete relation facts once
// after a real Home open succeeds. It is set alignment input, not a periodic
// sweep and not a realm-side readback.
func (h *Home) emitRelationSnapshot(ctx context.Context) {
	if h == nil || h.onRelationChange == nil {
		return
	}
	deltas := []channelspec.RelationDelta{{ChannelID: h.channelID, Reset: true}}
	roster, err := h.View().HumanRoster(ctx)
	if err != nil {
		h.logger.Warn("platform.relation_snapshot_failed", "channel", h.channelID, "edge", "membership", "err", err)
		return
	}
	for _, entry := range roster {
		deltas = append(deltas, channelspec.RelationDelta{
			Kind: channelspec.RelationJoined, ChannelID: h.channelID,
			Principal: entry.Principal, ActorID: entry.ActorID,
		})
	}
	instances, err := h.controller.DeclaredReconcileList()
	if err != nil {
		h.logger.Warn("platform.relation_snapshot_failed", "channel", h.channelID, "edge", "instances", "err", err)
		return
	}
	for _, instance := range instances {
		deltas = append(deltas, channelspec.RelationDelta{
			Kind: channelspec.RelationIntroduced, ChannelID: h.channelID,
			DeclID: instance.SourceDeclID, ActorID: instance.ID,
		})
	}
	daemons, err := h.bindings.ListBoundDaemons(ctx)
	if err != nil {
		h.logger.Warn("platform.relation_snapshot_failed", "channel", h.channelID, "edge", "bindings", "err", err)
		return
	}
	for _, daemon := range daemons {
		deltas = append(deltas, channelspec.RelationDelta{
			Kind: channelspec.RelationBound, ChannelID: h.channelID,
			DaemonID: string(daemon),
		})
	}
	h.onRelationChange(h.channelID, deltas)
}
