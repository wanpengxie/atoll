package home

import (
	"context"
	"sort"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// PlanForDaemon is the sole PlanActor constructor. It projects attachment
// intent already established by Home reconciliation; it never independently
// decides whether a dormant actor should run.
func (h *Home) planForDaemon(ctx context.Context, daemonID string) ([]platform.PlanActor, error) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]platform.PlanActor, 0, len(rows))
	for _, row := range rows {
		if row.Placement.Kind != storespec.PlacementDaemon || row.Placement.Host != daemonID {
			continue
		}
		intent := h.liveness.AttachmentIntent(row.ID)
		if !intent.Present || intent.Version != row.CurrentDeclVersion {
			continue
		}
		out = append(out, platform.PlanActor{
			InstanceID: row.ID, Class: row.Class, Config: append([]byte(nil), row.Config...),
			Kind: row.Kind, Binding: actor.BindingRuntimeInboundViaRelay, Version: row.CurrentDeclVersion,
			TIdleMs: row.TIdle.Milliseconds(), EnsureTicket: string(intent.Ticket),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

func (h *Home) reconcileDaemonIntent(ctx context.Context) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		h.logger.Error("platform.reconcile.daemon_intent_failed", "err", err)
		return
	}
	for _, row := range rows {
		if row.Placement.Kind != storespec.PlacementDaemon {
			continue
		}
		if _, retired := h.liveness.RetireIfVersionSkew(row.ID, row.CurrentDeclVersion); retired {
			h.channel.Cells().DespawnID(row.ID)
		}
		state, ok := h.liveness.WakeStanding(row.ID)
		if !ok {
			continue
		}
		if state.Occ == occNone && (row.TIdle == 0 || state.Dirty || state.Restart) {
			_, _ = h.liveness.BeginEnsure(row.ID, row.CurrentDeclVersion)
		}
	}
}
