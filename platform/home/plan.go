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
func (h *Home) PlanForDaemon(ctx context.Context, daemonID string) ([]platform.PlanActor, error) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]platform.PlanActor, 0, len(rows))
	for _, row := range rows {
		if row.Placement.Kind != storespec.PlacementDaemon || row.Placement.Host != daemonID {
			continue
		}
		state, ok := h.liveness.snapshot(row.ID)
		if !ok || state.ticket == "" || state.version != row.CurrentDeclVersion ||
			(state.occ != occStarting && state.occ != occDetached && state.occ != occRunning) {
			continue
		}
		config := append([]byte(nil), row.Config...)
		if view, ok := h.factories.(*compositionView); ok {
			config, err = view.resolveConfig(ctx, row.SourceDeclID, row.Config)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, platform.PlanActor{
			InstanceID: row.ID, Class: row.Class, Config: config,
			Kind: row.Kind, Binding: actor.BindingRuntimeInboundViaRelay, Version: row.CurrentDeclVersion,
			TIdleMs: row.TIdle.Milliseconds(), EnsureTicket: string(state.ticket),
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
		state, ok := h.liveness.snapshot(row.ID)
		if !ok {
			continue
		}
		if state.version != 0 && state.version != row.CurrentDeclVersion && state.occ != occNone {
			_, _ = h.liveness.Retire(row.ID, true)
			h.channel.Cells().DespawnID(row.ID)
			state, _ = h.liveness.snapshot(row.ID)
		}
		if state.occ == occNone && (row.TIdle == 0 || state.dirty || state.restart) {
			_, _ = h.liveness.BeginEnsure(row.ID, row.CurrentDeclVersion)
		}
	}
}
