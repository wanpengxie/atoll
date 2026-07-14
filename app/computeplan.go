package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type appPlanProvider struct{ app *App }

func (p appPlanProvider) Plan(ctx context.Context, chID channel.ID, daemonID string) ([]platform.PlanActor, error) {
	return p.app.daemonComposition(ctx, chID, daemonID)
}

// daemonComposition reads the channel's DESIRED daemon-placed composition
// assigned to THIS daemon (channel-local composition placement='daemon' AND
// desired_host=daemonID) and resolves each instance's config — the SAME read +
// global/per-channel overlay serverCompositionRows does for server-placed rows, but
// it RETURNS the data instead of spawning (the daemon builds + runs them with
// its local creds). The engine is ca.class directly. tool / looper / device are
// all just rows here — uniform, no special-casing.
//
// desired_host filtering resolves G4: two daemons bound to one channel each pull
// ONLY their own rows; an unassigned pool row (desired_host=”) is delivered to
// no daemon (a legal transient — no daemon claims it yet).
func (a *App) daemonComposition(ctx context.Context, chID channel.ID, daemonID string) ([]platform.PlanActor, error) {
	h := a.getHome(chID)
	if h == nil {
		return nil, channelUnavailable()
	}
	rows, err := h.Composition(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]platform.PlanActor, 0, len(rows))
	for _, r := range rows {
		if r.Placement != storespec.PlacementDaemon || r.DesiredHost != daemonID {
			continue
		}
		global := ""
		if !strings.HasPrefix(r.DeclID, "sys:") {
			if err := a.db.QueryRowContext(ctx, `SELECT COALESCE(config_json,'') FROM actor_decls WHERE id=? AND deleted_at IS NULL`, r.DeclID).Scan(&global); err != nil {
				return nil, fmt.Errorf("app: plan instance %s resolve declaration %q: %w", r.InstanceID, r.DeclID, err)
			}
		}
		kind, ok := registry.ClassKind(r.Class)
		if !ok {
			return nil, fmt.Errorf("app: plan instance %s has unknown class %q", r.InstanceID, r.Class)
		}
		out = append(out, platform.PlanActor{
			InstanceID: r.InstanceID,
			Class:      r.Class,
			Config:     mergeConfig(global, r.ConfigJSON),
			Kind:       kind,
			Binding:    actor.BindingRuntimeInboundViaRelay,
			Epoch:      r.Epoch,
		})
	}
	return out, nil
}
