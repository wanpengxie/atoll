package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// daemonAssignment is one actor instance the server assigns a daemon to host for
// a channel. It carries only what the daemon needs to BUILD the instance: id +
// class (the engine/tool class) + the resolved config (global identity overlaid
// by per-channel). Deliberately NO state/seed: a daemon-placed looper resumes
// from its OWN local state slot (claude self-manages locally); the server holds
// no state for daemon instances (a server-side audit copy is a future addition).
type daemonAssignment struct {
	InstanceID string          `json:"instance_id"`
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config,omitempty"`
	Epoch      int64           `json:"epoch"`
}

type appPlanProvider struct{ app *App }

func (p appPlanProvider) Plan(ctx context.Context, chID channel.ID, daemonID string) ([]platform.PlanActor, error) {
	assignments, err := p.app.daemonComposition(ctx, chID, daemonID)
	if err != nil {
		return nil, err
	}
	out := make([]platform.PlanActor, 0, len(assignments))
	for _, a := range assignments {
		kind, ok := registry.ClassKind(a.Class)
		if !ok {
			return nil, fmt.Errorf("app: plan instance %s has unknown class %q", a.InstanceID, a.Class)
		}
		out = append(out, platform.PlanActor{InstanceID: actor.ActorID(a.InstanceID), Class: a.Class, Config: a.Config,
			Kind: kind, Binding: actor.BindingRuntimeInboundViaRelay, Epoch: a.Epoch})
	}
	return out, nil
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
func (a *App) daemonComposition(ctx context.Context, chID channel.ID, daemonID string) ([]daemonAssignment, error) {
	h := a.getHome(chID)
	if h == nil {
		return nil, channelUnavailable()
	}
	rows, err := h.Composition(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]daemonAssignment, 0, len(rows))
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
		if _, ok := registry.ClassKind(r.Class); !ok {
			return nil, fmt.Errorf("app: plan instance %s has unknown class %q", r.InstanceID, r.Class)
		}
		out = append(out, daemonAssignment{
			InstanceID: string(r.InstanceID),
			Class:      r.Class,
			Config:     mergeConfig(global, r.ConfigJSON),
			Epoch:      r.Epoch,
		})
	}
	return out, nil
}
