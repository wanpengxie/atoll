package app

import (
	"context"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/channel"
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
	return h.PlanForDaemon(ctx, daemonID)
}
