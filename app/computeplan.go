package app

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/protocol/channel"
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
}

// daemonComposition reads the channel's DESIRED daemon-placed composition
// assigned to THIS daemon (channel_actors placement='daemon' AND
// desired_host=daemonID) and resolves each instance's config — the SAME read +
// global/per-channel overlay serverCompositionRows does for server-placed rows, but
// it RETURNS the data instead of spawning (the daemon builds + runs them with
// its local creds). The engine is ca.class directly. tool / looper / device are
// all just rows here — uniform, no special-casing.
//
// desired_host filtering resolves G4: two daemons bound to one channel each pull
// ONLY their own rows; an unassigned pool row (desired_host='') is delivered to
// no daemon (a legal transient — no daemon claims it yet).
func (a *App) daemonComposition(chID channel.ID, daemonID string) ([]daemonAssignment, error) {
	rows, err := a.daemonCompositionRows(chID, daemonID)
	if err != nil {
		return nil, err
	}
	out := make([]daemonAssignment, 0, len(rows))
	for _, r := range rows {
		out = append(out, daemonAssignment{
			InstanceID: r.instanceID,
			Class:      r.class,
			Config:     mergeConfig(r.globalCfg, r.channelCfg),
		})
	}
	return out, nil
}

// handleComputePlan is the daemon's pull endpoint: authenticated by the same
// ?key=+?channel= as /compute, it returns the channel's placement='daemon'
// assignment so the daemon builds EXACTLY that set. The link/attach protocol
// is untouched — the daemon declares what it builds, as today. This pull avoids a server→
// daemon wire change AND the membership-needs-Kind/Binding problem of a push
// model (cmd/server does not import actors/all, so it could not synthesise tool
// Kind/Binding; the daemon, which builds the decls, has them for free).
func (a *App) handleComputePlan(c *gin.Context) {
	apiKey := c.Query("key")
	chIDStr := c.Query("channel")
	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "api key required"})
		return
	}
	if chIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}
	chID := channel.ID(chIDStr)
	// Same single auth path as /compute: verify key + daemon-channel binding. A
	// flat 403 (no oracle) on any failure. The resolved daemonID filters the
	// plan to this daemon's own assignment (G4).
	daemonID, err := a.authAndResolve(apiKey, chID)
	if err != nil {
		a.logger.Warn("compute plan: auth failed", "channel", string(chID), "err", err.Error())
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}
	plan, err := a.daemonComposition(chID, daemonID)
	if err != nil {
		a.logger.Error("compute plan: read composition", "channel", string(chID), "err", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"assignments": plan})
}
