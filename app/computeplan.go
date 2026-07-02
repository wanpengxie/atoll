package app

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// daemonAssignment is one actor instance the server assigns a daemon to host for
// a channel (daemon-composition spec §3). It carries only what the daemon needs
// to BUILD the instance: id + class (the engine/tool class — engine=class per
// agent-kind-vs-class) + the resolved config (global identity overlaid by
// per-channel). Deliberately NO state/seed: a daemon-placed looper resumes from
// its OWN local state slot (claude self-manages locally); the server holds no
// state for daemon instances (server-side audit copy = v2).
type daemonAssignment struct {
	InstanceID string          `json:"instance_id"`
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// daemonComposition reads the channel's DESIRED daemon-placed composition
// (channel_actors placement='daemon') and resolves each instance's config — the
// SAME read + global/per-channel overlay spawnComposition does for server-placed
// rows, but it RETURNS the data instead of spawning (the daemon builds + runs
// them with its local creds). The engine is ca.class directly. tool / looper /
// device are all just rows here — uniform, no special-casing.
func (a *App) daemonComposition(chID channel.ID) ([]daemonAssignment, error) {
	rows, err := a.db.Query(
		`SELECT ca.instance_id, ca.class, COALESCE(ca.config_json, ''), COALESCE(a.config_json, '')
		   FROM channel_actors ca
		   LEFT JOIN agents a ON ca.instance_id = 'agent:' || a.id
		  WHERE ca.channel_id = ? AND ca.placement = ?`,
		string(chID), placementDaemon)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []daemonAssignment{}
	for rows.Next() {
		var id, class, cfg, gcfg string
		if err := rows.Scan(&id, &class, &cfg, &gcfg); err != nil {
			continue
		}
		out = append(out, daemonAssignment{
			InstanceID: id,
			Class:      class,
			Config:     mergeConfig(gcfg, cfg),
		})
	}
	return out, rows.Err()
}

// handleComputePlan is the daemon's pull endpoint (daemon-composition spec §3):
// authenticated by the same ?key=+?channel= as /compute, it returns the
// channel's placement='daemon' assignment so the daemon builds EXACTLY that set
// (no blind-build of registry.Classes()). The link/attach protocol is untouched
// — the daemon declares what it builds, as today. This pull avoids a server→
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
	// flat 403 (no oracle) on any failure.
	if _, err := a.authAndResolve(apiKey, chID); err != nil {
		a.logger.Warn("compute plan: auth failed", "channel", string(chID), "err", err.Error())
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}
	plan, err := a.daemonComposition(chID)
	if err != nil {
		a.logger.Error("compute plan: read composition", "channel", string(chID), "err", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"assignments": plan})
}
