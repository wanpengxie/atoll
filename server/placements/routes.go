package placements

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// RegisterRoutes mounts read-only placement endpoints — useful for
// UI debugging + ops. Mutating endpoints (reserve / activate) are
// internal: they're invoked by gateway after catalog.CreateChannel
// or by daemonbus after reading ACK frames.
func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.GET("/placements/:chID", s.handleGetPlacement)
	g.GET("/placements", s.handleListPlacements)
}

func (s *Service) handleGetPlacement(c *gin.Context) {
	p, ok, err := s.Get(c.Request.Context(), channel.ID(c.Param("chID")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "placement not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"channel_id":              string(p.ChannelID),
		"daemon_id":               string(p.DaemonID),
		"state":                   string(p.State),
		"owner_epoch":             int64(p.OwnerEpoch),
		"fencing_token":           int64(p.FencingToken),
		"create_request_id":       string(p.CreateRequestID),
		"daemon_connection_epoch": int64(p.DaemonConnectionEpoch),
		"last_heartbeat_at":       p.LastHeartbeatAt,
		"created_at":              p.CreatedAt,
		"activated_at":            p.ActivatedAt,
		"entered_state_at":        p.EnteredStateAt,
	})
}

func (s *Service) handleListPlacements(c *gin.Context) {
	state := c.Query("state")
	if state == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "state query required"})
		return
	}
	plist, err := s.ListByState(c.Request.Context(), parseState(state))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	out := make([]gin.H, 0, len(plist))
	for _, p := range plist {
		out = append(out, gin.H{
			"channel_id":       string(p.ChannelID),
			"daemon_id":        string(p.DaemonID),
			"state":            string(p.State),
			"created_at":       p.CreatedAt,
			"activated_at":     p.ActivatedAt,
			"entered_state_at": p.EnteredStateAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"placements": out})
}
