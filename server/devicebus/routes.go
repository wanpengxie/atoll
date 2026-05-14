package devicebus

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/server/identity"
)

// RegisterRoutes mounts the issuance + revoke endpoints. Callers
// must be authenticated upstream (gateway's identity middleware).
func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("/channels/:chID/devices", s.handleIssue)
	g.DELETE("/devices/:sid", s.handleRevoke)
	g.GET("/devices/:sid", s.handleGet)
}

type issueReq struct {
	DeviceID   string `json:"device_id"   binding:"required"`
	DeviceType string `json:"device_type"`
	DaemonID   string `json:"daemon_id"   binding:"required"`
}

func (s *Service) handleIssue(c *gin.Context) {
	var req issueReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)
	res, err := s.IssueSession(c.Request.Context(), IssueInput{
		DeviceID:   req.DeviceID,
		DeviceType: req.DeviceType,
		ChannelID:  channel.ID(c.Param("chID")),
		UserID:     u.ID,
		DaemonID:   placement.DaemonID(req.DaemonID),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"device_session_id": res.Session.ID,
		"token":             res.Token,
		"expires_at":        res.Session.ExpiresAt,
	})
}

func (s *Service) handleRevoke(c *gin.Context) {
	if err := s.Revoke(c.Request.Context(), c.Param("sid")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "revoked"})
}

func (s *Service) handleGet(c *gin.Context) {
	row, err := s.Get(c.Request.Context(), c.Param("sid"))
	if err != nil {
		status := http.StatusInternalServerError
		if err == ErrSessionNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"device_session_id": row.ID,
		"device_id":         row.DeviceID,
		"channel_id":        string(row.ChannelID),
		"daemon_id":         string(row.DaemonID),
		"state":             string(row.State),
		"expires_at":        row.ExpiresAt,
		"created_at":        row.CreatedAt,
	})
}
