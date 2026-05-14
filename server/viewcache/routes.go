package viewcache

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/viewsync"
)

// RegisterRoutes mounts the read-only viewcache endpoints + the
// front-end-triggered resync hook.
func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.GET("/channels/:chID/messages", s.handleMessages)
	g.GET("/channels/:chID/cursor", s.handleCursor)
	g.POST("/channels/:chID/resync", s.handleResync)
}

func (s *Service) handleMessages(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	afterStr := c.DefaultQuery("after", "0")
	limitStr := c.DefaultQuery("limit", "200")
	after, err := strconv.ParseInt(afterStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "after must be integer"})
		return
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be integer"})
		return
	}
	msgs, err := s.Messages(c.Request.Context(), chID, viewsync.Seq(after), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"messages": msgs})
}

func (s *Service) handleCursor(c *gin.Context) {
	cur, err := s.Cursor(c.Request.Context(), channel.ID(c.Param("chID")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"last_received_seq": int64(cur)})
}

type resyncReq struct {
	SinceSeq int64 `json:"since_seq"`
	UntilSeq int64 `json:"until_seq"`
}

func (s *Service) handleResync(c *gin.Context) {
	var req resyncReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cur, err := s.TriggerResync(
		c.Request.Context(),
		channel.ID(c.Param("chID")),
		viewsync.Seq(req.SinceSeq), viewsync.Seq(req.UntilSeq),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"last_received_seq": int64(cur)})
}
