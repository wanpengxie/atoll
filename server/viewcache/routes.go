package viewcache

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/identity"
)

const (
	defaultMessagesLimit = 200
	maxMessagesLimit     = 500
	maxResyncRange       = 500
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
	memberActorID, err := s.authorizeChannel(c, chID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	afterStr := c.DefaultQuery("after", "0")
	limitStr := c.DefaultQuery("limit", strconv.Itoa(defaultMessagesLimit))
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
	if limit <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be positive"})
		return
	}
	if limit > maxMessagesLimit {
		limit = maxMessagesLimit
	}
	msgs, err := s.Messages(c.Request.Context(), chID, viewsync.Seq(after), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Return a flat envelope array — UI consumes the same envelope shape
	// here as it does on the pushhub WS frame. The outer store-derived
	// metadata (StoredMessage{Seq, MessageID, ReceivedAt}) is intentionally
	// not leaked: seq is already inside envelope.seq, the message id is
	// envelope.id, and received_at corresponds to envelope.ts_received.
	envs := make([]message.Envelope, 0, len(msgs))
	for _, m := range msgs {
		env := m.Envelope
		if env.Seq == 0 {
			env.Seq = int64(m.Seq)
		}
		if env.TSReceived == 0 {
			env.TSReceived = m.ReceivedAt
		}
		if !channelaccess.VisibleToActor(env, memberActorID) {
			continue
		}
		envs = append(envs, env)
	}
	c.JSON(http.StatusOK, gin.H{"messages": envs})
}

func (s *Service) handleCursor(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	if _, err := s.authorizeChannel(c, chID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	cur, err := s.Cursor(c.Request.Context(), chID)
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
	chID := channel.ID(c.Param("chID"))
	if _, err := s.authorizeChannel(c, chID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	var req resyncReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.SinceSeq < 0 || req.UntilSeq < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "since_seq and until_seq must be non-negative"})
		return
	}
	if req.SinceSeq > req.UntilSeq {
		c.JSON(http.StatusBadRequest, gin.H{"error": "since_seq must be <= until_seq"})
		return
	}
	if req.UntilSeq-req.SinceSeq+1 > maxResyncRange {
		c.JSON(http.StatusBadRequest, gin.H{"error": "resync range exceeds max 500"})
		return
	}
	cur, err := s.TriggerResync(
		c.Request.Context(),
		chID,
		viewsync.Seq(req.SinceSeq), viewsync.Seq(req.UntilSeq),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"last_received_seq": int64(cur)})
}

func (s *Service) authorizeChannel(c *gin.Context, channelID channel.ID) (string, error) {
	u := identity.UserFrom(c)
	return channelaccess.RequireMemberActor(c.Request.Context(), s.accessAuthorizer(), string(channelID), u.ID)
}
