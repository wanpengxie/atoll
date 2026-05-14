package catalog

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/server/identity"
)

// PlacementHook is the optional integration point that catalog calls
// after creating / updating a channel — typically the gateway wires
// it to placements.CreateChannel + daemonbus update_members frames so
// the daemon side stays in sync with catalog (covers T1.9).
//
// Implementations MUST be idempotent — catalog calls them after the
// channel + member rows are durably committed, so retrying is safe.
type PlacementHook interface {
	OnChannelCreated(ctx ctxLike, channel Channel, members []ChannelMember) error
	OnChannelMembersChanged(ctx ctxLike, channelID string, adds []ChannelMember, removes []string) error
}

// ctxLike is a tiny shim so PlacementHook implementations can take a
// context.Context without forcing this file to import "context"
// twice; gin's Context implements it.
type ctxLike interface {
	Deadline() (deadline interface{}, ok bool)
}

// RegisterRoutes mounts the catalog endpoints. ident is required so
// handlers can extract user_id from cookie. hook may be nil — when
// nil the daemonbus-sync path is skipped.
func (s *Service) RegisterRoutes(g *gin.RouterGroup, ident *identity.Service) {
	g.GET("/workspaces", s.handleListWorkspaces)
	g.POST("/workspaces", s.handleCreateWorkspace)
	g.GET("/workspaces/:wsID", s.handleGetWorkspace)
	g.GET("/workspaces/:wsID/channels", s.handleListChannels)
	g.POST("/workspaces/:wsID/channels", s.handleCreateChannel)
	g.GET("/channels/:chID", s.handleGetChannel)
	g.GET("/channels/:chID/members", s.handleListChannelMembers)
	g.POST("/channels/:chID/members", s.handleAddChannelMember)
	g.DELETE("/channels/:chID/members/:uid", s.handleRemoveChannelMember)
}

func (s *Service) handleListWorkspaces(c *gin.Context) {
	u := identity.UserFrom(c)
	ws, err := s.ListWorkspaces(c.Request.Context(), u.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"workspaces": ws})
}

type createWorkspaceReq struct {
	Name string `json:"name" binding:"required"`
}

func (s *Service) handleCreateWorkspace(c *gin.Context) {
	var req createWorkspaceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)
	ws, err := s.CreateWorkspace(c.Request.Context(), CreateWorkspaceInput{
		Name: req.Name, OwnerID: u.ID,
	})
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, ws)
}

func (s *Service) handleGetWorkspace(c *gin.Context) {
	u := identity.UserFrom(c)
	ws, err := s.GetWorkspace(c.Request.Context(), c.Param("wsID"), u.ID)
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ws)
}

func (s *Service) handleListChannels(c *gin.Context) {
	u := identity.UserFrom(c)
	chs, err := s.ListChannels(c.Request.Context(), c.Param("wsID"), u.ID)
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channels": chs})
}

type createChannelReq struct {
	Name string `json:"name" binding:"required"`
	Type string `json:"type"`
	// MemberUserIDs adds the listed user IDs to the channel beyond
	// the creator.
	MemberUserIDs []string `json:"member_user_ids"`
}

func (s *Service) handleCreateChannel(c *gin.Context) {
	var req createChannelReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)
	members := make([]NewMember, 0, len(req.MemberUserIDs))
	for _, uid := range req.MemberUserIDs {
		members = append(members, NewMember{UserID: uid, Role: "member"})
	}
	ch, all, err := s.CreateChannel(c.Request.Context(), CreateChannelInput{
		WorkspaceID: c.Param("wsID"),
		Name:        req.Name,
		Type:        req.Type,
		CreatorID:   u.ID,
		Members:     members,
	})
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"channel": ch,
		"members": all,
	})
}

func (s *Service) handleGetChannel(c *gin.Context) {
	u := identity.UserFrom(c)
	ch, _, err := s.GetChannel(c.Request.Context(), c.Param("chID"), u.ID)
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ch)
}

func (s *Service) handleListChannelMembers(c *gin.Context) {
	u := identity.UserFrom(c)
	if _, err := s.GetChannelMember(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	members, err := s.ListChannelMembers(c.Request.Context(), c.Param("chID"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

type addMemberReq struct {
	UserID           string `json:"user_id"             binding:"required"`
	ActorIDInChannel string `json:"actor_id_in_channel"`
	Role             string `json:"role"`
}

func (s *Service) handleAddChannelMember(c *gin.Context) {
	var req addMemberReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)
	// Only existing channel members can add.
	if _, err := s.GetChannelMember(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	m, err := s.AddChannelMember(c.Request.Context(), c.Param("chID"), NewMember{
		UserID:           req.UserID,
		ActorIDInChannel: req.ActorIDInChannel,
		Role:             req.Role,
	})
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, m)
}

func (s *Service) handleRemoveChannelMember(c *gin.Context) {
	u := identity.UserFrom(c)
	if _, err := s.GetChannelMember(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	if err := s.RemoveChannelMember(c.Request.Context(), c.Param("chID"), c.Param("uid")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "removed"})
}

// httpStatusFor maps catalog errors to HTTP status.
func httpStatusFor(err error) int {
	switch {
	case errors.Is(err, ErrInvalidName):
		return http.StatusBadRequest
	case errors.Is(err, ErrNotWorkspaceMember), errors.Is(err, ErrNotChannelMember):
		return http.StatusForbidden
	case errors.Is(err, ErrWorkspaceNotFound), errors.Is(err, ErrChannelNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrMemberExists):
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}
