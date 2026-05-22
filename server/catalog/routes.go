package catalog

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

const catalogJSONBodyLimit = 64 << 10

// PlacementHook is the optional integration point that catalog calls
// after creating / updating a channel — typically the gateway wires
// it to placements.CreateChannel + daemonbus update_members frames so
// the daemon side stays in sync with catalog (covers T1.9).
//
// Implementations MUST be idempotent — catalog calls them after the
// channel + member rows are durably committed, so retrying is safe.
type PlacementHook interface {
	OnChannelCreated(ctx context.Context, channel Channel, members []ChannelMember) error
	OnChannelMembersChanged(ctx context.Context, channelID string, adds []ChannelMember, removes []string) error
}

// RegisterRoutes mounts the catalog endpoints. ident is required so
// handlers can extract user_id from cookie. hook may be nil — when
// nil the daemonbus-sync path is skipped.
func (s *Service) RegisterRoutes(g *gin.RouterGroup, ident *identity.Service) {
	g.GET("/workspaces", s.handleListWorkspaces)
	g.POST("/workspaces", httperr.MaxBodyBytes(catalogJSONBodyLimit), s.handleCreateWorkspace)
	g.GET("/workspaces/:wsID", s.handleGetWorkspace)
	g.GET("/workspaces/:wsID/channels", s.handleListChannels)
	g.POST("/workspaces/:wsID/channels", httperr.MaxBodyBytes(catalogJSONBodyLimit), s.handleCreateChannel)
	g.GET("/channels/:chID", s.handleGetChannel)
	g.GET("/channels/:chID/members", s.handleListChannelMembers)
	g.POST("/channels/:chID/members", httperr.MaxBodyBytes(catalogJSONBodyLimit), s.handleAddChannelMember)
	g.DELETE("/channels/:chID/members/:uid", s.handleRemoveChannelMember)
}

func (s *Service) handleListWorkspaces(c *gin.Context) {
	u := identity.UserFrom(c)
	ws, err := s.ListWorkspaces(c.Request.Context(), u.ID)
	if err != nil {
		httperr.Internal(c, "catalog.list_workspaces", err)
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
		httperr.Respond(c, "catalog.create_workspace", httpStatusFor(err), err)
		return
	}
	c.JSON(http.StatusCreated, ws)
}

func (s *Service) handleGetWorkspace(c *gin.Context) {
	u := identity.UserFrom(c)
	ws, err := s.GetWorkspace(c.Request.Context(), c.Param("wsID"), u.ID)
	if err != nil {
		httperr.Respond(c, "catalog.get_workspace", httpStatusFor(err), err)
		return
	}
	c.JSON(http.StatusOK, ws)
}

func (s *Service) handleListChannels(c *gin.Context) {
	u := identity.UserFrom(c)
	chs, err := s.ListChannels(c.Request.Context(), c.Param("wsID"), u.ID)
	if err != nil {
		httperr.Respond(c, "catalog.list_channels", httpStatusFor(err), err)
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
		httperr.Respond(c, "catalog.create_channel", httpStatusFor(err), err)
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
		httperr.Respond(c, "catalog.get_channel", httpStatusFor(err), err)
		return
	}
	c.JSON(http.StatusOK, ch)
}

func (s *Service) handleListChannelMembers(c *gin.Context) {
	u := identity.UserFrom(c)
	if _, err := s.GetChannelMember(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		httperr.Respond(c, "catalog.list_channel_members.auth", httpStatusFor(err), err)
		return
	}
	members, err := s.ListChannelMembers(c.Request.Context(), c.Param("chID"))
	if err != nil {
		httperr.Internal(c, "catalog.list_channel_members", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

type addMemberReq struct {
	UserID        string `json:"user_id"             binding:"required"`
	MemberActorID string `json:"member_actor_id"`
	Role          string `json:"role"`
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
		httperr.Respond(c, "catalog.add_channel_member.auth", httpStatusFor(err), err)
		return
	}
	m, err := s.AddChannelMember(c.Request.Context(), c.Param("chID"), NewMember(req))
	if err != nil {
		httperr.Respond(c, "catalog.add_channel_member", httpStatusFor(err), err)
		return
	}
	c.JSON(http.StatusCreated, m)
}

func (s *Service) handleRemoveChannelMember(c *gin.Context) {
	u := identity.UserFrom(c)
	if _, err := s.GetChannelMember(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		httperr.Respond(c, "catalog.remove_channel_member.auth", httpStatusFor(err), err)
		return
	}
	if err := s.RemoveChannelMember(c.Request.Context(), c.Param("chID"), c.Param("uid")); err != nil {
		httperr.Internal(c, "catalog.remove_channel_member", err)
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
