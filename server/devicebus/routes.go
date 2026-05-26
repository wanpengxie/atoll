package devicebus

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

const deviceJSONBodyLimit = 64 << 10

func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("/channels/:chID/device-actor", httperr.MaxBodyBytes(deviceJSONBodyLimit), s.handleDeprecatedDeviceActor)
	g.DELETE("/channels/:chID/device-actor/:actorID", s.handleDeprecatedDeviceActor)
	g.GET("/channels/:chID/device-actor/:actorID", s.handleDeprecatedDeviceActor)
	g.POST("/channels/:chID/daemons", httperr.MaxBodyBytes(deviceJSONBodyLimit), s.handleCreateDaemon)
	g.GET("/channels/:chID/daemons", s.handleListDaemons)
	g.DELETE("/channels/:chID/daemons/:daemonID", s.handleDeleteDaemon)
}

type createDaemonReq struct {
	Name string `json:"name" binding:"required"`
}

type daemonResp struct {
	ID            string `json:"id"`
	ChannelID     string `json:"channel_id"`
	OwnerID       string `json:"owner_id"`
	Name          string `json:"name"`
	APIKeyPrefix  string `json:"api_key_prefix"`
	Status        string `json:"status"`
	Hostname      string `json:"hostname,omitempty"`
	ProxyVersion  string `json:"proxy_version,omitempty"`
	LastHeartbeat int64  `json:"last_heartbeat,omitempty"`
	CreatedAt     int64  `json:"created_at"`
	APIKey        string `json:"apiKey,omitempty"`
}

func daemonResponse(d Daemon, apiKey string) daemonResp {
	return daemonResp{
		ID:            string(d.ID),
		ChannelID:     string(d.ChannelID),
		OwnerID:       d.OwnerID,
		Name:          d.Name,
		APIKeyPrefix:  d.APIKeyPrefix,
		Status:        d.Status,
		Hostname:      d.Hostname,
		ProxyVersion:  d.ProxyVersion,
		LastHeartbeat: d.LastHeartbeat,
		CreatedAt:     d.CreatedAt,
		APIKey:        apiKey,
	}
}

func (s *Service) handleCreateDaemon(c *gin.Context) {
	var req createDaemonReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	u := identity.UserFrom(c)
	chID := channel.ID(c.Param("chID"))
	if err := s.authorizeChannel(c.Request.Context(), string(chID), u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	row, apiKey, err := s.CreateDaemon(c.Request.Context(), CreateDaemonInput{
		ChannelID: chID,
		OwnerID:   u.ID,
		Name:      req.Name,
	})
	if err != nil {
		httperr.Internal(c, "devicebus.create_daemon", err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	c.JSON(http.StatusCreated, daemonResponse(row, apiKey))
}

func (s *Service) handleListDaemons(c *gin.Context) {
	u := identity.UserFrom(c)
	chID := channel.ID(c.Param("chID"))
	if err := s.authorizeChannel(c.Request.Context(), string(chID), u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	rows, err := s.ListDaemons(c.Request.Context(), chID)
	if err != nil {
		httperr.Internal(c, "devicebus.list_daemons", err)
		return
	}
	out := make([]daemonResp, 0, len(rows))
	for _, row := range rows {
		out = append(out, daemonResponse(row, ""))
	}
	c.JSON(http.StatusOK, gin.H{"daemons": out})
}

func (s *Service) handleDeleteDaemon(c *gin.Context) {
	u := identity.UserFrom(c)
	chID := channel.ID(c.Param("chID"))
	if err := s.authorizeChannel(c.Request.Context(), string(chID), u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	if err := s.DeleteDaemon(c.Request.Context(), chID, placement.DaemonID(c.Param("daemonID"))); err != nil {
		if errors.Is(err, ErrDaemonNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error(), "reject_reason": "daemon_unknown"})
			return
		}
		httperr.Internal(c, "devicebus.delete_daemon", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (s *Service) handleDeprecatedDeviceActor(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{
		"error":         "device actor token endpoints have been retired; use proxy daemon installation flow",
		"reject_reason": "device_actor_tokens_retired",
	})
}

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}
