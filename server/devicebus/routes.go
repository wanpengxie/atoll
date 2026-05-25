package devicebus

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

const deviceJSONBodyLimit = 64 << 10
const defaultActorID actor.ActorID = "tool:xhs-adapter"

func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("/channels/:chID/device-actor", httperr.MaxBodyBytes(deviceJSONBodyLimit), s.handleRegisterActor)
	g.DELETE("/channels/:chID/device-actor/:actorID", s.handleRevokeActor)
	g.GET("/channels/:chID/device-actor/:actorID", s.handleGetActor)
}

type registerReq struct {
	ActorID    string `json:"actor_id"`
	DeviceID   string `json:"device_id" binding:"required"`
	DeviceType string `json:"device_type"`
	DaemonID   string `json:"daemon_id" binding:"required"`
}

func (s *Service) handleRegisterActor(c *gin.Context) {
	var req registerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u := identity.UserFrom(c)
	if err := s.authorizeChannel(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	actorID := actor.ActorID(req.ActorID)
	if actorID == "" {
		actorID = defaultActorID
	}
	res, err := s.RegisterActor(c.Request.Context(), RegisterInput{
		ActorID:    actorID,
		DeviceID:   req.DeviceID,
		DeviceType: req.DeviceType,
		ChannelID:  channel.ID(c.Param("chID")),
		UserID:     u.ID,
		DaemonID:   placement.DaemonID(req.DaemonID),
	})
	if err != nil {
		if errors.Is(err, ErrDeviceTypeUnsupported) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"error":         err.Error(),
				"reject_reason": "device_type_invalid",
			})
			return
		}
		httperr.Internal(c, "devicebus.register_actor", err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	c.JSON(http.StatusCreated, gin.H{
		"actor_id":          string(res.Registration.ActorID),
		"token":             res.Token,
		"expires_at":        res.Registration.ExpiresAt,
		"token_fingerprint": res.TokenFingerprint,
	})
}

func (s *Service) handleRevokeActor(c *gin.Context) {
	u := identity.UserFrom(c)
	row, err := s.GetActor(c.Request.Context(), channel.ID(c.Param("chID")), actor.ActorID(c.Param("actorID")))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrRegistrationNotFound) {
			status = http.StatusNotFound
		}
		if status >= http.StatusInternalServerError {
			httperr.Internal(c, "devicebus.revoke_actor.get", err)
			return
		}
		c.JSON(status, gin.H{"error": err.Error(), "reject_reason": "actor_registration_unknown"})
		return
	}
	if err := s.authorizeActor(c.Request.Context(), row, u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	if err := s.RevokeActor(c.Request.Context(), row.ChannelID, row.ActorID); err != nil && !errors.Is(err, ErrRegistrationNotFound) {
		httperr.Internal(c, "devicebus.revoke_actor", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "revoked"})
}

func (s *Service) handleGetActor(c *gin.Context) {
	row, err := s.GetActor(c.Request.Context(), channel.ID(c.Param("chID")), actor.ActorID(c.Param("actorID")))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrRegistrationNotFound) {
			status = http.StatusNotFound
		}
		if status >= http.StatusInternalServerError {
			httperr.Internal(c, "devicebus.get_actor", err)
			return
		}
		c.JSON(status, gin.H{"error": err.Error(), "reject_reason": "actor_registration_unknown"})
		return
	}
	u := identity.UserFrom(c)
	if err := s.authorizeActor(c.Request.Context(), row, u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"actor_id":    string(row.ActorID),
		"channel_id":  string(row.ChannelID),
		"daemon_id":   string(row.DaemonID),
		"device_id":   row.DeviceID,
		"device_type": row.DeviceType,
		"expires_at":  row.ExpiresAt,
		"created_at":  row.CreatedAt,
	})
}

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}

func (s *Service) authorizeActor(ctx context.Context, row ActorRegistration, userID string) error {
	if row.UserID == userID {
		return nil
	}
	return s.authorizeChannel(ctx, string(row.ChannelID), userID)
}
