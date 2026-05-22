package devicebus

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	serverdaemonbus "github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

const deviceJSONBodyLimit = 64 << 10

// RegisterRoutes mounts the issuance + revoke endpoints. Callers
// must be authenticated upstream (gateway's identity middleware).
func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.POST("/channels/:chID/devices", httperr.MaxBodyBytes(deviceJSONBodyLimit), s.handleIssue)
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
	if err := s.authorizeChannel(c.Request.Context(), c.Param("chID"), u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	notifier := s.bindNotifier()
	if notifier == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "devicebus: bind notifier unavailable"})
		return
	}
	res, err := s.IssueSession(c.Request.Context(), IssueInput{
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
		if errors.Is(err, ErrSessionLimitExceeded) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error()})
			return
		}
		httperr.Internal(c, "devicebus.issue", err)
		return
	}
	// T147 §A-S2 — push the bind frame to the owning daemon. The session
	// row was already INSERTed in state=pending; advance pending → ready
	// only when the daemon ACKs. A failure here leaves the row pending
	// so a follow-up reconciler / retry can pick it up without confusing
	// the client (no half-ready sessions advertised over HTTP).
	bindErr := notifier.Bind(c.Request.Context(), BindInput{
		Session:          res.Session,
		TokenFingerprint: res.TokenFingerprint,
	})
	if bindErr != nil {
		_ = s.Revoke(c.Request.Context(), res.Session.ID)
		_ = notifier.Unbind(c.Request.Context(), UnbindInput{
			Session: res.Session,
			Reason:  "bind_failed",
		})
		status := http.StatusBadGateway
		switch {
		case errors.Is(bindErr, serverdaemonbus.ErrPendingAwaitLimitExceeded):
			status = http.StatusTooManyRequests
		case errors.Is(bindErr, serverdaemonbus.ErrSendAndAwaitTimeout):
			status = http.StatusGatewayTimeout
		case errors.Is(bindErr, serverdaemonbus.ErrConnectionClosed):
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{
			"error":             "bind_device_session_failed",
			"device_session_id": res.Session.ID,
		})
		return
	}
	if err := s.MarkBound(c.Request.Context(), res.Session.ID); err != nil {
		_ = s.Revoke(c.Request.Context(), res.Session.ID)
		_ = notifier.Unbind(c.Request.Context(), UnbindInput{
			Session: res.Session,
			Reason:  "mark_bound_failed",
		})
		httperr.Internal(c, "devicebus.mark_bound", err)
		return
	}
	for _, old := range res.ReplacedSessions {
		_ = notifier.Unbind(c.Request.Context(), UnbindInput{
			Session: old,
			Reason:  "replaced",
		})
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	c.JSON(http.StatusCreated, gin.H{
		"device_session_id": res.Session.ID,
		"token":             res.Token,
		"expires_at":        res.Session.ExpiresAt,
		"token_fingerprint": res.TokenFingerprint,
	})
}

func (s *Service) handleRevoke(c *gin.Context) {
	sid := c.Param("sid")
	u := identity.UserFrom(c)
	row, getErr := s.Get(c.Request.Context(), sid)
	if getErr != nil {
		status := http.StatusInternalServerError
		if getErr == ErrSessionNotFound {
			status = http.StatusNotFound
		}
		if status >= http.StatusInternalServerError {
			httperr.Internal(c, "devicebus.revoke.get", getErr)
			return
		}
		c.JSON(status, gin.H{
			"error":         getErr.Error(),
			"reject_reason": string(kerneldaemonbus.DeviceSessionRejectUnbindSessionUnknown),
		})
		return
	}
	if err := s.authorizeSession(c.Request.Context(), row, u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
		return
	}
	// T147 §A-S2 — best-effort daemon notification BEFORE flipping the
	// authoritative row to revoked. If the daemon ack fails we still
	// proceed with the local Revoke (idempotent): a stale mirror row
	// inside a daemon process is acceptable because every subsequent
	// device WS handshake re-validates against the server's row state.
	if notifier := s.bindNotifier(); notifier != nil {
		_ = notifier.Unbind(c.Request.Context(), UnbindInput{
			Session: row,
			Reason:  "revoked",
		})
	}
	if err := s.Revoke(c.Request.Context(), sid); err != nil {
		httperr.Internal(c, "devicebus.revoke", err)
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
		if status >= http.StatusInternalServerError {
			httperr.Internal(c, "devicebus.get", err)
			return
		}
		c.JSON(status, gin.H{
			"error":         err.Error(),
			"reject_reason": string(kerneldaemonbus.DeviceSessionRejectSessionUnknown),
		})
		return
	}
	u := identity.UserFrom(c)
	if err := s.authorizeSession(c.Request.Context(), row, u.ID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
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

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}

func (s *Service) authorizeSession(ctx context.Context, row Session, userID string) error {
	if row.UserID == userID {
		return nil
	}
	return s.authorizeChannel(ctx, string(row.ChannelID), userID)
}
