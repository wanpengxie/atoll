package devicebus

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

// DeviceJSONBodyLimit is the max body size accepted on daemon-create.
// Exported so the gateway-side wrapper (which owns the POST route now)
// can apply the same limit.
const DeviceJSONBodyLimit = 64 << 10

func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	// POST /channels/:chID/daemons is owned by gateway (handleCreateDaemonWithAutoBind)
	// so the create call can lazy-bind the channel to a cloud daemon
	// before the proxy daemon row is issued. devicebus.Service still
	// exposes CreateDaemon for that gateway handler to call.
	g.GET("/channels/:chID/daemons", s.handleListDaemons)
	g.DELETE("/channels/:chID/daemons/:daemonID", s.handleDeleteDaemon)
}

// DaemonResp is the JSON shape returned by daemons endpoints. Exported so
// gateway-side wrappers (auto-bind on POST /channels/:chID/daemons) can
// reuse it without duplicating field names.
type DaemonResp struct {
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

// DaemonResponse maps a Daemon row plus the single-display apiKey into
// the JSON envelope the daemons API returns. Exported alongside DaemonResp
// so gateway can call CreateDaemon directly without re-serializing.
func DaemonResponse(d Daemon, apiKey string) DaemonResp {
	return DaemonResp{
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
	out := make([]DaemonResp, 0, len(rows))
	for _, row := range rows {
		out = append(out, DaemonResponse(row, ""))
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

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}
