package devicebus

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/server/channelaccess"
	"github.com/wanpengxie/ActOS/server/httperr"
	"github.com/wanpengxie/ActOS/server/identity"
)

// DeviceJSONBodyLimit is the max body size accepted on daemon-create.
// Exported so the gateway-side wrapper (which owns the POST route now)
// can apply the same limit.
const DeviceJSONBodyLimit = 64 << 10

func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	// POST /channels/:chID/daemons (composite create + attach) is owned
	// by gateway (handleCreateDaemonWithAutoBind) so it can auto-bind
	// the channel before the daemon row is issued. POST/.../attach +
	// DELETE/.../attach/:id (detach existing) are owned by gateway too
	// so attach can drive on-the-fly facade install when the daemon is
	// already connected. Devicebus retains the data-layer methods.
	g.GET("/channels/:chID/daemons", s.handleListDaemons)
	// Owner-scoped daemon catalog. Lives under /api/daemons (no :chID).
	// Listing here lets the "+ 新建设备" + "勾选已有 daemon" UI render a
	// single picker across all the owner's daemons regardless of which
	// channels they're attached to.
	g.GET("/daemons", s.handleListOwnerDaemons)
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

// handleListOwnerDaemons returns every daemon owned by the caller plus
// the list of channel ids currently attached to each. Drives the per-
// channel attach UI: the dialog renders one checkbox per daemon with
// `attached_channels` pre-filled so the user sees current state.
func (s *Service) handleListOwnerDaemons(c *gin.Context) {
	u := identity.UserFrom(c)
	rows, err := s.ListDaemonsByOwner(c.Request.Context(), u.ID)
	if err != nil {
		httperr.Internal(c, "devicebus.list_owner_daemons", err)
		return
	}
	type ownerDaemonResp struct {
		DaemonResp
		AttachedChannels []string `json:"attached_channels"`
	}
	out := make([]ownerDaemonResp, 0, len(rows))
	for _, row := range rows {
		attached, _ := s.ListDaemonAttachments(c.Request.Context(), row.ID)
		ids := make([]string, 0, len(attached))
		for _, ch := range attached {
			ids = append(ids, string(ch))
		}
		out = append(out, ownerDaemonResp{
			DaemonResp:       DaemonResponse(row, ""),
			AttachedChannels: ids,
		})
	}
	c.JSON(http.StatusOK, gin.H{"daemons": out})
}

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}
