package devicebus

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

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

// daemonLivenessTTL bounds how stale the persisted daemon liveness signal
// (last_heartbeat) may be before the server stops treating the daemon — and
// therefore the proxy-facade reachability it cached — as live.
//
// channel-lifecycle-reconcile-architecture.md §4/§6 step4: facade_state /
// ready_state are persisted DISPLAY caches, never API authority. `callable`
// is a realtime derivation: it requires the daemon to be freshly heartbeating
// (within this TTL). Once the heartbeat lapses, the cached facade/ready
// values are stale and callable collapses to false — the server stops
// claiming an unreachable actor is callable even if no disconnect/reset frame
// ever landed (crash, lost callback). Proxy heartbeat interval is 25s
// (adapters/proxy/daemon DefaultHeartbeatInterval); ~3 missed beats.
const daemonLivenessTTL = 90 * time.Second

// daemonLivenessTTLMs is daemonLivenessTTL in milliseconds (last_heartbeat is
// stored as unix-ms).
const daemonLivenessTTLMs = int64(daemonLivenessTTL / time.Millisecond)

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
	start := time.Now()
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
	s.log.Info("devicebus.list_daemons_completed",
		"channel_id", string(chID),
		"count", len(out),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	c.JSON(http.StatusOK, gin.H{"daemons": out})
}

// handleListOwnerDaemons returns every daemon owned by the caller plus:
//   - attached_channels: the channels currently linked (drives the
//     per-channel attach checkboxes)
//   - hosted_actors:     the adapter manifest from the last ready frame
//     (drives the 我的设备 page adapter chips)
func (s *Service) handleListOwnerDaemons(c *gin.Context) {
	start := time.Now()
	u := identity.UserFrom(c)
	rows, err := s.ListDaemonsByOwner(c.Request.Context(), u.ID)
	if err != nil {
		httperr.Internal(c, "devicebus.list_owner_daemons", err)
		return
	}
	type hostedActorResp struct {
		ActorID              string          `json:"actor_id"`
		CapabilitySet        any             `json:"capability_set,omitempty"`
		ActiveChannels       []string        `json:"active_channels"`
		RouteActive          bool            `json:"route_active"`
		FacadeInstalled      bool            `json:"facade_installed"`
		FacadeState          string          `json:"facade_state"`
		FacadeDetail         string          `json:"facade_detail,omitempty"`
		FacadeUpdatedAt      int64           `json:"facade_updated_at"`
		DaemonOnline         bool            `json:"daemon_online"`
		ActorReadinessReady  bool            `json:"actor_readiness_ready"`
		ActorReadinessState  string          `json:"actor_readiness_state"`
		Callable             bool            `json:"callable"`
		Ready                bool            `json:"ready"`
		ReadyState           string          `json:"ready_state"`
		ReadyReason          string          `json:"ready_reason"`
		ReadyDetail          json.RawMessage `json:"ready_detail,omitempty"`
		ReadinessCheckedAt   int64           `json:"readiness_checked_at"`
		LastReadyAt          int64           `json:"last_ready_at"`
		LastStateChangeAt    int64           `json:"last_state_change_at"`
		OperationalStateNote string          `json:"operational_state_note,omitempty"`
	}
	type ownerDaemonResp struct {
		DaemonResp
		AttachedChannels []string          `json:"attached_channels"`
		HostedActors     []hostedActorResp `json:"hosted_actors"`
	}
	now := s.nowMs()
	out := make([]ownerDaemonResp, 0, len(rows))
	for _, row := range rows {
		attached, _ := s.ListDaemonAttachments(c.Request.Context(), row.ID)
		ids := make([]string, 0, len(attached))
		for _, ch := range attached {
			ids = append(ids, string(ch))
		}
		hosted, _ := s.ListDaemonHostedActors(c.Request.Context(), row.ID)
		hostedResp := make([]hostedActorResp, 0, len(hosted))
		for _, h := range hosted {
			var cap any
			if len(h.CapabilitySet) > 0 {
				_ = json.Unmarshal(h.CapabilitySet, &cap)
			}
			activeChannels := make([]string, 0, len(h.ActiveChannels))
			for _, chID := range h.ActiveChannels {
				activeChannels = append(activeChannels, string(chID))
			}
			// channel-lifecycle-reconcile §4/§6 step4 — callable is a realtime
			// derivation, not a read of persisted authority. facade_state /
			// ready_state below are DISPLAY caches; they only contribute to
			// callable while the daemon liveness signal is fresh (heartbeat
			// within daemonLivenessTTL). A stale heartbeat collapses callable to
			// false regardless of what the cached facade/ready columns still say
			// — closing 现象2 (sticky "可调用" after the device is gone).
			daemonLive := row.Status == "online" && (now-row.LastHeartbeat) <= daemonLivenessTTLMs
			actorReady := h.ReadyState == "ready"
			routeActive := len(activeChannels) > 0
			facadeInstalled := h.FacadeState == "installed"
			hostedResp = append(hostedResp, hostedActorResp{
				ActorID:              string(h.ActorID),
				CapabilitySet:        cap,
				ActiveChannels:       activeChannels,
				RouteActive:          routeActive,
				FacadeInstalled:      facadeInstalled,
				FacadeState:          h.FacadeState,
				FacadeDetail:         h.FacadeDetail,
				FacadeUpdatedAt:      h.FacadeUpdatedAt,
				DaemonOnline:         daemonLive,
				ActorReadinessReady:  actorReady,
				ActorReadinessState:  h.ReadyState,
				Callable:             daemonLive && routeActive && facadeInstalled && actorReady,
				Ready:                actorReady,
				ReadyState:           h.ReadyState,
				ReadyReason:          h.ReadyReason,
				ReadyDetail:          append(json.RawMessage(nil), h.ReadyDetail...),
				ReadinessCheckedAt:   h.ReadinessCheckedAt,
				LastReadyAt:          h.LastReadyAt,
				LastStateChangeAt:    h.LastStateChangeAt,
				OperationalStateNote: "display cache; callable is a realtime derivation (daemon heartbeat fresh ∧ route ∧ facade ∧ ready); actor.status is authoritative for a single actor",
			})
		}
		out = append(out, ownerDaemonResp{
			DaemonResp:       DaemonResponse(row, ""),
			AttachedChannels: ids,
			HostedActors:     hostedResp,
		})
	}
	s.log.Info("devicebus.list_owner_daemons_completed",
		"owner_id", u.ID,
		"count", len(out),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	c.JSON(http.StatusOK, gin.H{"daemons": out})
}

func (s *Service) authorizeChannel(ctx context.Context, channelID, userID string) error {
	return channelaccess.Require(ctx, s.accessAuthorizer(), channelID, userID)
}
