package app

import (
	"context"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// ---------------------------------------------------------------------------
// WebSocket: client message tail (/ws)
// ---------------------------------------------------------------------------

// wsUpgrader gates the WS handshake. CheckOrigin defends against cross-site
// WebSocket hijacking: /ws authenticates by cookie, so without an Origin check a
// malicious page in the user's browser could open a cross-origin WS riding the
// user's session. Same-origin only; an absent Origin (non-browser client, e.g.
// curl/API) is allowed — it still needs a valid session cookie + channel ACL.
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		return err == nil && u.Host == r.Host
	},
}

func (a *App) handleWS(c *gin.Context) {
	// Auth via cookie.
	token, err := c.Cookie(middleware.SessionCookie)
	if err != nil || token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	userID, ok := middleware.VerifySession(c.Request.Context(), a.db, token)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
		return
	}

	ws, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	// Read the subscribe message from client (ws.js sends {type:"subscribe", channel_id, since_seq}).
	var sub struct {
		Type      string `json:"type"`
		ChannelID string `json:"channel_id"`
		SinceSeq  int64  `json:"since_seq"`
	}
	if err := ws.ReadJSON(&sub); err != nil {
		return
	}
	if sub.Type != "subscribe" {
		_ = ws.WriteJSON(gin.H{"error": "expected subscribe frame"})
		return
	}

	chID := channel.ID(sub.ChannelID)

	// Channel-access ACL: a valid session is NOT enough — the user must be a
	// member of the channel's workspace, the same gate REST goes through
	// (requireChannelAccess). Without this any logged-in user could subscribe to
	// any channel and tail its whole log from seq 0.
	wsID, okWs := a.channelWorkspaceID(c.Request.Context(), string(chID))
	if !okWs || !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		_ = ws.WriteJSON(gin.H{"error": "forbidden"})
		return
	}

	home := a.getHome(chID)
	if home == nil {
		_ = ws.WriteJSON(gin.H{"error": "channel not loaded"})
		return
	}

	// Subscribe to the commit Signal (tap fan-out).
	notify, cancel := home.Subscribe()
	defer cancel()

	cursor := sub.SinceSeq

	// Initial backfill.
	a.wsSendMessages(ws, home, chID, &cursor)

	// Tail loop.
	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-notify:
			if !ok {
				return
			}
			a.wsSendMessages(ws, home, chID, &cursor)
		}
	}
}

func (a *App) wsSendMessages(ws *websocket.Conn, home *platform.Home, chID channel.ID, cursor *int64) {
	for {
		rows, err := home.View().ReadAfterSeq(context.Background(), *cursor, 100)
		if err != nil || len(rows) == 0 {
			return
		}
		for _, r := range rows {
			frame := gin.H{
				"type":       "message",
				"channel_id": string(chID),
				"seq":        r.Seq,
				"envelope":   r.Envelope,
			}
			if err := ws.WriteJSON(frame); err != nil {
				return
			}
			if r.Seq > *cursor {
				*cursor = r.Seq
			}
		}
	}
}

// ---------------------------------------------------------------------------
// WebSocket: compute attach (/compute)
// ---------------------------------------------------------------------------

func (a *App) handleCompute(c *gin.Context) {
	// v1 approach: api-key + channel in query params. App does all auth
	// (key verification + daemon-channel binding check) before handing off
	// to the link acceptor, which receives the pre-authenticated daemonID.
	apiKey := c.Query("key")
	chIDStr := c.Query("channel")

	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "api key required"})
		return
	}
	if chIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}

	chID := channel.ID(chIDStr)

	// Single auth path: verify key + daemon-channel binding. The specific
	// reason (bad key / no binding / db error) stays server-side — a public
	// auth endpoint must not be an oracle, so the client gets one flat 403.
	daemonID, err := a.authAndResolve(apiKey, chID)
	if err != nil {
		a.logger.Warn("compute attach: auth failed", "channel", chID, "err", err)
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	home := a.getHome(chID)
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	// Delegate to the link acceptor with the pre-authenticated daemonID.
	home.ServeAttach(c.Writer, c.Request, daemonID)
}
