package app

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// ---------------------------------------------------------------------------
// WebSocket: client message tail (/ws)
// ---------------------------------------------------------------------------

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (a *App) handleWS(c *gin.Context) {
	// Auth via cookie.
	token, err := c.Cookie(middleware.SessionCookie)
	if err != nil || token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	if _, ok := middleware.VerifySession(c.Request.Context(), a.db, token); !ok {
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
	home := a.getHome(chID)
	if home == nil {
		_ = ws.WriteJSON(gin.H{"error": "channel not loaded"})
		return
	}

	// Subscribe to the commit Signal (tap fan-out).
	notify, cancel := home.Subscribe()
	defer cancel()

	cursor := sub.SinceSeq
	gw := homeGateway(chID, home)

	// Initial backfill.
	a.wsSendMessages(ws, gw, chID, &cursor)

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
			a.wsSendMessages(ws, gw, chID, &cursor)
		}
	}
}

func (a *App) wsSendMessages(ws *websocket.Conn, gw gateway, chID channel.ID, cursor *int64) {
	for {
		rows, err := gw.ListMessages(context.Background(), *cursor, 100)
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

	// Single auth path: verify key + daemon-channel binding.
	daemonID, err := a.authAndResolve(apiKey, chID)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
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
