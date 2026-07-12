package app

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ---------------------------------------------------------------------------
// WebSocket: the subject's single gateway link (/ws)
// ---------------------------------------------------------------------------
//
// gateway 期 S3: the ws transport + frame protocol moved to the drivers/gateway伞包
// (the connector does byte↔IR, the gateway core does the session cross). The app
// keeps only the MEMBRANE half — authenticate the session, enforce the workspace
// (tail) ACL, resolve the open Home — then hand the upgraded-pending request to the
// injected connector (WSGateway). Channel membership (write eligibility) is the
// gateway's own resolve; a workspace member who is not a channel member gets a
// tail-only session whose business frames are refused not_member. The single-link
// law (红线 10) is unchanged: one ws carries the feed DOWN and the standard frames UP.
func (a *App) handleWS(c *gin.Context) {
	if a.wsGateway == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gateway unavailable"})
		return
	}
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

	chID := channel.ID(c.Query("channel"))
	if chID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}

	// Channel-access ACL (tail eligibility): a valid session is not enough — the
	// user must be a member of the channel's workspace, the same gate REST goes
	// through. This is broader than channel membership; the gateway resolves the
	// narrower channel membership (write eligibility) itself.
	wsID, okWs := a.channelWorkspaceID(c.Request.Context(), string(chID))
	if !okWs || !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	home := a.getHome(chID)
	if home == nil {
		// Access passed (workspace member of an existing channel) but the universe is
		// not open (A-P8): honestly "unavailable" (retryable), not "not found".
		a.logger.Warn("channel unavailable: directory has channel but its home is not open",
			"channel", string(chID))
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return
	}

	// Hand off to the connector: it upgrades the ws and runs the gateway session
	// cross. principal = the session's user id (the door resolves it to the subject).
	a.wsGateway.ServeWeb(c.Writer, c.Request, home, chID, userID)
}

// wsSubmitErrCode maps a Submit error to the message frame's error code (期12 v0.4
// P0-2 的honest arms). Retained until S5 folds the WriteRejectedError public form —
// exposed to WSSubmitErrCodeForTest.
func wsSubmitErrCode(err error) (code, detail string, internal bool) {
	var wr *platform.WriteRejectedError
	switch {
	case errors.As(err, &wr):
		return wr.Reason, wr.Detail, false
	case errors.Is(err, platform.ErrNotMember):
		return "not_member", "not a channel member", false
	case errors.Is(err, platform.ErrCellUnavailable):
		return "unavailable", "subject cell unavailable — retry", false
	case errors.Is(err, platform.ErrClosed):
		return "closed", "channel home is closed", false
	default:
		return "internal", "", true
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

	home := a.homeOrError(c, chID)
	if home == nil {
		return
	}

	// Delegate to the link acceptor with the pre-authenticated daemonID.
	home.ServeAttach(c.Writer, c.Request, daemonID)
}
