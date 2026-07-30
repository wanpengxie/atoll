package app

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
)

// ---------------------------------------------------------------------------
// WebSocket: the subject's single gateway link (/ws)
// ---------------------------------------------------------------------------
//
// gateway 期 S3 (连接模型勘误期): the ws transport + frame protocol live in the
// drivers/gateway伞包 (the connector does byte↔IR, the gateway core does the session
// cross). 连接即人: the app membrane authenticates ONLY session→principal — there is NO
// connection-level channel query and NO connection-level channel ACL. A connection
// subscribes to the实时动态 of ALL the person's合法频道 (户籍 ∪ 读资格), resolved LIVE by
// the gateway's injected EntitlementResolver per frame/batch; "which window is open" is
// client内政. The single-link law (红线 10) is unchanged: one ws carries the feed DOWN
// (for every eligible channel) and the standard frames UP (each naming its channel_id).
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

	// Hand off to the connector: it upgrades the ws and runs the gateway session.
	// principal = the session's user id; the gateway resolves eligibility live.
	a.wsGateway.ServeWeb(c.Writer, c.Request, userID)
}

// ---------------------------------------------------------------------------
// WebSocket: compute attach (/compute)
// ---------------------------------------------------------------------------

func (a *App) handleCompute(c *gin.Context) {
	if c.Query("key") != "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "query credentials are not accepted"})
		return
	}
	auth := c.GetHeader("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") ||
		strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed bearer authorization"})
		return
	}
	key := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	daemonID, status := a.resolveDaemonCredential(c.Request.Context(), key)
	if status != http.StatusOK {
		if status == http.StatusServiceUnavailable {
			c.Header("Retry-After", "5")
		}
		c.JSON(status, gin.H{"error": http.StatusText(status)})
		return
	}
	a.daemonHost.Serve(c.Writer, c.Request, daemonID)
}
