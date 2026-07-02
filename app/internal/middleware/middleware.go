// Package middleware holds the app's HTTP interceptor pipeline — the security
// boundary (session auth) and cross-cutting request policy (CORS). It lives in
// its own package on purpose: a change to a request guard is a change to THIS
// package, so it stands out in a diff and is unreachable from business handlers
// except through the exported surface. In particular the authenticated user is
// stamped under a context key private to this package; handlers can only READ it
// via UserID and cannot forge a caller by setting the key themselves.
//
// It is under internal/ because it is the app layer's private boundary, not a
// reusable library — only the app may import it.
package middleware

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// SessionCookie is the canonical session cookie name. It is the contract between
// the session-minting path (login/logout, in the app package) and the
// session-verifying path (here), so it is exported for the minter to reference.
const SessionCookie = "atoll_session"

// ctxKeyUserID is where Auth stamps the authenticated user id. It is private so
// only this package can write it (via Auth) and read it (via UserID) — business
// handlers cannot set it, so a handler cannot forge its caller's identity.
const ctxKeyUserID = "user_id"

// VerifySession is the single authoritative session check: it returns the owning
// user id iff the token names a session that exists and has not expired. Auth
// (the /api guard) and the cookie-authenticated endpoints that run OUTSIDE that
// guard (handleMe, /ws) all funnel through here, so the session-verification SQL
// lives in exactly one place.
func VerifySession(ctx context.Context, db *sql.DB, token string) (userID string, ok bool) {
	if token == "" {
		return "", false
	}
	var expiresAt int64
	err := db.QueryRowContext(ctx,
		`SELECT user_id, expires_at FROM sessions WHERE token = ?`, token,
	).Scan(&userID, &expiresAt)
	if err != nil {
		return "", false
	}
	if time.Now().UnixMilli() > expiresAt {
		return "", false
	}
	return userID, true
}

// Auth is the /api request guard: it requires a valid session cookie and stamps
// the authenticated user id for downstream handlers to read via UserID. A
// missing or invalid session is rejected with 401 and the chain is aborted.
func Auth(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, _ := c.Cookie(SessionCookie)
		userID, ok := VerifySession(c.Request.Context(), db, token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		c.Set(ctxKeyUserID, userID)
		c.Next()
	}
}

// UserID returns the authenticated user id stamped by Auth, or "" if the request
// did not pass through Auth. This is the ONLY way business handlers learn the
// caller's identity.
func UserID(c *gin.Context) string {
	v, _ := c.Get(ctxKeyUserID)
	s, _ := v.(string)
	return s
}

// CORS allows all origins (dev policy). It short-circuits OPTIONS preflight.
func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type,Authorization")
		c.Header("Access-Control-Allow-Credentials", "true")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
