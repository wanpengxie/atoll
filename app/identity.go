package app

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"golang.org/x/crypto/bcrypt"
)

// sessionDuration is how long a minted session stays valid. Session minting
// lives here (the identity subsystem); session verification and the cookie name
// live in the middleware package (the request guard).
const sessionDuration = 30 * 24 * time.Hour

func (a *App) isWorkspaceMember(ctx context.Context, wsID, userID string) bool {
	var count int
	err := a.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_members WHERE workspace_id = ? AND user_id = ?`,
		wsID, userID,
	).Scan(&count)
	return err == nil && count > 0
}

func (a *App) channelWorkspaceID(ctx context.Context, chID string) (string, bool) {
	var wsID string
	err := a.db.QueryRowContext(ctx,
		`SELECT workspace_id FROM channels WHERE id = ?`, chID,
	).Scan(&wsID)
	return wsID, err == nil
}

func (a *App) requireChannelAccess(c *gin.Context) (string, bool) {
	chID := c.Param("chID")
	userID := middleware.UserID(c)
	wsID, ok := a.channelWorkspaceID(c.Request.Context(), chID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return "", false
	}
	if !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "not a workspace member"})
		return "", false
	}
	return chID, true
}

// ---------------------------------------------------------------------------
// Identity handlers
// ---------------------------------------------------------------------------

func (a *App) handleRegister(c *gin.Context) {
	var req struct {
		Email       string `json:"email"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if req.Email == "" || req.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email and password required"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash failed"})
		return
	}

	userID := uuid.NewString()
	now := time.Now().UnixMilli()

	_, err = a.db.ExecContext(c.Request.Context(),
		`INSERT INTO users (id, email, password, display_name, created_at) VALUES (?,?,?,?,?)`,
		userID, req.Email, string(hash), req.DisplayName, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			c.JSON(http.StatusConflict, gin.H{"error": "email already registered"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create user failed"})
		return
	}

	// Auto-login.
	a.setSession(c, userID)
	c.JSON(http.StatusCreated, gin.H{
		"id":           userID,
		"email":        req.Email,
		"display_name": req.DisplayName,
	})
}

func (a *App) handleLogin(c *gin.Context) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	var userID, hash, displayName string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, password, display_name FROM users WHERE email = ?`, req.Email,
	).Scan(&userID, &hash, &displayName)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	a.setSession(c, userID)
	c.JSON(http.StatusOK, gin.H{
		"id":           userID,
		"email":        req.Email,
		"display_name": displayName,
	})
}

func (a *App) handleLogout(c *gin.Context) {
	token, err := c.Cookie(middleware.SessionCookie)
	if err == nil && token != "" {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM sessions WHERE token = ?`, token,
		)
	}
	c.SetCookie(middleware.SessionCookie, "", -1, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleMe(c *gin.Context) {
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

	var email, displayName string
	err = a.db.QueryRowContext(c.Request.Context(),
		`SELECT email, display_name FROM users WHERE id = ?`, userID,
	).Scan(&email, &displayName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "user not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":           userID,
		"email":        email,
		"display_name": displayName,
	})
}

func (a *App) handleVerificationIssue(c *gin.Context) {
	// Stub: v1 skips email verification.
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) setSession(c *gin.Context, userID string) {
	token := uuid.NewString()
	now := time.Now().UnixMilli()
	expiresAt := now + sessionDuration.Milliseconds()

	_, _ = a.db.ExecContext(c.Request.Context(),
		`INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)`,
		token, userID, now, expiresAt,
	)
	c.SetCookie(middleware.SessionCookie, token, int(sessionDuration.Seconds()), "/", "", false, true)
}
