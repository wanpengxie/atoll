package app

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"
)

// sessionDuration is how long a minted session stays valid. Session minting
// lives here (the identity subsystem); session verification and the cookie name
// live in the middleware package (the request guard).
const sessionDuration = 30 * 24 * time.Hour

// bcryptCost is the password-hash work factor. Production always runs
// bcrypt.DefaultCost; it is a var ONLY so the test seam
// (SetBcryptCostForTest) can drop it to MinCost — under the race detector a
// single DefaultCost hash+compare costs ~1.7s of pure CPU, which multiplied
// by every register+login fixture was the app test suite's single biggest
// time sink (34 tests × ~1.7s). Nothing outside export_test may write it.
var bcryptCost = bcrypt.DefaultCost

func (a *App) channelExists(ctx context.Context, chID string) (bool, error) {
	var exists bool
	err := a.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM channels WHERE id = ? AND status='present')`, chID,
	).Scan(&exists)
	return exists, err
}

func (a *App) requireChannelAccess(c *gin.Context) (string, bool) {
	chID := c.Param("chID")
	exists, err := a.channelExists(c.Request.Context(), chID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel directory unavailable"})
		return "", false
	}
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return "", false
	}
	return chID, true
}

func (a *App) requireChannelMember(c *gin.Context) (string, bool) {
	chID, _, ok := a.requireChannelMemberActor(c)
	return chID, ok
}

func (a *App) requireChannelMemberActor(c *gin.Context) (string, actor.ActorID, bool) {
	chID := c.Param("chID")
	bundle, err := a.acquireBundle(c.Request.Context(), channel.ID(chID))
	if errors.Is(err, errChannelNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return "", "", false
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return "", "", false
	}
	id, found, err := bundle.View().ResolvePrincipal(c.Request.Context(), middleware.UserID(c))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return "", "", false
	}
	if !found {
		c.JSON(http.StatusForbidden, gin.H{"error": "active channel membership required"})
		return "", "", false
	}
	return chID, id, true
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

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
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

// handleMe returns the current user's profile. The route carries middleware.Auth,
// so the session is already verified and the user id stamped by the time we get
// here — session checking lives in exactly one place (the guard), never inline.
func (a *App) handleMe(c *gin.Context) {
	userID := middleware.UserID(c)

	var email, displayName string
	err := a.db.QueryRowContext(c.Request.Context(),
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
