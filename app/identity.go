package app

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
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
		writeAPIError(c, http.StatusServiceUnavailable, contract.CodeChannelUnavailable, "channel directory unavailable")
		return "", false
	}
	if !exists {
		writeAPIError(c, http.StatusNotFound, contract.CodeChannelNotFound, "channel not found")
		return "", false
	}
	return chID, true
}

func (a *App) requireChannelMember(c *gin.Context) (string, bool) {
	chID, _, ok := a.requireChannelMemberActor(c)
	return chID, ok
}

// resolveMember is the single carrier of the "active channel member" ruling:
// membership is exactly the principal resolving in the membrane's roster.
// Every consumer of that ruling — the gin guards, sysop_forward's memberGate,
// the verb predicates/qualifiers, the observer classifier and the routing
// grant — answers through this one function so the ruling can never fork.
func resolveMember(ctx context.Context, bundle channelhost.Bundle, principal string) (actor.ActorID, bool, error) {
	return bundle.View().ResolvePrincipal(ctx, principal)
}

func (a *App) requireChannelMemberActor(c *gin.Context) (string, actor.ActorID, bool) {
	chID := c.Param("chID")
	bundle, err := a.acquireBundle(c.Request.Context(), channel.ID(chID))
	if errors.Is(err, errChannelNotFound) {
		writeAPIError(c, http.StatusNotFound, contract.CodeChannelNotFound, "channel not found")
		return "", "", false
	}
	if err != nil {
		writeAPIError(c, http.StatusServiceUnavailable, contract.CodeChannelUnavailable, "channel unavailable")
		return "", "", false
	}
	id, found, err := resolveMember(c.Request.Context(), bundle, middleware.UserID(c))
	if err != nil {
		writeAPIError(c, http.StatusServiceUnavailable, contract.CodeChannelUnavailable, "channel unavailable")
		return "", "", false
	}
	if !found {
		writeAPIError(c, http.StatusForbidden, contract.CodeForbidden, "active channel membership required")
		return "", "", false
	}
	return chID, id, true
}

// ---------------------------------------------------------------------------
// Identity handlers
// ---------------------------------------------------------------------------

func (a *App) handleRegister(c *gin.Context) {
	var req contract.RegisterRequest
	if !decodeRequest(c, &req) {
		return
	}
	if req.Email == "" || req.Password == "" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "email and password required")
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "password processing failed")
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
			writeAPIError(c, http.StatusConflict, contract.CodeAlreadyExists, "email already registered")
			return
		}
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "create user failed")
		return
	}

	// Auto-login.
	a.setSession(c, userID)
	c.JSON(http.StatusCreated, contract.Principal{ID: userID, Email: req.Email, DisplayName: req.DisplayName})
}

func (a *App) handleLogin(c *gin.Context) {
	var req contract.LoginRequest
	if !decodeRequest(c, &req) {
		return
	}

	var userID, hash, displayName string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, password, display_name FROM users WHERE email = ?`, req.Email,
	).Scan(&userID, &hash, &displayName)
	if err != nil {
		writeAPIError(c, http.StatusUnauthorized, contract.CodeInvalidCredentials, "invalid credentials")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
		writeAPIError(c, http.StatusUnauthorized, contract.CodeInvalidCredentials, "invalid credentials")
		return
	}

	a.setSession(c, userID)
	c.JSON(http.StatusOK, contract.Principal{ID: userID, Email: req.Email, DisplayName: displayName})
}

func (a *App) handleLogout(c *gin.Context) {
	token, err := c.Cookie(middleware.SessionCookie)
	if err == nil && token != "" {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM sessions WHERE token = ?`, token,
		)
	}
	c.SetCookie(middleware.SessionCookie, "", -1, "/", "", false, true)
	c.JSON(http.StatusOK, contract.OK{OK: true})
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
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "user lookup failed")
		return
	}

	c.JSON(http.StatusOK, contract.Principal{ID: userID, Email: email, DisplayName: displayName})
}

func (a *App) handleVerificationIssue(c *gin.Context) {
	// Stub: v1 skips email verification.
	c.JSON(http.StatusOK, contract.OK{OK: true})
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
