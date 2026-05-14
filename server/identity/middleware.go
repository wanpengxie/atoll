package identity

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// CookieName is the cookie key carrying the raw session token.
const CookieName = "coagent_session"

// ginUserKey is the gin.Context key for the resolved user (set by
// AuthMiddleware, read by handlers).
const ginUserKey = "coagent.user"

// contextUserKey is a private type used as the context.Context key
// so it can't collide with other packages' values (staticcheck
// SA1029). Used by WithUserContext / UserFromContext.
type contextUserKey struct{}

// ExtractTokenFromRequest returns the raw session token from the
// request — preferring the cookie, falling back to the
// `Authorization: Bearer …` header for non-browser callers (cli /
// device extension).
func ExtractTokenFromRequest(r *http.Request) string {
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		return c.Value
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(auth, prefix) {
			return strings.TrimSpace(auth[len(prefix):])
		}
	}
	return ""
}

// SetCookie writes the session cookie on the response. SameSite=Lax,
// HttpOnly=true; Secure is only set when the request scheme is
// https — demo period runs over http for local dev.
func SetCookie(c *gin.Context, token string, expiresMs int64) {
	secure := c.Request.TLS != nil
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(
		CookieName,
		token,
		secondsUntil(expiresMs),
		"/",
		"",     // host-only — works for any host
		secure, // HTTPS-only in prod, off in dev
		true,   // HttpOnly
	)
}

// ClearCookie expires the session cookie.
func ClearCookie(c *gin.Context) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(CookieName, "", -1, "/", "", c.Request.TLS != nil, true)
}

// secondsUntil returns the seconds remaining until expiresMs from
// the service clock. Wrapped on Service so tests can use the
// injected clock; the package-level fallback uses time.Now.
func secondsUntil(expiresMs int64) int {
	now := nowMillis()
	if expiresMs <= now {
		return -1
	}
	return int((expiresMs - now) / 1000)
}

// AuthMiddleware returns a gin handler that:
//
//  1. extracts the session token from cookie / Bearer header
//  2. calls Authenticate to resolve the User
//  3. on success: stashes User in the gin context under ginUserKey
//     and continues
//  4. on failure: aborts with 401
func (s *Service) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := ExtractTokenFromRequest(c.Request)
		if raw == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing session"})
			return
		}
		u, err := s.Authenticate(c.Request.Context(), raw)
		if err != nil {
			if errors.Is(err, ErrSessionInvalid) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
				return
			}
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "auth error"})
			return
		}
		c.Set(ginUserKey, u)
		c.Next()
	}
}

// UserFrom returns the authenticated user from a gin context. Panics
// when AuthMiddleware wasn't applied (programmer error).
func UserFrom(c *gin.Context) User {
	v, ok := c.Get(ginUserKey)
	if !ok {
		panic("identity: UserFrom called outside AuthMiddleware")
	}
	return v.(User)
}

// UserFromContext returns the user attached to a request context (or
// zero User + false). Used by WS upgrades where the gin context may
// not be available downstream.
func UserFromContext(ctx context.Context) (User, bool) {
	v, ok := ctx.Value(contextUserKey{}).(User)
	return v, ok
}

// WithUserContext attaches a user to a context — used by WS upgrades
// to thread the auth result through to background goroutines.
func WithUserContext(parent context.Context, u User) context.Context {
	return context.WithValue(parent, contextUserKey{}, u)
}

// nowMillis is the package-level clock used by SetCookie /
// secondsUntil where we can't reach into the Service clock. Tests
// don't exercise this path directly.
var nowMillis = func() int64 {
	return _nowFn().UnixMilli()
}

// _nowFn is replaced by tests to inject a fake clock at the package
// level. Service-level clock injection is preferred for everything
// that goes through Service methods.
var _nowFn = realNow

func realNow() time.Time { return time.Now() }
