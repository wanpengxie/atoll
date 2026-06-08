package feishu

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Credential environment variable keys for the feishu actor.
const (
	// EnvKeyAppID stores the public app_id.
	EnvKeyAppID = "FEISHU_APP_ID"

	// EnvKeyAppSecret stores the secret half — NEVER printed.
	EnvKeyAppSecret = "FEISHU_APP_SECRET"
)

// tokenSafetyMargin is the slack the cache leaves before expiring a
// token so concurrent callers never observe a stale value.
const tokenSafetyMargin = 60 * time.Second

// CredentialBundle is the value loaded from environment variables at
// construction time. The bundle holds both the public app_id and the
// private app_secret — callers MUST pass it through the adapter's
// redact wrapper before logging.
type CredentialBundle struct {
	AppID     string
	AppSecret string
}

// LoadCredentialsFromEnv reads FEISHU_APP_ID and FEISHU_APP_SECRET from
// the process environment. Returns an error when either is absent.
func LoadCredentialsFromEnv() (CredentialBundle, error) {
	appID := os.Getenv(EnvKeyAppID)
	appSecret := os.Getenv(EnvKeyAppSecret)
	if appID == "" || appSecret == "" {
		return CredentialBundle{}, fmt.Errorf("feishu: %s and %s required", EnvKeyAppID, EnvKeyAppSecret)
	}
	return CredentialBundle{AppID: appID, AppSecret: appSecret}, nil
}

// tokenCache stores the cached tenant_access_token with refresh time
// safety margin. Safe for concurrent use.
type tokenCache struct {
	mu        sync.Mutex
	token     string
	expiresAt time.Time
	clock     func() time.Time
}

func newTokenCache(clock func() time.Time) *tokenCache {
	if clock == nil {
		clock = time.Now
	}
	return &tokenCache{clock: clock}
}

// get returns the cached token when still valid; refresh tells the
// caller whether they need to call the API to refresh.
func (c *tokenCache) get() (token string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token == "" {
		return "", false
	}
	if c.clock().Add(tokenSafetyMargin).After(c.expiresAt) {
		return "", false
	}
	return c.token, true
}

// set stores a fresh token + its expiry time (computed from a Feishu
// `expire` (seconds) returned by /auth/v3/tenant_access_token/internal).
func (c *tokenCache) set(token string, ttlSeconds int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	c.expiresAt = c.clock().Add(time.Duration(ttlSeconds) * time.Second)
}

// invalidate clears the cache so the next call will refresh.
func (c *tokenCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = ""
	c.expiresAt = time.Time{}
}

// snapshot returns the current token + expiry (used for diagnostics —
// the token value is RAW; callers MUST redact before logging).
func (c *tokenCache) snapshot() (string, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token, c.expiresAt
}
