package feishu

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coagent-ai/coagent/adapters/framework"
)

// Credential store keys for the feishu adapter.
const (
	// CredKeyAppID stores the public app_id.
	CredKeyAppID = "feishu.app_id"

	// CredKeyAppSecret stores the secret half — NEVER printed.
	CredKeyAppSecret = "feishu.app_secret"
)

// tokenSafetyMargin is the slack the cache leaves before expiring a
// token so concurrent callers never observe a stale value.
const tokenSafetyMargin = 60 * time.Second

// credentialBundle is the value loaded from the framework
// CredentialStore at Init time. The bundle holds both the public
// app_id and the private app_secret — callers MUST pass it through the
// adapter's redact wrapper before logging.
type credentialBundle struct {
	AppID     string
	AppSecret string
}

// loadCredentials pulls app_id + app_secret from store. Returns
// framework.ErrCredentialMissing wrapped errors when either key is
// absent — daemons treat this as a fatal install error.
func loadCredentials(ctx context.Context, store framework.CredentialStore) (credentialBundle, error) {
	appID, ok, err := store.Get(ctx, CredKeyAppID)
	if err != nil {
		return credentialBundle{}, fmt.Errorf("feishu: load app_id: %w", err)
	}
	if !ok || appID == "" {
		return credentialBundle{}, framework.MissingCredentialError(CredKeyAppID)
	}
	appSecret, ok, err := store.Get(ctx, CredKeyAppSecret)
	if err != nil {
		return credentialBundle{}, fmt.Errorf("feishu: load app_secret: %w", err)
	}
	if !ok || appSecret == "" {
		return credentialBundle{}, framework.MissingCredentialError(CredKeyAppSecret)
	}
	return credentialBundle{AppID: appID, AppSecret: appSecret}, nil
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
