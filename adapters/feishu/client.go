package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/coagent-ai/coagent/adapters/framework"
)

// DefaultBaseURL is the public Feishu OpenAPI host.
const DefaultBaseURL = "https://open.feishu.cn/open-apis"

// tokenResponse is the body returned by /auth/v3/tenant_access_token/internal.
type tokenResponse struct {
	Code             int    `json:"code"`
	Msg              string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
	Expire            int    `json:"expire"`
}

// apiEnvelope is the standard Feishu API success envelope.
type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data,omitempty"`
}

// SendMessageRequest is the body for POST /im/v1/messages.
//
// receive_id_type is passed as a query parameter (the Feishu API
// requires it that way), not inside this struct.
type SendMessageRequest struct {
	ReceiveID string `json:"receive_id"`
	MsgType   string `json:"msg_type"`
	Content   string `json:"content"`
}

// SendMessageResponse is the data block on a successful send.
type SendMessageResponse struct {
	MessageID string `json:"message_id"`
	RootID    string `json:"root_id,omitempty"`
	ParentID  string `json:"parent_id,omitempty"`
	ChatID    string `json:"chat_id,omitempty"`
}

// CreateChatRequest is the body for POST /im/v1/chats.
type CreateChatRequest struct {
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	UserIDList  []string `json:"user_id_list,omitempty"`
	BotIDList   []string `json:"bot_id_list,omitempty"`
}

// CreateChatResponse is the data block on a successful chat create.
type CreateChatResponse struct {
	ChatID  string `json:"chat_id"`
	Name    string `json:"name,omitempty"`
	OwnerID string `json:"owner_id,omitempty"`
}

// client is the feishu OpenAPI wrapper sitting on top of
// framework.HTTPClient. It owns the tenant_access_token cache so callers
// (Handle goroutines) share one cached token even under concurrent
// dispatch.
type client struct {
	http     *framework.HTTPClient
	creds    credentialBundle
	tokens   *tokenCache
	logger   framework.Logger
	metrics  framework.Metrics
}

func newClient(http *framework.HTTPClient, creds credentialBundle, tokens *tokenCache, logger framework.Logger, metrics framework.Metrics) *client {
	if logger == nil {
		logger = framework.NoopLogger{}
	}
	if metrics == nil {
		logger = framework.NoopLogger{}
	}
	return &client{
		http:    http,
		creds:   creds,
		tokens:  tokens,
		logger:  logger,
		metrics: metrics,
	}
}

// ensureToken returns a valid tenant_access_token, fetching one when
// the cache misses. Failure surfaces a redacted error so secrets cannot
// leak through the error chain.
func (c *client) ensureToken(ctx context.Context) (string, error) {
	if c.http == nil {
		return "", errors.New("feishu: http client not initialised")
	}
	if tok, ok := c.tokens.get(); ok {
		return tok, nil
	}
	body, err := json.Marshal(map[string]string{
		"app_id":     c.creds.AppID,
		"app_secret": c.creds.AppSecret,
	})
	if err != nil {
		return "", fmt.Errorf("feishu: marshal token body: %w", err)
	}
	resp, err := c.http.DoWithRetry(ctx, http.MethodPost,
		"/auth/v3/tenant_access_token/internal",
		func() (io.Reader, error) { return bytes.NewReader(body), nil },
		http.Header{"Content-Type": []string{"application/json"}},
	)
	if err != nil {
		return "", c.redact(fmt.Errorf("feishu: fetch token: %w", err))
	}
	if resp.StatusCode/100 != 2 {
		return "", c.redact(fmt.Errorf("feishu: fetch token status %d body=%s", resp.StatusCode, string(resp.Body)))
	}
	var tr tokenResponse
	if err := json.Unmarshal(resp.Body, &tr); err != nil {
		return "", c.redact(fmt.Errorf("feishu: decode token body: %w", err))
	}
	if tr.Code != 0 {
		return "", c.redact(fmt.Errorf("feishu: token api code=%d msg=%s", tr.Code, tr.Msg))
	}
	if tr.TenantAccessToken == "" {
		return "", errors.New("feishu: token api returned empty tenant_access_token")
	}
	c.tokens.set(tr.TenantAccessToken, tr.Expire)
	c.metrics.IncCounter("adapter.feishu.token_refresh")
	c.logger.Info("feishu.token.refreshed",
		"app_id", c.creds.AppID,
		"ttl_seconds", tr.Expire,
		"token", framework.Redact(tr.TenantAccessToken),
	)
	return tr.TenantAccessToken, nil
}

// SendMessage POSTs /im/v1/messages?receive_id_type=<receiveIDType> and
// returns the parsed data block.
func (c *client) SendMessage(ctx context.Context, receiveIDType string, body SendMessageRequest) (*SendMessageResponse, error) {
	if receiveIDType == "" {
		receiveIDType = "chat_id"
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("feishu: marshal send body: %w", err)
	}
	var data SendMessageResponse
	if err := c.callJSON(ctx, http.MethodPost,
		"/im/v1/messages?receive_id_type="+receiveIDType,
		raw, &data,
	); err != nil {
		return nil, err
	}
	return &data, nil
}

// CreateChat POSTs /im/v1/chats and returns the parsed data block.
func (c *client) CreateChat(ctx context.Context, body CreateChatRequest) (*CreateChatResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("feishu: marshal create body: %w", err)
	}
	var data CreateChatResponse
	if err := c.callJSON(ctx, http.MethodPost, "/im/v1/chats", raw, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

// callJSON is the shared envelope-handling JSON call helper. Decodes
// the Feishu code != 0 envelope into a typed error so handlers can
// translate to status=failed terminal responses.
func (c *client) callJSON(ctx context.Context, method, path string, body []byte, out any) error {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}
	headers := http.Header{
		"Content-Type":  []string{"application/json"},
		"Authorization": []string{"Bearer " + token},
	}
	resp, err := c.http.DoWithRetry(ctx, method, path,
		func() (io.Reader, error) { return bytes.NewReader(body), nil },
		headers,
	)
	if err != nil {
		return c.redact(fmt.Errorf("feishu: %s %s: %w", method, path, err))
	}
	if resp.StatusCode == http.StatusUnauthorized {
		// Token may have been revoked — invalidate cache + bubble.
		c.tokens.invalidate()
	}
	if resp.StatusCode/100 != 2 {
		return c.redact(fmt.Errorf("feishu: %s %s status %d body=%s",
			method, path, resp.StatusCode, string(resp.Body)))
	}
	var env apiEnvelope
	if err := json.Unmarshal(resp.Body, &env); err != nil {
		return c.redact(fmt.Errorf("feishu: decode envelope: %w", err))
	}
	if env.Code != 0 {
		return &APIError{Code: env.Code, Msg: env.Msg, Path: path}
	}
	if out == nil || len(env.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return c.redact(fmt.Errorf("feishu: decode data: %w", err))
	}
	return nil
}

// redact wraps an error so its message has every credential substring
// scrubbed. Helpers MUST call redact before returning any error that
// could embed app_secret / access_token bytes.
func (c *client) redact(err error) error {
	if err == nil {
		return nil
	}
	secrets := []string{c.creds.AppSecret}
	if tok, _ := c.tokens.snapshot(); tok != "" {
		secrets = append(secrets, tok)
	}
	return framework.RedactError(err, secrets...)
}

// APIError is the Feishu envelope code != 0 surfaced as a typed error.
// Handlers translate APIError into status=failed Respond terminals.
type APIError struct {
	Code int
	Msg  string
	Path string
}

// Error returns the wire form (safe to log — never contains secrets).
func (e *APIError) Error() string {
	return fmt.Sprintf("feishu api %s code=%d msg=%s", e.Path, e.Code, e.Msg)
}
