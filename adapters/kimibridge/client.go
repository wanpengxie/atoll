package kimibridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/wanpengxie/ActOS/adapters/framework"
)

// CommandRequest is the wire body sent to POST /command. Mirrors the
// SKILL.md §Call Format example. `Session` partitions browser tab
// groups — distinct names keep parallel multi-site tasks isolated.
type CommandRequest struct {
	Action  string          `json:"action"`
	Args    json.RawMessage `json:"args,omitempty"`
	Session string          `json:"session,omitempty"`
}

// CommandResponse is the daemon's reply envelope.
//
// Observed wire (kimi-webbridge v1.9.13):
//
//	success: {"ok": true, "data": {...tool-specific fields...}}
//	failure: {"ok": false, "error": {"code": "<closed-set>", "message": "..."}}
//
// We mirror that shape verbatim so the JSON unmarshal stays lossless,
// then expose convenience accessors (Succeeded / ErrorMessage /
// ErrorCode) so callers don't reach into the nested struct.
type CommandResponse struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *CommandError   `json:"error,omitempty"`
}

// CommandError is the nested error object the daemon returns when
// ok=false. Fields are informative; coagent surfaces them as
// payload.error_code + payload.detail on the failed terminal.
type CommandError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Succeeded reports whether the daemon flagged the tool call as
// successful. Centralised so callers don't have to remember that the
// wire field is `ok`, not the more common `success`.
func (r *CommandResponse) Succeeded() bool {
	return r != nil && r.OK
}

// ErrorCode returns the daemon-supplied error code (empty when none).
func (r *CommandResponse) ErrorCode() string {
	if r == nil || r.Error == nil {
		return ""
	}
	return r.Error.Code
}

// ErrorMessage returns the daemon-supplied error message (empty when
// none).
func (r *CommandResponse) ErrorMessage() string {
	if r == nil || r.Error == nil {
		return ""
	}
	return r.Error.Message
}

// Client is a thin wrapper around framework.HTTPClient that knows the
// kimi-webbridge POST /command contract. One instance per Module — the
// adapter framework F8 HTTPClient lives below it so retries / breaker
// metrics inherit framework defaults.
type Client struct {
	http *framework.HTTPClient
}

// NewClient wraps an HTTPClient that's already configured with the
// daemon BaseURL (typically http://127.0.0.1:10086). NewModule injects
// this via the framework Deps bundle.
func NewClient(http *framework.HTTPClient) *Client {
	return &Client{http: http}
}

// Call dispatches one tool invocation. Returns:
//   - the parsed CommandResponse (always non-nil when err is nil)
//   - the raw HTTP status code (200 on success; non-2xx surfaces via err)
//   - err when the wire didn't deliver / the daemon body wasn't JSON
//
// Business-level failure (daemon returned `{success:false, ...}`)
// arrives as a CommandResponse with Success=false — caller maps it to
// a failed terminal via FailNow; it is NOT bubbled up as a transport
// error because there's nothing for the adapter to retry.
func (c *Client) Call(ctx context.Context, req CommandRequest) (*CommandResponse, int, error) {
	if c.http == nil {
		return nil, 0, errors.New("kimibridge.Client.Call: http client nil")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, 0, fmt.Errorf("kimibridge.Client.Call: marshal request: %w", err)
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	resp, err := c.http.Do(ctx, http.MethodPost, "/command", bytes.NewReader(body), headers)
	if err != nil {
		return nil, 0, fmt.Errorf("kimibridge.Client.Call: transport: %w", err)
	}
	var out CommandResponse
	if len(resp.Body) > 0 {
		if jerr := json.Unmarshal(resp.Body, &out); jerr != nil {
			return nil, resp.StatusCode, fmt.Errorf("kimibridge.Client.Call: decode response: %w (body=%s)", jerr, snippet(resp.Body))
		}
	}
	if resp.StatusCode >= 400 {
		// HTTP-level failure (5xx daemon error, 4xx schema reject).
		// Promote out.Error message so the caller has a single
		// human-readable string for the failed terminal.
		errMsg := out.ErrorMessage()
		if errMsg == "" {
			errMsg = fmt.Sprintf("kimi-webbridge HTTP %d", resp.StatusCode)
		}
		return &out, resp.StatusCode, fmt.Errorf("kimi-webbridge daemon: %s", errMsg)
	}
	return &out, resp.StatusCode, nil
}

func snippet(b []byte) string {
	const max = 256
	if len(b) > max {
		return string(b[:max]) + "...(truncated)"
	}
	return string(b)
}
