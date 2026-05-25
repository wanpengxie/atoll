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

// CommandResponse is the daemon's reply envelope. The daemon wraps
// every tool result in a generic envelope with `success` + `data` (or
// `error`). The adapter passes `Data` through to coagent.Respond as
// payload (Level A — adapter doesn't reshape business fields beyond
// the success/failure dichotomy).
type CommandResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   string          `json:"error,omitempty"`
	Code    string          `json:"code,omitempty"`
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
		// Promote `out.Error` / `out.Code` to surface what the daemon
		// said. Caller maps to failed terminal.
		errMsg := out.Error
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
