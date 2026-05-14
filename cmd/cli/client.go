package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// httpClient bundles the server URL + bearer token; methods do JSON
// request/response with consistent error mapping.
type httpClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

// newHTTPClient resolves the server URL + token from explicit flags
// then env vars, then defaults. Caller passes the parsed flag values
// (empty string means "use env/default").
func newHTTPClient(serverURL, token string) (*httpClient, error) {
	if serverURL == "" {
		serverURL = os.Getenv("COAGENT_SERVER_URL")
	}
	if serverURL == "" {
		serverURL = "http://localhost:8080"
	}
	if token == "" {
		token = os.Getenv("COAGENT_SESSION_TOKEN")
	}
	if _, err := url.Parse(serverURL); err != nil {
		return nil, fmt.Errorf("invalid --server-url %q: %w", serverURL, err)
	}
	return &httpClient{
		baseURL: strings.TrimRight(serverURL, "/"),
		token:   token,
		hc:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// do executes an HTTP request and decodes the JSON body into `out`.
// `body` is JSON-encoded if non-nil. Non-2xx responses are returned
// as errors with the response body inlined.
func (c *httpClient) do(method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode body: %w", err)
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpError{
			Method: method, Path: path,
			Status: resp.StatusCode, Body: string(raw),
		}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w (body=%s)", err, truncate(string(raw), 300))
	}
	return nil
}

// httpError is returned when the server replies with a non-2xx status.
type httpError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s %s -> %d: %s", e.Method, e.Path, e.Status, truncate(e.Body, 500))
}

// isHTTPStatus reports whether `err` is an httpError with the given
// status code.
func isHTTPStatus(err error, status int) bool {
	var he *httpError
	if errors.As(err, &he) {
		return he.Status == status
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// emitJSON writes `v` as 2-space-indented JSON to stdout with a
// trailing newline. Used by subcommands for human-readable output.
func emitJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
