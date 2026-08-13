package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
)

const (
	protocolVersion = "2026-07-28"
	clientName      = "atoll-mcp"
	clientVersion   = "0.1.0"
)

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	Method  string          `json:"method,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	detail := fmt.Sprintf("MCP protocol error %d: %s", e.Code, e.Message)
	if len(e.Data) != 0 && string(e.Data) != "null" {
		detail += ": " + string(e.Data)
	}
	return detail
}

type responseInfo struct {
	ContentType   string
	Notifications int
}

type transport interface {
	RoundTrip(context.Context, rpcRequest, string) (rpcResponse, responseInfo, error)
	Close() error
}

type client struct {
	transport transport
	nextID    int64
}

func newClient(cfg Config) (*client, error) {
	var t transport
	switch cfg.Transport {
	case transportHTTP:
		t = &httpTransport{url: cfg.URL, client: &http.Client{}}
	case transportStdio:
		stdio, err := startStdio(cfg)
		if err != nil {
			return nil, err
		}
		t = stdio
	default:
		return nil, fmt.Errorf("mcp: unsupported transport %q", cfg.Transport)
	}
	return &client{transport: t}, nil
}

func (c *client) Close() error { return c.transport.Close() }

func (c *client) request(ctx context.Context, method, name string, params map[string]any, progress bool, out any) (responseInfo, error) {
	c.nextID++
	id := c.nextID
	meta := map[string]any{
		"io.modelcontextprotocol/protocolVersion": protocolVersion,
		"io.modelcontextprotocol/clientInfo": map[string]any{
			"name": clientName, "version": clientVersion,
		},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}
	if progress {
		meta["progressToken"] = id
	}
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = meta
	response, info, err := c.transport.RoundTrip(ctx, rpcRequest{
		JSONRPC: "2.0", ID: id, Method: method, Params: params,
	}, name)
	if err != nil {
		return info, err
	}
	if response.Error != nil {
		return info, response.Error
	}
	if len(response.Result) == 0 {
		return info, errors.New("mcp: JSON-RPC response omitted result")
	}
	if err := json.Unmarshal(response.Result, out); err != nil {
		return info, fmt.Errorf("mcp: decode %s result: %w", method, err)
	}
	return info, nil
}

type discovery struct {
	Instructions string `json:"instructions"`
	Meta         struct {
		ServerInfo json.RawMessage `json:"io.modelcontextprotocol/serverInfo"`
	} `json:"_meta"`
}

type toolList struct {
	Tools []tool `json:"tools"`
}

type tool struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
}

type callResult struct {
	Content           []json.RawMessage `json:"content"`
	StructuredContent json.RawMessage   `json:"structuredContent,omitempty"`
	IsError           bool              `json:"isError"`
	ResultType        string            `json:"resultType"`
}

func (c *client) discover(ctx context.Context) (discovery, error) {
	var out discovery
	_, err := c.request(ctx, "server/discover", "", nil, false, &out)
	return out, err
}

func (c *client) listTools(ctx context.Context) (toolList, error) {
	var out toolList
	_, err := c.request(ctx, "tools/list", "", nil, false, &out)
	return out, err
}

func (c *client) callTool(ctx context.Context, name string, arguments json.RawMessage, progress bool) (callResult, responseInfo, error) {
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	var rawArguments any
	if err := json.Unmarshal(arguments, &rawArguments); err != nil {
		return callResult{}, responseInfo{}, fmt.Errorf("mcp: arguments must be JSON: %w", err)
	}
	var out callResult
	info, err := c.request(ctx, "tools/call", name, map[string]any{
		"name": name, "arguments": rawArguments,
	}, progress, &out)
	return out, info, err
}

type httpTransport struct {
	url    string
	client *http.Client
}

func (t *httpTransport) Close() error {
	t.client.CloseIdleConnections()
	return nil
}

func (t *httpTransport) RoundTrip(ctx context.Context, request rpcRequest, name string) (rpcResponse, responseInfo, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return rpcResponse{}, responseInfo{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return rpcResponse{}, responseInfo{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", protocolVersion)
	req.Header.Set("Mcp-Method", request.Method)
	if name != "" {
		req.Header.Set("Mcp-Name", name)
	}
	response, err := t.client.Do(req)
	if err != nil {
		return rpcResponse{}, responseInfo{}, fmt.Errorf("mcp http: %w", err)
	}
	defer response.Body.Close()
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return rpcResponse{}, responseInfo{}, fmt.Errorf("mcp http: invalid Content-Type: %w", err)
	}
	info := responseInfo{ContentType: mediaType}
	switch mediaType {
	case "application/json":
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		var message rpcResponse
		decodeErr := json.Unmarshal(raw, &message)
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			if readErr == nil && decodeErr == nil && message.Error != nil {
				return message, info, nil
			}
			return rpcResponse{}, info, fmt.Errorf("mcp http: status %s: %s", response.Status, strings.TrimSpace(string(raw)))
		}
		if readErr != nil {
			return rpcResponse{}, info, fmt.Errorf("mcp http: read JSON response: %w", readErr)
		}
		if decodeErr != nil {
			return rpcResponse{}, info, fmt.Errorf("mcp http: decode JSON response: %w", decodeErr)
		}
		return message, info, nil
	case "text/event-stream":
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			return rpcResponse{}, info, fmt.Errorf("mcp http: status %s: %s", response.Status, strings.TrimSpace(string(raw)))
		}
		message, notifications, err := readSSE(response.Body, request.ID)
		info.Notifications = notifications
		return message, info, err
	default:
		return rpcResponse{}, info, fmt.Errorf("mcp http: unsupported Content-Type %q", mediaType)
	}
}

func readSSE(r io.Reader, requestID int64) (rpcResponse, int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var data []string
	notifications := 0
	consume := func() (rpcResponse, bool, error) {
		if len(data) == 0 {
			return rpcResponse{}, false, nil
		}
		raw := strings.Join(data, "\n")
		data = data[:0]
		var message rpcResponse
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			return rpcResponse{}, false, fmt.Errorf("mcp sse: decode event: %w", err)
		}
		if message.Method != "" && len(message.ID) == 0 {
			notifications++
			return rpcResponse{}, false, nil
		}
		var id int64
		if err := json.Unmarshal(message.ID, &id); err != nil || id != requestID {
			return rpcResponse{}, false, nil
		}
		return message, true, nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			message, done, err := consume()
			if err != nil || done {
				return message, notifications, err
			}
		case strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "data:"):
			value := strings.TrimPrefix(line, "data:")
			if strings.HasPrefix(value, " ") {
				value = value[1:]
			}
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return rpcResponse{}, notifications, fmt.Errorf("mcp sse: read: %w", err)
	}
	if message, done, err := consume(); err != nil || done {
		return message, notifications, err
	}
	return rpcResponse{}, notifications, errors.New("mcp sse: stream ended without final response")
}

type stdioTransport struct {
	roundMu  sync.Mutex
	closeMu  sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	scanner  *bufio.Scanner
	closeErr error
	closed   bool
}

func startStdio(cfg Config) (*stdioTransport, error) {
	cmd := exec.Command(cfg.Command, cfg.Args...)
	cmd.Dir = cfg.Cwd
	cmd.Env = mergedEnv(cfg.Env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp stdio: stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp stdio: stdout: %w", err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp stdio: start: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	return &stdioTransport{cmd: cmd, stdin: stdin, scanner: scanner}, nil
}

func mergedEnv(overrides map[string]string) []string {
	values := map[string]string{}
	for _, item := range os.Environ() {
		if key, value, ok := strings.Cut(item, "="); ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}
	return out
}

func (t *stdioTransport) RoundTrip(ctx context.Context, request rpcRequest, _ string) (rpcResponse, responseInfo, error) {
	t.roundMu.Lock()
	defer t.roundMu.Unlock()
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		return rpcResponse{}, responseInfo{}, errors.New("mcp stdio: process is not running")
	}
	t.closeMu.Unlock()
	raw, err := json.Marshal(request)
	if err != nil {
		return rpcResponse{}, responseInfo{}, err
	}
	if _, err := t.stdin.Write(append(raw, '\n')); err != nil {
		t.closeProcess()
		return rpcResponse{}, responseInfo{}, fmt.Errorf("mcp stdio: write: %w", err)
	}
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			t.closeProcess()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)
	info := responseInfo{ContentType: "application/json"}
	for t.scanner.Scan() {
		var response rpcResponse
		if err := json.Unmarshal(t.scanner.Bytes(), &response); err != nil {
			t.closeProcess()
			return rpcResponse{}, info, fmt.Errorf("mcp stdio: decode response: %w", err)
		}
		if response.Method != "" && len(response.ID) == 0 {
			info.Notifications++
			continue
		}
		var id int64
		if json.Unmarshal(response.ID, &id) == nil && id == request.ID {
			return response, info, nil
		}
	}
	scanErr := t.scanner.Err()
	t.closeProcess()
	if ctx.Err() != nil {
		return rpcResponse{}, info, ctx.Err()
	}
	if scanErr != nil {
		return rpcResponse{}, info, fmt.Errorf("mcp stdio: read: %w", scanErr)
	}
	return rpcResponse{}, info, errors.New("mcp stdio: process ended before response")
}

func (t *stdioTransport) Close() error {
	t.closeProcess()
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	return t.closeErr
}

func (t *stdioTransport) closeProcess() {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.closeLocked()
}

func (t *stdioTransport) closeLocked() {
	if t.closed {
		return
	}
	t.closed = true
	_ = t.stdin.Close()
	if t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
	t.closeErr = t.cmd.Wait()
}
