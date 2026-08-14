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
	"sync/atomic"
	"time"
)

const (
	protocolVersion       = "2026-07-28"
	clientName            = "atoll-mcp"
	clientVersion         = "0.1.0"
	stdioCancelWriteGrace = 100 * time.Millisecond
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
	nextID    atomic.Int64
	cacheMu   sync.Mutex
	cache     map[string]cachedResult
	now       func() time.Time
}

type cachedResult struct {
	raw       json.RawMessage
	expiresAt time.Time
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
	return &client{transport: t, cache: make(map[string]cachedResult), now: time.Now}, nil
}

func (c *client) Close() error { return c.transport.Close() }

func (c *client) request(ctx context.Context, method, name string, params map[string]any, progress bool, out any) (responseInfo, error) {
	raw, info, err := c.requestRaw(ctx, method, name, params, progress)
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return info, fmt.Errorf("mcp: decode %s result: %w", method, err)
	}
	return info, nil
}

func (c *client) requestRaw(ctx context.Context, method, name string, params map[string]any, progress bool) (json.RawMessage, responseInfo, error) {
	id := c.nextID.Add(1)
	meta := map[string]any{
		"io.modelcontextprotocol/protocolVersion": protocolVersion,
		"io.modelcontextprotocol/clientInfo": map[string]any{
			"name": clientName, "version": clientVersion,
		},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{
			"elicitation": map[string]any{"form": map[string]any{}},
		},
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
		return nil, info, err
	}
	if response.Error != nil {
		return nil, info, response.Error
	}
	if len(response.Result) == 0 {
		return nil, info, errors.New("mcp: JSON-RPC response omitted result")
	}
	return append(json.RawMessage(nil), response.Result...), info, nil
}

// cachedRequest holds cacheMu only around the map itself, never across the
// round trip. sync.Mutex.Lock takes no context, so a request waiting on a peer's
// in-flight fetch could not honour its own cancellation — one slow server would
// pin every concurrent describe for the full call timeout. The cost of releasing
// it is that two callers racing a cold entry each issue a request; for ttlMs=0
// methods (tools/list) that is what happens anyway, and for cached ones it is
// one extra fetch at the expiry instant.
func (c *client) cachedRequest(ctx context.Context, method, name string, params map[string]any, out any) (responseInfo, error) {
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	c.cacheMu.Lock()
	cached, ok := c.cache[method]
	c.cacheMu.Unlock()
	if ok && now().Before(cached.expiresAt) {
		if err := json.Unmarshal(cached.raw, out); err != nil {
			return responseInfo{}, fmt.Errorf("mcp: decode cached %s result: %w", method, err)
		}
		return responseInfo{}, nil
	}
	raw, info, err := c.requestRaw(ctx, method, name, params, false)
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return info, fmt.Errorf("mcp: decode %s result: %w", method, err)
	}
	var hint struct {
		TTLMS int64 `json:"ttlMs"`
	}
	if err := json.Unmarshal(raw, &hint); err != nil {
		return info, fmt.Errorf("mcp: decode %s cache hint: %w", method, err)
	}
	c.cacheMu.Lock()
	if hint.TTLMS > 0 {
		c.cache[method] = cachedResult{
			raw:       append(json.RawMessage(nil), raw...),
			expiresAt: now().Add(time.Duration(hint.TTLMS) * time.Millisecond),
		}
	} else {
		delete(c.cache, method)
	}
	c.cacheMu.Unlock()
	return info, nil
}

type discovery struct {
	SupportedVersions []string `json:"supportedVersions"`
	Instructions      string   `json:"instructions"`
	TTLMS             int64    `json:"ttlMs"`
	CacheScope        string   `json:"cacheScope"`
	Meta              struct {
		ServerInfo json.RawMessage `json:"io.modelcontextprotocol/serverInfo"`
	} `json:"_meta"`
}

type toolList struct {
	Tools      []tool `json:"tools"`
	TTLMS      int64  `json:"ttlMs"`
	CacheScope string `json:"cacheScope"`
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
	InputRequests     json.RawMessage   `json:"inputRequests,omitempty"`
	RequestState      *string           `json:"requestState,omitempty"`
}

func (c *client) discover(ctx context.Context) (discovery, error) {
	var out discovery
	_, err := c.cachedRequest(ctx, "server/discover", "", nil, &out)
	if err != nil {
		var protocolErr *rpcError
		if errors.As(err, &protocolErr) && protocolErr.Code == -32601 {
			return discovery{}, fmt.Errorf("mcp: server is not a %s v2 server (possibly 2025-11-25): server/discover is unavailable: %w", protocolVersion, err)
		}
		return discovery{}, err
	}
	for _, version := range out.SupportedVersions {
		if version == protocolVersion {
			return out, nil
		}
	}
	return discovery{}, fmt.Errorf("mcp: server is not a %s v2 server; advertised supportedVersions=%v", protocolVersion, out.SupportedVersions)
}

func (c *client) listTools(ctx context.Context) (toolList, error) {
	var out toolList
	_, err := c.cachedRequest(ctx, "tools/list", "", nil, &out)
	return out, err
}

func (c *client) callTool(ctx context.Context, name string, arguments json.RawMessage, progress bool) (callResult, responseInfo, error) {
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	rawArguments, inputResponses, requestState, err := splitCallPayload(arguments)
	if err != nil {
		return callResult{}, responseInfo{}, fmt.Errorf("mcp: arguments must be JSON: %w", err)
	}
	params := map[string]any{"name": name, "arguments": rawArguments}
	if inputResponses != nil {
		params["inputResponses"] = inputResponses
	}
	if requestState != nil {
		params["requestState"] = *requestState
	}
	var out callResult
	info, err := c.request(ctx, "tools/call", name, params, progress, &out)
	return out, info, err
}

func splitCallPayload(raw json.RawMessage) (map[string]json.RawMessage, any, *string, error) {
	arguments := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &arguments); err != nil {
		return nil, nil, nil, err
	}
	// Underscore-prefixed per MCP's own reserved-namespace convention (_meta).
	// This object is the tool's own argument map, so an unprefixed control key
	// would silently claim a name a tool may legitimately declare.
	continuationRaw, ok := arguments["_continuation"]
	if !ok {
		return arguments, nil, nil, nil
	}
	delete(arguments, "_continuation")
	var continuation struct {
		Responses json.RawMessage `json:"responses"`
		Answers   json.RawMessage `json:"answers"`
		State     *string         `json:"state"`
	}
	if err := json.Unmarshal(continuationRaw, &continuation); err != nil {
		return nil, nil, nil, fmt.Errorf("decode continuation: %w", err)
	}
	responses := continuation.Responses
	if len(responses) == 0 {
		responses = continuation.Answers
	}
	var inputResponses any
	if len(responses) != 0 && string(responses) != "null" {
		if err := json.Unmarshal(responses, &inputResponses); err != nil {
			return nil, nil, nil, fmt.Errorf("decode continuation responses: %w", err)
		}
	}
	return arguments, inputResponses, continuation.State, nil
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
	roundMu   sync.Mutex
	writeMu   sync.Mutex
	stateMu   sync.Mutex
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	scanner   *bufio.Scanner
	incoming  chan stdioRead
	stop      chan struct{}
	waitDone  chan struct{}
	closeOnce sync.Once
	closed    bool
	readErr   error
	closeErr  error
}

type stdioRead struct {
	response rpcResponse
	err      error
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
	t := &stdioTransport{
		cmd: cmd, stdin: stdin, scanner: scanner,
		incoming: make(chan stdioRead, 256), stop: make(chan struct{}), waitDone: make(chan struct{}),
	}
	go t.readLoop()
	return t, nil
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
	if t.isClosed() {
		return rpcResponse{}, responseInfo{}, errors.New("mcp stdio: process is not running")
	}
	if err := t.writeJSONContext(ctx, request); err != nil {
		return rpcResponse{}, responseInfo{}, fmt.Errorf("mcp stdio: write: %w", err)
	}
	info := responseInfo{ContentType: "application/json"}
	for {
		select {
		case item, ok := <-t.incoming:
			if !ok {
				if err := t.readerError(); err != nil {
					return rpcResponse{}, info, err
				}
				return rpcResponse{}, info, errors.New("mcp stdio: process ended before response")
			}
			if item.err != nil {
				return rpcResponse{}, info, item.err
			}
			response := item.response
			if response.Method != "" && len(response.ID) == 0 {
				info.Notifications++
				continue
			}
			var id int64
			if json.Unmarshal(response.ID, &id) == nil && id == request.ID {
				return response, info, nil
			}
		case <-ctx.Done():
			// stdio remains deliberately serial, but a timed-out request must not
			// own the subprocess forever. The protocol supports cancellation by
			// request id; keep the process and reader alive so the next serial call
			// can proceed after the server cancels this handler.
			t.sendCancellation(map[string]any{
				"jsonrpc": "2.0", "method": "notifications/cancelled",
				"params": map[string]any{"requestId": request.ID, "reason": ctx.Err().Error()},
			})
			return rpcResponse{}, info, ctx.Err()
		}
	}
}

func (t *stdioTransport) Close() error {
	t.abort()
	<-t.waitDone
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.closeErr
}

func (t *stdioTransport) abort() {
	t.closeOnce.Do(func() {
		t.stateMu.Lock()
		t.closed = true
		t.stateMu.Unlock()
		close(t.stop)
		_ = t.stdin.Close()
		if t.cmd.Process != nil {
			_ = t.cmd.Process.Kill()
		}
		go func() {
			err := t.cmd.Wait()
			t.stateMu.Lock()
			t.closeErr = err
			t.stateMu.Unlock()
			close(t.waitDone)
		}()
	})
}

func (t *stdioTransport) isClosed() bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.closed
}

func (t *stdioTransport) writeJSON(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.isClosed() {
		return errors.New("process is not running")
	}
	_, err = t.stdin.Write(append(raw, '\n'))
	return err
}

func (t *stdioTransport) writeJSONContext(ctx context.Context, value any) error {
	done := make(chan error, 1)
	go func() { done <- t.writeJSON(value) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Closing stdin is the only portable way to interrupt a blocked pipe
		// write. A child that does not read its request cannot be kept usable;
		// mark this transport closed, but never let it outlive the call bound.
		t.abort()
		return ctx.Err()
	}
}

func (t *stdioTransport) sendCancellation(value any) {
	done := make(chan struct{})
	go func() {
		_ = t.writeJSON(value)
		close(done)
	}()
	timer := time.NewTimer(stdioCancelWriteGrace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		// A server that has stopped reading stdin cannot receive cancellation
		// or another call. Retire that subprocess instead of pinning writeMu.
		t.abort()
	case <-t.stop:
	}
}

func (t *stdioTransport) readLoop() {
	defer close(t.incoming)
	for t.scanner.Scan() {
		var response rpcResponse
		if err := json.Unmarshal(t.scanner.Bytes(), &response); err != nil {
			t.publishRead(stdioRead{err: fmt.Errorf("mcp stdio: decode response: %w", err)})
			return
		}
		if !t.publishRead(stdioRead{response: response}) {
			return
		}
	}
	if err := t.scanner.Err(); err != nil {
		t.stateMu.Lock()
		t.readErr = fmt.Errorf("mcp stdio: read: %w", err)
		t.stateMu.Unlock()
	}
}

func (t *stdioTransport) publishRead(item stdioRead) bool {
	select {
	case t.incoming <- item:
		return true
	case <-t.stop:
		return false
	}
}

func (t *stdioTransport) readerError() error {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.readErr
}
