package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

func TestReferenceServerBothTransportsAndSSE(t *testing.T) {
	python := os.Getenv("ATOLL_MCP_TESTSERVER")
	if python == "" {
		t.Skip("ATOLL_MCP_TESTSERVER is unset; set it to mcp-testserver/.venv/bin/python to run the real MCP fixture")
	}
	python, err := filepath.Abs(python)
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Dir(filepath.Dir(filepath.Dir(python)))
	port, localListenErr := freeTCPPort()
	t.Run("http json and sse", func(t *testing.T) {
		if localListenErr != nil {
			t.Skipf("HTTP fixture cannot listen in this environment: %v", localListenErr)
		}
		server := exec.Command(python, "-m", "server.main", "--transport", "http", "--port", fmt.Sprint(port))
		server.Dir = project
		server.Stdout = os.Stderr
		server.Stderr = os.Stderr
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = server.Process.Kill()
			_ = server.Wait()
		}()
		waitTCP(t, fmt.Sprintf("127.0.0.1:%d", port))

		c, err := newClient(Config{Name: "reference-http", Transport: transportHTTP, URL: fmt.Sprintf("http://127.0.0.1:%d/mcp", port)})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		verifyReferenceClient(t, c)

		without, plainInfo, err := c.callTool(context.Background(), "slow_task", json.RawMessage(`{"seconds":0.12}`), false)
		if err != nil {
			t.Fatal(err)
		}
		with, streamInfo, err := c.callTool(context.Background(), "slow_task", json.RawMessage(`{"seconds":0.12}`), true)
		if err != nil {
			t.Fatal(err)
		}
		if plainInfo.ContentType != "application/json" {
			t.Fatalf("plain content-type=%q", plainInfo.ContentType)
		}
		if streamInfo.ContentType != "text/event-stream" || streamInfo.Notifications < 2 {
			t.Fatalf("SSE info=%+v", streamInfo)
		}
		if !reflect.DeepEqual(without, with) {
			t.Fatalf("progress changed final result:\nwithout=%#v\nwith=%#v", without, with)
		}
		t.Logf("plain=%+v stream=%+v final results equal", plainInfo, streamInfo)
		verifyMRTR(t, c)
		verifyTTLRefreshAndPromptCache(t, c)
		verifyTimeoutAndReuse(t, c)
		verifyActorTimeoutWithoutExpires(t, c, transportHTTP)
		verifyHTTPConcurrency(t, c)
	})

	t.Run("stdio and close owns child", func(t *testing.T) {
		if localListenErr != nil {
			t.Skipf("Python SDK async runtime is unavailable in the no-socket execution sandbox: %v", localListenErr)
		}
		c, err := newClient(Config{
			Name: "reference-stdio", Transport: transportStdio, Command: python,
			Args: []string{"-m", "server.main", "--transport", "stdio"}, Cwd: project,
		})
		if err != nil {
			t.Fatal(err)
		}
		stdio := c.transport.(*stdioTransport)
		pid := stdio.cmd.Process.Pid
		verifyReferenceClient(t, c)
		verifyMRTR(t, c)
		verifyTTLRefreshAndPromptCache(t, c)
		verifyTimeoutAndReuse(t, c)
		verifyActorTimeoutWithoutExpires(t, c, transportStdio)
		if err := c.Close(); err != nil && !stringsContains(err.Error(), "killed") {
			t.Fatalf("close: %v", err)
		}
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("stdio child %d still exists: %v", pid, err)
		}
		t.Logf("stdio child %d exited after client close", pid)
	})
}

type terminalRecorder struct {
	actorbase.Sys
	reply    map[string]any
	failCode string
	failText string
}

type discardState struct{}

func (discardState) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (discardState) Put(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (discardState) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

func (s *terminalRecorder) State() actorbase.StateHandle { return discardState{} }

func (s *terminalRecorder) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.reply, _ = value.(map[string]any)
	return "reply", nil
}

func (s *terminalRecorder) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.failCode, s.failText = code, detail
	return "fail", nil
}

func verifyActorTimeoutWithoutExpires(t *testing.T, c *client, transportName string) {
	t.Helper()
	a := &mcpActor{
		cfg:    Config{Name: "fixture", Transport: transportName, CallTimeoutMS: 150},
		client: c,
		snapshot: snapshot{tools: map[string]string{
			"fixture.never_returns": "never_returns",
			"fixture.echo":          "echo",
		}},
	}
	sys := &terminalRecorder{}
	envelope := message.Envelope{
		ID: "caller-unbounded", Kind: message.KindRequest, Type: "fixture.never_returns",
		Payload: json.RawMessage(`{"body":{}}`),
	}
	if envelope.ExpiresAt != nil {
		t.Fatal("test fixture unexpectedly set expires_at")
	}
	a.call(sys, actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), envelope))
	if sys.failCode != "mcp_timeout" || sys.failText == "" {
		t.Fatalf("actor timeout terminal: code=%q detail=%q", sys.failCode, sys.failText)
	}
	if err := a.currentLastError(); err != nil {
		t.Fatalf("actor timeout poisoned reachability: %v", err)
	}
	sys.failCode, sys.failText = "", ""
	envelope.ID = "after-timeout"
	envelope.Type = "fixture.echo"
	envelope.Payload = json.RawMessage(`{"text":"actor-still-usable"}`)
	a.call(sys, actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), envelope))
	if sys.failCode != "" || sys.reply["text"] != "actor-still-usable" {
		t.Fatalf("actor call after timeout: reply=%v fail=%s %s", sys.reply, sys.failCode, sys.failText)
	}
	t.Log("actor mapped a request with nil ExpiresAt to mcp_timeout, preserved reachability, and served the next call")
}

func verifyTimeoutAndReuse(t *testing.T, c *client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _, err := c.callTool(ctx, "never_returns", json.RawMessage(`{}`), false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("never_returns error=%v, want deadline exceeded", err)
	}
	result, _, err := c.callTool(context.Background(), "echo", json.RawMessage(`{"text":"after-timeout"}`), false)
	if err != nil {
		t.Fatalf("echo after timeout: %v", err)
	}
	payload, _, err := translateCallResult(result)
	if err != nil || payload["text"] != "after-timeout" {
		t.Fatalf("echo after timeout payload=%v err=%v", payload, err)
	}
	t.Log("never_returns timed out and the same client remained usable")
}

func verifyHTTPConcurrency(t *testing.T, c *client) {
	t.Helper()
	type outcome struct {
		name   string
		result callResult
		err    error
	}
	results := make(chan outcome, 2)
	go func() {
		result, _, err := c.callTool(context.Background(), "slow_task", json.RawMessage(`{"seconds":0.4}`), false)
		results <- outcome{name: "slow", result: result, err: err}
	}()
	time.Sleep(30 * time.Millisecond)
	go func() {
		result, _, err := c.callTool(context.Background(), "echo", json.RawMessage(`{"text":"fast"}`), false)
		results <- outcome{name: "echo", result: result, err: err}
	}()
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent calls: first=%s err=%v second=%s err=%v", first.name, first.err, second.name, second.err)
	}
	if first.name != "echo" {
		t.Fatalf("fast HTTP call was blocked behind slow call: first=%s", first.name)
	}
	t.Log("concurrent HTTP echo completed before slow_task")
}

func verifyReferenceClient(t *testing.T, c *client) {
	t.Helper()
	discover, err := c.discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var serverInfo struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(discover.Meta.ServerInfo, &serverInfo); err != nil {
		t.Fatalf("decode server info %s: %v", discover.Meta.ServerInfo, err)
	}
	if serverInfo.Name != "mcp-v2-reference-testserver" || serverInfo.Version != "0.1.0" {
		t.Fatalf("server info=%+v", serverInfo)
	}
	if discover.Instructions != "Deterministic MCP 2026-07-28 protocol and tool conformance fixture." {
		t.Fatalf("instructions=%q", discover.Instructions)
	}
	if !reflect.DeepEqual(discover.SupportedVersions, []string{protocolVersion}) {
		t.Fatalf("supportedVersions=%v", discover.SupportedVersions)
	}
	listing, err := c.listTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Tools) != 15 {
		t.Fatalf("tools=%d", len(listing.Tools))
	}
	byName := make(map[string]tool, len(listing.Tools))
	for _, item := range listing.Tools {
		byName[item.Name] = item
	}
	createOrder := translateTool(byName["create_order"])
	if !bytes.Equal(createOrder.InputSchema, byName["create_order"].InputSchema) ||
		!bytes.Contains(createOrder.InputSchema, []byte(`"$defs"`)) {
		t.Fatalf("create_order input schema changed: got=%s want=%s", createOrder.InputSchema, byName["create_order"].InputSchema)
	}
	structured := translateTool(byName["structured_report"])
	if !bytes.Equal(structured.OutputSchema, byName["structured_report"].OutputSchema) {
		t.Fatalf("structured_report output schema changed: got=%s want=%s", structured.OutputSchema, byName["structured_report"].OutputSchema)
	}
	if stringsContains(createOrder.Notes, string(createOrder.InputSchema)) || stringsContains(structured.Notes, string(structured.OutputSchema)) {
		t.Fatalf("schema leaked into notes: create=%q structured=%q", createOrder.Notes, structured.Notes)
	}
	result, _, err := c.callTool(context.Background(), "echo", json.RawMessage(`{"text":"go-mcp"}`), false)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := translateCallResult(result)
	if err != nil || payload["text"] != "go-mcp" {
		t.Fatalf("echo payload=%v err=%v", payload, err)
	}
	failed, _, err := c.callTool(context.Background(), "fail_tool_error", json.RawMessage(`{}`), false)
	if err != nil || !failed.IsError {
		t.Fatalf("tool failure=%+v err=%v", failed, err)
	}
	_, _, err = c.callTool(context.Background(), "fail_protocol_error", json.RawMessage(`{}`), false)
	var protocolErr *rpcError
	if !errors.As(err, &protocolErr) || protocolErr.Code != -32602 {
		t.Fatalf("protocol failure=%T %v", err, err)
	}
}

func verifyMRTR(t *testing.T, c *client) {
	t.Helper()
	first, _, err := c.callTool(context.Background(), "book_ticket", json.RawMessage(`{"destination":"Singapore"}`), false)
	if err != nil {
		t.Fatalf("book_ticket first round: %v", err)
	}
	payload, _, err := translateCallResult(first)
	if err != nil {
		t.Fatal(err)
	}
	continuation, ok := payload["_continuation"].(map[string]any)
	if !ok || continuation["reason"] != "input_required" || continuation["state"] == "" {
		t.Fatalf("first continuation=%#v", payload["_continuation"])
	}
	retry, err := json.Marshal(map[string]any{
		"destination": "Singapore",
		"_continuation": map[string]any{
			"responses": map[string]any{
				"confirm_booking": map[string]any{"action": "accept", "content": map[string]any{"confirm": true}},
			},
			"state": continuation["state"],
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	final, _, err := c.callTool(context.Background(), "book_ticket", retry, false)
	if err != nil {
		t.Fatalf("book_ticket retry: %v", err)
	}
	finalPayload, _, err := translateCallResult(final)
	if err != nil {
		t.Fatal(err)
	}
	var ids struct {
		Ticket           string `json:"ticket"`
		InitialRequestID string `json:"initialRequestId"`
		RetryRequestID   string `json:"retryRequestId"`
	}
	if err := json.Unmarshal([]byte(finalPayload["text"].(string)), &ids); err != nil {
		t.Fatal(err)
	}
	if ids.Ticket != "TICKET-SINGAPORE" || ids.InitialRequestID == ids.RetryRequestID {
		t.Fatalf("MRTR final=%+v", ids)
	}
	t.Logf("MRTR completed with distinct ids initial=%s retry=%s", ids.InitialRequestID, ids.RetryRequestID)
}

type countingTransport struct {
	transport
	mu    sync.Mutex
	calls map[string]int
}

func (t *countingTransport) RoundTrip(ctx context.Context, request rpcRequest, name string) (rpcResponse, responseInfo, error) {
	t.mu.Lock()
	t.calls[request.Method]++
	t.mu.Unlock()
	return t.transport.RoundTrip(ctx, request, name)
}

func (t *countingTransport) count(method string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls[method]
}

func verifyTTLRefreshAndPromptCache(t *testing.T, c *client) {
	t.Helper()
	counter := &countingTransport{transport: c.transport, calls: make(map[string]int)}
	c.transport = counter
	var prompts struct {
		TTLMS int64 `json:"ttlMs"`
	}
	if _, err := c.cachedRequest(context.Background(), "prompts/list", "", nil, &prompts); err != nil {
		t.Fatal(err)
	}
	if _, err := c.cachedRequest(context.Background(), "prompts/list", "", nil, &prompts); err != nil {
		t.Fatal(err)
	}
	if prompts.TTLMS != 60_000 || counter.count("prompts/list") != 1 {
		t.Fatalf("prompts cache ttl=%d calls=%d", prompts.TTLMS, counter.count("prompts/list"))
	}

	a := &mcpActor{cfg: Config{Name: "ttl", Transport: transportHTTP}, client: c}
	sys := &terminalRecorder{}
	a.refresh(sys, context.Background())
	if got := len(a.snapshot.types); got != 15 {
		t.Fatalf("initial types=%d", got)
	}
	if _, _, err := c.callTool(context.Background(), "toggle_extra_tool", json.RawMessage(`{}`), false); err != nil {
		t.Fatal(err)
	}
	a.refresh(sys, context.Background())
	if got := len(a.snapshot.types); got != 16 {
		t.Fatalf("types after enabling extra_tool=%d", got)
	}
	if _, ok := a.snapshot.types["ttl.extra_tool"]; !ok {
		t.Fatal("enabled snapshot omitted ttl.extra_tool")
	}
	if _, _, err := c.callTool(context.Background(), "toggle_extra_tool", json.RawMessage(`{}`), false); err != nil {
		t.Fatal(err)
	}
	a.refresh(sys, context.Background())
	if got := len(a.snapshot.types); got != 15 {
		t.Fatalf("types after disabling extra_tool=%d", got)
	}
	if _, ok := a.snapshot.types["ttl.extra_tool"]; ok {
		t.Fatal("disabled snapshot retained ttl.extra_tool")
	}
	if counter.count("tools/list") < 3 {
		t.Fatalf("ttlMs=0 tools/list calls=%d, want each refresh to pull", counter.count("tools/list"))
	}
	t.Logf("ttl refresh tools/list calls=%d; prompts/list calls=%d", counter.count("tools/list"), counter.count("prompts/list"))
}

type describeRecorder struct {
	actorbase.Sys
	reply any
	fail  string
}

func (s *describeRecorder) Self() actor.ActorID { return "tool:mcp-test" }
func (s *describeRecorder) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.reply = value
	return "reply", nil
}
func (s *describeRecorder) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.fail = code + ": " + detail
	return "fail", nil
}

func TestRejectsNonV2ServersWithActionableDescribe(t *testing.T) {
	for _, tc := range []struct {
		name       string
		response   rpcResponse
		wantDetail string
	}{
		{name: "method missing", response: rpcResponse{Error: &rpcError{Code: -32601, Message: "Method not found"}}, wantDetail: "not a 2026-07-28 v2 server"},
		{name: "old advertised version", response: rpcResponse{Result: json.RawMessage(`{"resultType":"complete","supportedVersions":["2025-11-25"],"capabilities":{},"ttlMs":60000,"cacheScope":"public"}`)}, wantDetail: "2025-11-25"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				var request rpcRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				response := tc.response
				response.JSONRPC = "2.0"
				response.ID = json.RawMessage(fmt.Sprint(request.ID))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			})
			c := testClient(&httpTransport{
				url:    "http://old-server.test/mcp",
				client: &http.Client{Transport: handlerRoundTripper{handler: handler}},
			})
			a := &mcpActor{cfg: Config{Name: "old", Transport: transportHTTP}, client: c}
			a.refresh(&terminalRecorder{}, context.Background())
			if err := a.currentLastError(); err == nil || !stringsContains(err.Error(), tc.wantDetail) {
				t.Fatalf("last error=%v", err)
			}
		})
	}
}

type handlerRoundTripper struct{ handler http.Handler }

func (t handlerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	t.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

func freeTCPPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitTCP(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not listen on %s", address)
}

func stringsContains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
