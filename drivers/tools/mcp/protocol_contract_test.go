package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// describe answers with its own id; the embedded Sys is nil, so supply it.
func (s *terminalRecorder) Self() actor.ActorID { return "tool:fixture" }

type handlerTransport struct {
	mu     sync.Mutex
	handle func(rpcRequest) (json.RawMessage, *rpcError, error)
	calls  map[string]int
}

func (t *handlerTransport) RoundTrip(_ context.Context, request rpcRequest, _ string) (rpcResponse, responseInfo, error) {
	t.mu.Lock()
	t.calls[request.Method]++
	t.mu.Unlock()
	result, protocolErr, err := t.handle(request)
	return rpcResponse{
		JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(request.ID)),
		Result: result, Error: protocolErr,
	}, responseInfo{ContentType: "application/json"}, err
}

func (*handlerTransport) Close() error { return nil }

func (t *handlerTransport) count(method string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls[method]
}

func testClient(transport transport) *client {
	return &client{transport: transport, cache: make(map[string]cachedResult), now: time.Now}
}

func TestCacheHintsControlRefetchWithoutLeakingIntoContract(t *testing.T) {
	transport := &handlerTransport{calls: make(map[string]int)}
	transport.handle = func(request rpcRequest) (json.RawMessage, *rpcError, error) {
		switch request.Method {
		case "server/discover":
			return json.RawMessage(`{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{},"instructions":"fixture","ttlMs":60000,"cacheScope":"public"}`), nil, nil
		case "tools/list":
			return json.RawMessage(`{"resultType":"complete","tools":[],"ttlMs":0,"cacheScope":"public"}`), nil, nil
		case "prompts/list":
			return json.RawMessage(`{"resultType":"complete","prompts":[],"ttlMs":60000,"cacheScope":"public"}`), nil, nil
		default:
			return nil, nil, fmt.Errorf("unexpected method %s", request.Method)
		}
	}
	c := testClient(transport)
	for range 2 {
		if _, err := c.discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := c.listTools(context.Background()); err != nil {
			t.Fatal(err)
		}
		var prompts struct {
			TTLMS int64 `json:"ttlMs"`
		}
		if _, err := c.cachedRequest(context.Background(), "prompts/list", "", nil, &prompts); err != nil {
			t.Fatal(err)
		}
	}
	if got := transport.count("server/discover"); got != 1 {
		t.Fatalf("server/discover calls=%d, want cached", got)
	}
	if got := transport.count("prompts/list"); got != 1 {
		t.Fatalf("prompts/list calls=%d, want cached", got)
	}
	if got := transport.count("tools/list"); got != 2 {
		t.Fatalf("tools/list calls=%d, want ttlMs=0 refetch", got)
	}
}

func TestMRTRDeclaresElicitationCarriesContinuationAndMintsNewID(t *testing.T) {
	var firstID int64
	transport := &handlerTransport{calls: make(map[string]int)}
	transport.handle = func(request rpcRequest) (json.RawMessage, *rpcError, error) {
		params, ok := request.Params.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("params type %T", request.Params)
		}
		meta := params["_meta"].(map[string]any)
		capabilities := meta["io.modelcontextprotocol/clientCapabilities"].(map[string]any)
		elicitation, ok := capabilities["elicitation"].(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("elicitation capability=%#v", capabilities)
		}
		if _, ok := elicitation["form"].(map[string]any); !ok {
			return nil, nil, fmt.Errorf("form elicitation capability=%#v", elicitation)
		}
		if firstID == 0 {
			firstID = request.ID
			return json.RawMessage(`{"resultType":"input_required","inputRequests":{"confirm":{"method":"elicitation/create","params":{"message":"Continue?"}}},"requestState":"opaque"}`), nil, nil
		}
		if request.ID == firstID {
			return nil, nil, fmt.Errorf("retry reused JSON-RPC id %d", request.ID)
		}
		if params["requestState"] != "opaque" {
			return nil, nil, fmt.Errorf("requestState=%#v", params["requestState"])
		}
		responses := params["inputResponses"].(map[string]any)
		if _, ok := responses["confirm"]; !ok {
			return nil, nil, fmt.Errorf("inputResponses=%#v", responses)
		}
		arguments := params["arguments"].(map[string]json.RawMessage)
		if _, leaked := arguments["_continuation"]; leaked {
			return nil, nil, fmt.Errorf("continuation leaked into tool arguments")
		}
		return json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"done"}],"isError":false}`), nil, nil
	}
	c := testClient(transport)
	first, _, err := c.callTool(context.Background(), "interactive", json.RawMessage(`{"destination":"Singapore"}`), false)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := translateCallResult(first)
	if err != nil {
		t.Fatal(err)
	}
	continuation := payload["_continuation"].(map[string]any)
	retry := json.RawMessage(`{"destination":"Singapore","_continuation":{"answers":{"confirm":{"action":"accept","content":{"confirm":true}}},"state":"opaque"}}`)
	final, _, err := c.callTool(context.Background(), "interactive", retry, false)
	if err != nil {
		t.Fatal(err)
	}
	finalPayload, _, err := translateCallResult(final)
	if err != nil || continuation["reason"] != "input_required" || finalPayload["text"] != "done" {
		t.Fatalf("first=%#v final=%#v err=%v", payload, finalPayload, err)
	}
}

// A tool may legitimately declare a parameter named "continuation"; the MRTR
// control field is underscore-prefixed precisely so it cannot claim that name.
// Without this the adapter would strip a real argument and the tool would be
// called with it missing — while describe kept advertising it.
func TestToolParameterNamedContinuationIsNotClaimedByMRTR(t *testing.T) {
	transport := &handlerTransport{calls: make(map[string]int)}
	var seen map[string]json.RawMessage
	transport.handle = func(request rpcRequest) (json.RawMessage, *rpcError, error) {
		params, ok := request.Params.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("params type %T", request.Params)
		}
		if _, control := params["inputResponses"]; control {
			return nil, nil, fmt.Errorf("plain argument was promoted to inputResponses: %#v", params)
		}
		if _, control := params["requestState"]; control {
			return nil, nil, fmt.Errorf("plain argument was promoted to requestState: %#v", params)
		}
		seen = params["arguments"].(map[string]json.RawMessage)
		return json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"ok"}],"isError":false}`), nil, nil
	}
	c := testClient(transport)
	if _, _, err := c.callTool(context.Background(), "planner",
		json.RawMessage(`{"continuation":{"step":2},"topic":"atoll"}`), false); err != nil {
		t.Fatal(err)
	}
	raw, ok := seen["continuation"]
	if !ok {
		t.Fatalf("the tool's own continuation argument was stripped; arguments=%#v", seen)
	}
	if string(raw) != `{"step":2}` {
		t.Fatalf("continuation argument mangled: %s", raw)
	}
}

// A transient refresh failure must not be self-perpetuating. lastError is
// reporting material, never an admission gate: reachability is the outcome of
// send→terminal, never a stored field. Gating either path on it produces an
// absorbing state — the server recovers, the actor never does (真机复现
// 2026-08-14). These two tests pin both halves of that escape.
func TestDescribeRefreshesEvenWhenLastErrorIsSet(t *testing.T) {
	transport := &handlerTransport{calls: make(map[string]int)}
	transport.handle = func(request rpcRequest) (json.RawMessage, *rpcError, error) {
		switch request.Method {
		case "server/discover":
			return json.RawMessage(`{"supportedVersions":["2026-07-28"],"instructions":"back","ttlMs":0}`), nil, nil
		case "tools/list":
			return json.RawMessage(`{"tools":[{"name":"echo","description":"e","inputSchema":{"type":"object"}}],"ttlMs":0}`), nil, nil
		}
		return nil, nil, fmt.Errorf("unexpected %s", request.Method)
	}
	a := &mcpActor{cfg: Config{Name: "fixture"}, client: testClient(transport)}
	a.setLastError(errors.New("dial tcp: connection refused"))

	sys := &terminalRecorder{}
	a.describe(sys, actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "d1", Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{}`),
	}))

	if got := transport.count("tools/list"); got == 0 {
		t.Fatal("describe skipped refresh while lastError was set: the outage is now permanent")
	}
	if err := a.currentLastError(); err != nil {
		t.Fatalf("a successful refresh must clear lastError, got %v", err)
	}
}

func TestCallIsNotGatedByAStaleLastError(t *testing.T) {
	transport := &handlerTransport{calls: make(map[string]int)}
	transport.handle = func(rpcRequest) (json.RawMessage, *rpcError, error) {
		return json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"ok"}],"isError":false}`), nil, nil
	}
	a := &mcpActor{
		cfg: Config{Name: "fixture", CallTimeoutMS: 1_000}, client: testClient(transport),
		snapshot: snapshot{tools: map[string]string{"fixture.echo": "echo"}},
	}
	a.setLastError(errors.New("dial tcp: connection refused"))

	sys := &terminalRecorder{}
	a.call(sys, actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "c1", Kind: message.KindRequest, Type: "fixture.echo", Payload: json.RawMessage(`{}`),
	}))

	if sys.failCode != "" {
		t.Fatalf("a stale lastError blocked the call before it was attempted: %s %s", sys.failCode, sys.failText)
	}
	if transport.count("tools/call") == 0 {
		t.Fatal("the call never reached the server: reachability was decided from a stored field")
	}
	if sys.reply["text"] != "ok" {
		t.Fatalf("reply=%#v", sys.reply)
	}
}

func TestActorMapsInputRequiredToCompletedReply(t *testing.T) {
	transport := &handlerTransport{calls: make(map[string]int)}
	transport.handle = func(rpcRequest) (json.RawMessage, *rpcError, error) {
		return json.RawMessage(`{"resultType":"input_required","inputRequests":{"confirm":{"method":"elicitation/create","params":{"message":"Continue?"}}},"requestState":"opaque"}`), nil, nil
	}
	a := &mcpActor{
		cfg: Config{Name: "fixture", CallTimeoutMS: 1_000}, client: testClient(transport),
		snapshot: snapshot{tools: map[string]string{"fixture.interactive": "interactive"}},
	}
	sys := &terminalRecorder{}
	a.call(sys, actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "input-required", Kind: message.KindRequest, Type: "fixture.interactive", Payload: json.RawMessage(`{}`),
	}))
	if sys.failCode != "" {
		t.Fatalf("input_required was failed: %s %s", sys.failCode, sys.failText)
	}
	continuation, ok := sys.reply["_continuation"].(map[string]any)
	if !ok || continuation["reason"] != "input_required" || continuation["state"] != "opaque" {
		t.Fatalf("completed reply=%#v", sys.reply)
	}
}
