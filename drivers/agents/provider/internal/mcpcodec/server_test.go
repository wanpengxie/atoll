package mcpcodec

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

func testServer(t *testing.T, life context.Context) *Server {
	t.Helper()
	surface, err := toolsurface.Assemble([]driverproto.ToolSpec{{Name: "call_actor", Description: "call", Schema: json.RawMessage(`{"type":"object"}`)}}, toolsurface.Claude, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	return New(life, surface)
}

func rpc(method, id, params string) json.RawMessage {
	if id == "" {
		return json.RawMessage(`{"jsonrpc":"2.0","method":"` + method + `","params":` + params + `}`)
	}
	return json.RawMessage(`{"jsonrpc":"2.0","id":` + id + `,"method":"` + method + `","params":` + params + `}`)
}

func TestMCPHandshakeCatalogPingAndNoInteractionMeta(t *testing.T) {
	s := testServer(t, context.Background())
	init := string(s.Handle(context.Background(), rpc("initialize", `"i"`, `{"protocolVersion":"2024-11-05"}`), nil))
	list := string(s.Handle(context.Background(), rpc("tools/list", `2`, `{}`), nil))
	ping := string(s.Handle(context.Background(), rpc("ping", `3`, `{}`), nil))
	if !strings.Contains(init, `"serverInfo"`) || !strings.Contains(list, `"call_actor"`) || strings.Contains(list, "requiresUserInteraction") || !strings.Contains(ping, `"result":{}`) {
		t.Fatalf("init=%s list=%s ping=%s", init, list, ping)
	}
	if got := s.Handle(context.Background(), rpc("notifications/initialized", "", `{}`), nil); got != nil {
		t.Fatalf("initialized response=%s", got)
	}
}

func TestModernDiscoveryAndCatalogCarryRequiredWireFields(t *testing.T) {
	s := testServer(t, context.Background())
	meta := `{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}`
	discover := string(s.Handle(context.Background(), rpc("server/discover", `"d"`, meta), nil))
	list := string(s.Handle(context.Background(), rpc("tools/list", `"l"`, meta), nil))
	for name, response := range map[string]string{"discover": discover, "tools/list": list} {
		for _, required := range []string{`"resultType":"complete"`, `"io.modelcontextprotocol/serverInfo"`} {
			if !strings.Contains(response, required) {
				t.Fatalf("%s missing %s: %s", name, required, response)
			}
		}
	}
	if !strings.Contains(discover, `"supportedVersions":["2026-07-28"]`) || !strings.Contains(list, `"cacheScope":"private"`) {
		t.Fatalf("discover=%s list=%s", discover, list)
	}
}

func TestDuplicateJSONRPCIDInFlightAndCompletedExecutesOnce(t *testing.T) {
	s := testServer(t, context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	invoke := func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return driverproto.ToolResult{Text: `{"ok":true}`}
	}
	request1 := rpc("tools/call", `1`, `{"name":"call_actor","arguments":{}}`)
	requestEquivalent := rpc("tools/call", `1.0`, `{"name":"call_actor","arguments":{}}`)
	first, second := make(chan string, 1), make(chan string, 1)
	go func() { first <- string(s.Handle(context.Background(), request1, invoke)) }()
	<-entered
	go func() { second <- string(s.Handle(context.Background(), requestEquivalent, invoke)) }()
	close(release)
	a, b := <-first, <-second
	if calls.Load() != 1 || !strings.Contains(a, `"id":1`) || !strings.Contains(b, `"id":1.0`) {
		t.Fatalf("calls=%d first=%s second=%s", calls.Load(), a, b)
	}
	completed := string(s.Handle(context.Background(), request1, invoke))
	if calls.Load() != 1 || !strings.Contains(completed, `"id":1`) {
		t.Fatalf("completed replay calls=%d got=%s", calls.Load(), completed)
	}
}

func TestCancellationPropagatesAndLateCancelIsIgnored(t *testing.T) {
	s := testServer(t, context.Background())
	entered := make(chan struct{})
	invoke := func(ctx context.Context, _ driverproto.ToolInvocation) driverproto.ToolResult {
		close(entered)
		<-ctx.Done()
		return driverproto.ToolResult{Text: ctx.Err().Error(), IsError: true}
	}
	done := make(chan json.RawMessage, 1)
	go func() {
		done <- s.Handle(context.Background(), rpc("tools/call", `"cancel-me"`, `{"name":"call_actor","arguments":{}}`), invoke)
	}()
	<-entered
	s.Handle(context.Background(), rpc("notifications/cancelled", "", `{"requestId":"cancel-me"}`), nil)
	select {
	case response := <-done:
		if !strings.Contains(string(response), `"isError":true`) {
			t.Fatalf("cancel response=%s", response)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not reach invocation")
	}
	// Completed and unknown cancellations are deliberately no-ops.
	s.Handle(context.Background(), rpc("notifications/cancelled", "", `{"requestId":"cancel-me"}`), nil)
	s.Handle(context.Background(), rpc("notifications/cancelled", "", `{"requestId":"unknown"}`), nil)
}

func TestOutOfOrderRepliesKeepTheirJSONRPCIDs(t *testing.T) {
	s := testServer(t, context.Background())
	invoke := func(_ context.Context, in driverproto.ToolInvocation) driverproto.ToolResult {
		if strings.HasSuffix(string(in.CallID), "n:1") {
			time.Sleep(30 * time.Millisecond)
		}
		return driverproto.ToolResult{Text: `{"call_id":"` + string(in.CallID) + `"}`}
	}
	responses := make(chan string, 2)
	for _, id := range []string{"1", "2"} {
		id := id
		go func() {
			responses <- string(s.Handle(context.Background(), rpc("tools/call", id, `{"name":"call_actor","arguments":{}}`), invoke))
		}()
	}
	a, b := <-responses, <-responses
	if !(strings.Contains(a, `"id":1`) && strings.Contains(b, `"id":2`) || strings.Contains(a, `"id":2`) && strings.Contains(b, `"id":1`)) {
		t.Fatalf("responses mismatched: %s / %s", a, b)
	}
}

func TestGenerationRetirementCancelsInFlight(t *testing.T) {
	life, retire := context.WithCancel(context.Background())
	s := testServer(t, life)
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		s.Handle(context.Background(), rpc("tools/call", `9`, `{"name":"call_actor","arguments":{}}`), func(ctx context.Context, _ driverproto.ToolInvocation) driverproto.ToolResult {
			close(entered)
			<-ctx.Done()
			return driverproto.ToolResult{Text: ctx.Err().Error(), IsError: true}
		})
		close(done)
	}()
	<-entered
	retire()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("generation retirement did not cancel invocation")
	}
}

func TestSlowToolDoesNotBlockCatalogOrPing(t *testing.T) {
	s := testServer(t, context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	go s.Handle(context.Background(), rpc("tools/call", `1`, `{"name":"call_actor","arguments":{}}`), func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult {
		close(entered)
		<-release
		return driverproto.ToolResult{Text: `{"ok":true}`}
	})
	<-entered
	started := time.Now()
	list := s.Handle(context.Background(), rpc("tools/list", `2`, `{}`), nil)
	ping := s.Handle(context.Background(), rpc("ping", `3`, `{}`), nil)
	close(release)
	if time.Since(started) > 100*time.Millisecond || !strings.Contains(string(list), `"call_actor"`) || !strings.Contains(string(ping), `"id":3`) {
		t.Fatalf("slow invocation blocked protocol work: list=%s ping=%s", list, ping)
	}
}

func TestClientTimeoutCancelsCallWithoutRetiringConnection(t *testing.T) {
	s := testServer(t, context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	done := make(chan json.RawMessage, 1)
	go func() {
		done <- s.Handle(ctx, rpc("tools/call", `4`, `{"name":"call_actor","arguments":{}}`), func(callCtx context.Context, _ driverproto.ToolInvocation) driverproto.ToolResult {
			close(entered)
			<-callCtx.Done()
			return driverproto.ToolResult{Text: callCtx.Err().Error(), IsError: true}
		})
	}()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client timeout did not cancel invocation")
	}
	if ping := s.Handle(context.Background(), rpc("ping", `5`, `{}`), nil); !strings.Contains(string(ping), `"id":5`) {
		t.Fatalf("client timeout retired connection: %s", ping)
	}
}

func TestCancelFinalRaceCachesExactlyOneTerminalResponse(t *testing.T) {
	s := testServer(t, context.Background())
	release := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int32
	request := rpc("tools/call", `6`, `{"name":"call_actor","arguments":{}}`)
	done := make(chan json.RawMessage, 1)
	go func() {
		done <- s.Handle(context.Background(), request, func(ctx context.Context, _ driverproto.ToolInvocation) driverproto.ToolResult {
			calls.Add(1)
			close(entered)
			select {
			case <-release:
				return driverproto.ToolResult{Text: `{"ok":true}`}
			case <-ctx.Done():
				return driverproto.ToolResult{Text: ctx.Err().Error(), IsError: true}
			}
		})
	}()
	<-entered
	go s.Handle(context.Background(), rpc("notifications/cancelled", "", `{"requestId":6}`), nil)
	close(release)
	first := <-done
	replayed := s.Handle(context.Background(), request, func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult {
		calls.Add(1)
		return driverproto.ToolResult{Text: `{"duplicate":true}`}
	})
	if calls.Load() != 1 || string(first) != string(replayed) {
		t.Fatalf("calls=%d first=%s replay=%s", calls.Load(), first, replayed)
	}
}
