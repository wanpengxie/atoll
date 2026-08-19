// Package mcpcodec implements the provider-neutral MCP wire codec used by
// transports whose native callback language is MCP. It owns no actor or Sys;
// execution is supplied as a ToolPort closure by the provider adapter.
package mcpcodec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

const (
	ProtocolVersion = "2025-11-25"
	ModernVersion   = "2026-07-28"
)
const maxRememberedCalls = 1024

type InvokeFunc func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult

type callRecord struct {
	done     chan struct{}
	cancel   context.CancelFunc
	response json.RawMessage
}

type Server struct {
	surface toolsurface.Surface
	epoch   string
	life    context.Context
	mu      sync.Mutex
	calls   map[string]*callRecord
}

func New(life context.Context, surface toolsurface.Surface) *Server {
	if life == nil {
		life = context.Background()
	}
	return &Server{surface: surface, epoch: uuid.NewString(), life: life, calls: map[string]*callRecord{}}
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, call := range s.calls {
		if call.cancel != nil {
			call.cancel()
		}
	}
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (s *Server) Handle(ctx context.Context, raw json.RawMessage, invoke InvokeFunc) json.RawMessage {
	var req request
	if err := json.Unmarshal(raw, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
		return rpcError(nil, -32600, "invalid JSON-RPC request")
	}
	switch req.Method {
	case "server/discover":
		return rpcResult(req.ID, completeResult(map[string]any{
			"supportedVersions": []string{ModernVersion},
			"capabilities":      map[string]any{"tools": map[string]any{}},
			"instructions":      "Nine fixed Atoll channel tools for member discovery, the system door, invocation, and asynchronous result collection.",
			"ttlMs":             60_000,
			"cacheScope":        "private",
		}))
	case "initialize":
		version := ProtocolVersion
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if json.Unmarshal(req.Params, &params) == nil && params.ProtocolVersion != "" {
			version = params.ProtocolVersion
		}
		return rpcResult(req.ID, map[string]any{"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": serverInfo()})
	case "notifications/initialized":
		return nil
	case "ping":
		return rpcResultFor(req, map[string]any{})
	case "tools/list":
		tools := make([]map[string]any, 0, len(s.surface.Entries()))
		for _, entry := range s.surface.Entries() {
			tools = append(tools, map[string]any{"name": entry.Wire, "description": entry.Spec.Description, "inputSchema": entry.Spec.Schema})
		}
		result := map[string]any{"tools": tools}
		if modern(req) {
			result["ttlMs"] = 60_000
			result["cacheScope"] = "private"
		}
		return rpcResultFor(req, result)
	case "tools/call":
		return s.call(ctx, req, invoke)
	case "notifications/cancelled":
		s.cancel(req.Params)
		return nil
	default:
		return rpcError(req.ID, -32601, "method not found: "+req.Method)
	}
}

func (s *Server) call(ctx context.Context, req request, invoke InvokeFunc) json.RawMessage {
	key, ok := canonicalID(req.ID)
	if !ok {
		return rpcError(req.ID, -32600, "tools/call requires a string or numeric id")
	}
	s.mu.Lock()
	if prior := s.calls[key]; prior != nil {
		done := prior.done
		s.mu.Unlock()
		select {
		case <-done:
			s.mu.Lock()
			response := append(json.RawMessage(nil), prior.response...)
			s.mu.Unlock()
			return responseWithID(response, req.ID)
		case <-ctx.Done():
			return rpcError(req.ID, -32800, "request cancelled")
		case <-s.life.Done():
			return rpcError(req.ID, -32800, "connection retired")
		}
	}
	if len(s.calls) >= maxRememberedCalls {
		s.mu.Unlock()
		return toolResultFor(req, toolsurface.ErrorText("internal_error", "MCP request table limit reached", "Start a fresh worker connection before issuing more tool calls"), true)
	}
	callCtx, cancel := context.WithCancel(s.life)
	stopClientCancel := context.AfterFunc(ctx, cancel)
	record := &callRecord{done: make(chan struct{}), cancel: cancel}
	s.calls[key] = record
	s.mu.Unlock()

	response := s.execute(callCtx, req, key, invoke)
	stopClientCancel()
	cancel()
	s.mu.Lock()
	record.cancel = nil
	record.response = append(json.RawMessage(nil), response...)
	close(record.done)
	s.mu.Unlock()
	return response
}

func (s *Server) execute(ctx context.Context, req request, key string, invoke InvokeFunc) json.RawMessage {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(req.Params, &params) != nil || params.Name == "" {
		return rpcError(req.ID, -32602, "invalid tools/call params")
	}
	canonical, ok := s.surface.Canonical(params.Name)
	if !ok {
		return rpcError(req.ID, -32601, "unknown tool: "+params.Name)
	}
	if len(params.Arguments) == 0 {
		params.Arguments = json.RawMessage(`{}`)
	}
	var object map[string]any
	if json.Unmarshal(params.Arguments, &object) != nil || object == nil {
		return rpcError(req.ID, -32602, "tool arguments must be an object")
	}
	if invoke == nil {
		return toolResultFor(req, toolsurface.ErrorText("internal_error", "tool host unavailable", "Retry from an active turn"), true)
	}
	result := invoke(ctx, driverproto.ToolInvocation{CallID: driverproto.ProviderToolCallID(s.epoch + ":" + key), Name: canonical, Params: params.Arguments})
	result = s.surface.MapResult(result)
	return toolResultFor(req, result.Text, result.IsError)
}

func (s *Server) cancel(raw json.RawMessage) {
	var params struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(raw, &params) != nil {
		return
	}
	key, ok := canonicalID(params.RequestID)
	if !ok {
		return
	}
	s.mu.Lock()
	record := s.calls[key]
	if record != nil && record.cancel != nil {
		record.cancel()
	}
	s.mu.Unlock()
}

func canonicalID(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", false
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return "", false
		}
		encoded, _ := json.Marshal(value)
		return "s:" + string(encoded), true
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if dec.Decode(&value) != nil {
		return "", false
	}
	number, ok := value.(json.Number)
	if !ok {
		return "", false
	}
	rat := new(big.Rat)
	if _, ok := rat.SetString(number.String()); !ok {
		float, _, err := big.ParseFloat(number.String(), 10, 256, big.ToNearestEven)
		if err != nil {
			return "", false
		}
		rat, _ = float.Rat(nil)
	}
	return "n:" + rat.RatString(), true
}

// BusyResponse returns a model-readable tools/call result without executing
// the tool. It preserves the client's JSON-RPC id for an admission rejection.
func BusyResponse(raw json.RawMessage) json.RawMessage {
	var req request
	if json.Unmarshal(raw, &req) != nil {
		return rpcError(nil, -32600, "invalid JSON-RPC request")
	}
	return toolResultFor(req, toolsurface.ErrorText("internal_error", "tool concurrency limit reached", "Wait for an in-flight tool call to finish, then retry if safe"), true)
}

func toolResultFor(req request, text string, isError bool) json.RawMessage {
	return rpcResultFor(req, map[string]any{"content": []map[string]any{{"type": "text", "text": text}}, "isError": isError})
}

func rpcResultFor(req request, result map[string]any) json.RawMessage {
	if modern(req) {
		result = completeResult(result)
	}
	return rpcResult(req.ID, result)
}

func completeResult(result map[string]any) map[string]any {
	result["resultType"] = "complete"
	meta, _ := result["_meta"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["io.modelcontextprotocol/serverInfo"] = serverInfo()
	result["_meta"] = meta
	return result
}

func serverInfo() map[string]any {
	return map[string]any{"name": toolsurface.ClaudeServer, "version": "1"}
}

func modern(req request) bool {
	if req.Method == "server/discover" {
		return true
	}
	var params struct {
		Meta map[string]any `json:"_meta"`
	}
	if json.Unmarshal(req.Params, &params) != nil {
		return false
	}
	version, _ := params.Meta["io.modelcontextprotocol/protocolVersion"].(string)
	return version == ModernVersion
}

func rpcResult(id json.RawMessage, result any) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": idValue(id), "result": result})
	return raw
}

func rpcError(id json.RawMessage, code int, message string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": idValue(id), "error": map[string]any{"code": code, "message": message}})
	return raw
}

func idValue(raw json.RawMessage) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&value) != nil {
		return nil
	}
	return value
}

func responseWithID(raw, id json.RawMessage) json.RawMessage {
	var response map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&response) != nil {
		return raw
	}
	response["id"] = idValue(id)
	out, _ := json.Marshal(response)
	return out
}

func (s *Server) String() string { return fmt.Sprintf("mcp server epoch=%s", s.epoch) }
