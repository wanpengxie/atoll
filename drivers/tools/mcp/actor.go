// Package mcp is a protocol adapter: it turns one external MCP server into one
// ordinary Atoll actor without knowing which concrete service is behind that
// server. The connection facts are configured and the capability surface is
// discovered live. The resulting actor is still KindTool, so this adapter
// lives beside the concrete device, kimi, and xhs drivers under drivers/tools.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const actorDoc = "External MCP server dynamically adapted as an Atoll tool actor."
const typeRefresh = "mcp.refresh"
const refreshInterval = time.Minute

type snapshot struct {
	description string
	skillDoc    string
	types       map[string]introspect.WordSpec
	tools       map[string]string
}

type mcpActor struct {
	cfg       Config
	client    *client
	mu        sync.RWMutex
	snapshot  snapshot
	lastError error
	inflight  sync.WaitGroup
}

func Def(cfg Config) actorbase.Def {
	return actorbase.Def{Manifest: introspect.Manifest{
		Class: "mcp", Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{},
	}, New: func() (actorbase.Proc, error) {
		return (&mcpActor{cfg: cfg}).run, nil
	}}
}

func (a *mcpActor) run(sys actorbase.Sys) error {
	client, err := newClient(a.cfg)
	if err != nil {
		a.lastError = err
	} else {
		a.client = client
		defer func() {
			_ = client.Close()
			a.inflight.Wait()
		}()
		a.refresh(sys, sys.Life())
		_, _ = sys.After(refreshInterval, typeRefresh, struct{}{}, schedule.TimerHomeMemory)
	}
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind == message.KindEvent && msg.Type == typeRefresh {
			a.refresh(sys, sys.Life())
			_, _ = sys.After(refreshInterval, typeRefresh, struct{}{}, schedule.TimerHomeMemory)
			continue
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		if a.cfg.Transport == transportHTTP {
			a.inflight.Add(1)
			go func() {
				defer a.inflight.Done()
				a.handle(sys, msg)
			}()
			continue
		}
		a.handle(sys, msg)
	}
}

func (a *mcpActor) handle(sys actorbase.Sys, msg actorbase.Msg) {
	a.call(sys, msg)
}

func (a *mcpActor) refresh(sys actorbase.Sys, ctx context.Context) {
	discover, err := a.client.discover(ctx)
	if err != nil {
		a.setLastError(err)
		return
	}
	tools, err := a.client.listTools(ctx)
	if err != nil {
		a.setLastError(err)
		return
	}
	next := buildSnapshot(a.cfg.Name, discover, tools)
	raw, err := json.Marshal(next.types)
	if err != nil {
		a.setLastError(err)
		return
	}
	if _, err := sys.State().Put(actorbase.ManifestStateKey, raw); err != nil {
		a.setLastError(err)
		return
	}
	a.mu.Lock()
	a.snapshot = next
	a.lastError = nil
	a.mu.Unlock()
}

func (a *mcpActor) call(sys actorbase.Sys, msg actorbase.Msg) {
	a.mu.RLock()
	lastError := a.lastError
	toolName, ok := a.snapshot.tools[msg.Type]
	everConnected := len(a.snapshot.tools) > 0
	a.mu.RUnlock()
	// lastError is REPORTING material (it feeds describe's self-answer), never an
	// admission gate: reachability is the outcome of send→terminal, never a stored
	// field. Gating here on the last observation would make the actor's own error
	// record decide whether a fresh observation may happen at all — an absorbing
	// state no recovery can escape (the server comes back; this actor never does).
	if !ok {
		// A tool table we never obtained cannot answer "is this type mine?". Saying
		// type_unsupported would assert an absence we have not observed.
		if !everConnected && lastError != nil {
			_, _ = sys.Fail(msg, "mcp_unreachable", lastError.Error())
			return
		}
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("this mcp adapter does not answer %q; call actor.describe on it for the tools the server currently exposes", msg.Type))
		return
	}
	if a.client == nil {
		_, _ = sys.Fail(msg, "mcp_unreachable", "the MCP server backing this adapter is not connected, so no tool on it can be called right now; retry once it reconnects")
		return
	}
	callCtx, cancel := context.WithTimeout(msg.Ctx(), a.callTimeout())
	defer cancel()
	result, _, err := a.client.callTool(callCtx, toolName, msg.Payload, false)
	if err != nil {
		var protocolErr *rpcError
		if errors.As(err, &protocolErr) {
			a.setLastError(nil)
			_, _ = sys.Fail(msg, fmt.Sprintf("mcp_protocol_%d", protocolErr.Code), protocolErr.Error())
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			_, _ = sys.Fail(msg, "mcp_timeout", fmt.Sprintf("MCP tool %s exceeded call timeout %s", toolName, a.callTimeout()))
			return
		}
		if errors.Is(err, context.Canceled) {
			_, _ = sys.Fail(msg, "mcp_cancelled", fmt.Sprintf("MCP tool %s was cancelled", toolName))
			return
		}
		a.setLastError(err)
		_, _ = sys.Fail(msg, "mcp_unreachable", err.Error())
		return
	}
	a.setLastError(nil)
	payload, detail, err := translateCallResult(result)
	if err != nil {
		_, _ = sys.Fail(msg, "mcp_result_invalid", err.Error())
		return
	}
	if result.IsError {
		_, _ = sys.Fail(msg, "mcp_tool_error", detail)
		return
	}
	_, _ = sys.Reply(msg, payload)
}

func (a *mcpActor) callTimeout() time.Duration {
	millis := a.cfg.CallTimeoutMS
	if millis == 0 {
		millis = defaultCallTimeoutMS
	}
	return time.Duration(millis) * time.Millisecond
}

func (a *mcpActor) currentLastError() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastError
}

func (a *mcpActor) setLastError(err error) {
	a.mu.Lock()
	a.lastError = err
	a.mu.Unlock()
}

func translateCallResult(result callResult) (map[string]any, string, error) {
	payload := map[string]any{}
	var texts []string
	var nonText []any
	for _, raw := range result.Content {
		var header struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			return nil, "", fmt.Errorf("decode MCP content: %w", err)
		}
		if header.Type == "text" {
			texts = append(texts, header.Text)
			continue
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, "", fmt.Errorf("decode MCP non-text content: %w", err)
		}
		nonText = append(nonText, value)
	}
	text := strings.Join(texts, "")
	if len(texts) > 0 {
		payload["text"] = text
	}
	if len(nonText) > 0 {
		payload["content"] = nonText
	}
	if len(result.StructuredContent) > 0 && string(result.StructuredContent) != "null" {
		var structured any
		if err := json.Unmarshal(result.StructuredContent, &structured); err != nil {
			return nil, "", fmt.Errorf("decode MCP structuredContent: %w", err)
		}
		payload["structured_content"] = structured
	}
	if result.ResultType == "input_required" {
		continuation := map[string]any{"reason": "input_required"}
		if len(result.InputRequests) > 0 && string(result.InputRequests) != "null" {
			var requests any
			if err := json.Unmarshal(result.InputRequests, &requests); err != nil {
				return nil, "", fmt.Errorf("decode MCP inputRequests: %w", err)
			}
			continuation["requests"] = requests
		}
		if result.RequestState != nil {
			continuation["state"] = *result.RequestState
		}
		if _, hasRequests := continuation["requests"]; !hasRequests && result.RequestState == nil {
			return nil, "", errors.New("MCP input_required result omitted inputRequests and requestState")
		}
		// Underscore-prefixed, following MCP's own reserved-namespace convention
		// (_meta): the control field shares one JSON object with the tool's own
		// arguments, so it needs a prefix no legitimate parameter would claim.
		payload["_continuation"] = continuation
	}
	if text != "" {
		return payload, text, nil
	}
	raw, _ := json.Marshal(payload)
	return payload, string(raw), nil
}

func buildSnapshot(name string, discover discovery, list toolList) snapshot {
	selfDescription := serverSelfDescription(name, discover)
	s := snapshot{
		description: selfDescription,
		types:       make(map[string]introspect.WordSpec, len(list.Tools)),
		tools:       make(map[string]string, len(list.Tools)),
	}
	var doc strings.Builder
	doc.WriteString("# ")
	doc.WriteString(name)
	doc.WriteString("\n\n")
	doc.WriteString(selfDescription)
	doc.WriteString("\n\n## Tools\n")
	for _, tool := range list.Tools {
		typeName := name + "." + tool.Name
		s.types[typeName] = translateTool(tool)
		s.tools[typeName] = tool.Name
		doc.WriteString("\n- `")
		doc.WriteString(typeName)
		doc.WriteString("` — ")
		doc.WriteString(tool.Description)
	}
	s.skillDoc = doc.String()
	return s
}

func serverSelfDescription(name string, discover discovery) string {
	var doc strings.Builder
	doc.WriteString("经 mcp 类接入的外部服务，本地名是 `")
	doc.WriteString(name)
	doc.WriteString("`。")
	if len(discover.Meta.ServerInfo) > 0 && string(discover.Meta.ServerInfo) != "null" {
		doc.WriteString("\n\nserverInfo（server 自报原文）：\n")
		doc.Write(discover.Meta.ServerInfo)
	}
	if discover.Instructions != "" {
		doc.WriteString("\n\ninstructions（server 自报原文）：\n")
		doc.WriteString(discover.Instructions)
	}
	return doc.String()
}

func translateTool(t tool) introspect.WordSpec {
	return introspect.WordSpec{
		Description:  t.Description,
		InputSchema:  append(json.RawMessage(nil), t.InputSchema...),
		OutputSchema: append(json.RawMessage(nil), t.OutputSchema...),
	}
}
