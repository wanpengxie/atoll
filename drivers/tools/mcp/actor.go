// Package mcp adapts one external MCP server into one ordinary Atoll tool
// actor. The connection facts are configured; the tool surface is discovered
// live from server/discover and tools/list.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

const actorDoc = "External MCP server dynamically adapted as an Atoll tool actor."

type snapshot struct {
	description string
	skillDoc    string
	types       map[string]introspect.TypeMeta
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
	return actorbase.Def{Doc: actorDoc, New: func() (actorbase.Proc, error) {
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
		a.refresh(sys.Life())
	}
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
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
	if msg.Type == introspect.QueryDescribe {
		a.describe(sys, msg)
		return
	}
	a.call(sys, msg)
}

func (a *mcpActor) refresh(ctx context.Context) {
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
	a.mu.Lock()
	a.snapshot = buildSnapshot(a.cfg.Name, discover, tools)
	a.lastError = nil
	a.mu.Unlock()
}

func (a *mcpActor) describe(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return
	}
	// A snapshot says what the server last advertised; a lightweight discover
	// probe says whether that snapshot is currently reachable. It never
	// replaces or empties the last successful tool list.
	if a.client != nil && a.currentLastError() == nil {
		if _, err := a.client.discover(msg.Ctx()); err != nil {
			a.setLastError(err)
		}
	}
	a.mu.RLock()
	current := a.snapshot
	lastError := a.lastError
	a.mu.RUnlock()
	description := current.description
	if description == "" {
		description = "经 mcp 类接入的外部服务"
	}
	if lastError != nil {
		if len(current.types) == 0 {
			description += "; 从未成功连接: " + lastError.Error()
		} else {
			description += "; 当前够不着（保留最后一次成功快照）: " + lastError.Error()
		}
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID: string(sys.Self()), Description: description,
		SkillDoc: current.skillDoc, Types: current.types,
	}, req)
	if !ok {
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("mcp actor does not handle %s", req.Type))
		return
	}
	_, _ = sys.Reply(msg, answer)
}

func (a *mcpActor) call(sys actorbase.Sys, msg actorbase.Msg) {
	a.mu.RLock()
	lastError := a.lastError
	toolName, ok := a.snapshot.tools[msg.Type]
	a.mu.RUnlock()
	if lastError != nil {
		_, _ = sys.Fail(msg, "mcp_unreachable", lastError.Error())
		return
	}
	if !ok {
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("mcp actor does not handle %s", msg.Type))
		return
	}
	if a.client == nil {
		_, _ = sys.Fail(msg, "mcp_unreachable", "MCP client is unavailable")
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
		types:       make(map[string]introspect.TypeMeta, len(list.Tools)),
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

func translateTool(t tool) introspect.TypeMeta {
	meta := introspect.TypeMeta{
		Description:  t.Description,
		AllowedKinds: []string{string(message.KindRequest)},
		Notes:        "过渡形：MCP inputSchema 原文如下；当前 PayloadFields 翻译有损，阶段 3 处理。\n" + string(t.InputSchema),
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if json.Unmarshal(t.InputSchema, &schema) != nil {
		return meta
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		raw := schema.Properties[name]
		var field struct {
			Description string `json:"description"`
			Type        string `json:"type"`
			Enum        []any  `json:"enum"`
		}
		_ = json.Unmarshal(raw, &field)
		details := []string{}
		if field.Type != "" {
			details = append(details, "type: "+field.Type)
		}
		if len(field.Enum) > 0 {
			details = append(details, "enum present")
		}
		if bytesContainSchemaDetail(raw) {
			details = append(details, "see raw schema")
		}
		description := field.Description
		if len(details) > 0 {
			if description != "" {
				description += " "
			}
			description += "(" + strings.Join(details, "; ") + ")"
		}
		meta.PayloadFields = append(meta.PayloadFields, introspect.FieldDoc{
			Name: name, Required: required[name], Description: description,
		})
	}
	return meta
}

func bytesContainSchemaDetail(raw []byte) bool {
	text := string(raw)
	for _, marker := range []string{`"$ref"`, `"oneOf"`, `"anyOf"`, `"items"`, `"properties"`, `"enum"`} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
