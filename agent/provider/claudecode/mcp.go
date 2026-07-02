package claudecode

import (
	"context"
	"encoding/json"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/lib/metatool"
)

// buildMCPServer wraps the 7 atoll meta-tools as an in-process SDK MCP server
// — the claude looper's path to the shared actor-invocation machinery (every
// looper must be able to inject the standard meta-tools into its own tool
// surface and invoke them; the go-kimi looper does the same via
// AdditionalTools). Each handler bridges to the held shell with the
// in-flight turn's RuntimeContext. The handler reads b.shell LAZILY (the
// server is built before Start assigns it; handlers only fire mid-turn, well
// after) so the binding is order-independent.
func (b *Bridge) buildMCPServer() *claude.McpSdkServerConfig {
	catalog := metatool.MetaTools()
	tools := make([]*claude.SdkMcpTool, 0, len(catalog))
	for _, mt := range catalog {
		mt := mt
		var schema map[string]any
		if len(mt.Spec.Schema) > 0 {
			_ = json.Unmarshal(mt.Spec.Schema, &schema)
		}
		handler := func(ctx context.Context, args map[string]any) (claude.MCPToolResult, error) {
			params, err := json.Marshal(args)
			if err != nil {
				return errResult(err.Error()), nil
			}
			rv := mt.Execute(ctx, params, b.shell, b.currentRC())
			return toMCPResult(rv), nil
		}
		tools = append(tools, claude.NewMCPTool(mt.Spec.Name, mt.Spec.Description, schema, handler))
	}
	return claude.CreateSdkMcpServer("atoll", "1.0.0", tools...)
}

// toMCPResult materialises a metatool.ResultValue as an MCP tool result. The
// value (the metatool's structured result map) is rendered to JSON text since
// the model consumes it as a tool message.
func toMCPResult(rv metatool.ResultValue) claude.MCPToolResult {
	var text string
	if bts, err := json.Marshal(rv.Value); err == nil {
		text = string(bts)
	}
	return claude.MCPToolResult{
		Content: []claude.MCPContent{{Type: "text", Text: text}},
		IsError: rv.IsError,
	}
}

func errResult(msg string) claude.MCPToolResult {
	return claude.MCPToolResult{
		Content: []claude.MCPContent{{Type: "text", Text: msg}},
		IsError: true,
	}
}
