package claudecode

import (
	"context"
	"encoding/json"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/metatool"
)

// buildMCPServer wraps the 7 atoll meta-tools as an in-process SDK MCP server —
// the claude looper's path to the shared actor-invocation machinery. It projects
// the neutral base.BuildMCPCatalog (§2 S2: agentbase出一个 MCP 目录) into claude
// SdkMcpTools; each handler bridges to the engine's held Exec face with the
// in-flight turn's RuntimeContext. Handlers read e.currentRC() lazily (they only
// fire mid-turn), so the binding is order-independent.
func (e *engine) buildMCPServer() *claude.McpSdkServerConfig {
	exec := func(ctx context.Context, mt metatool.MetaTool, params json.RawMessage) metatool.ResultValue {
		return mt.Execute(ctx, params, e.x, e.currentRC())
	}
	catalog := base.BuildMCPCatalog(exec)
	tools := make([]*claude.SdkMcpTool, 0, len(catalog))
	for _, row := range catalog {
		row := row
		var schema map[string]any
		if len(row.Schema) > 0 {
			_ = json.Unmarshal(row.Schema, &schema)
		}
		handler := func(ctx context.Context, args map[string]any) (claude.MCPToolResult, error) {
			params, err := json.Marshal(args)
			if err != nil {
				return errResult(err.Error()), nil
			}
			res := row.Handler(ctx, params)
			return claude.MCPToolResult{
				Content: []claude.MCPContent{{Type: "text", Text: res.Text}},
				IsError: res.IsError,
			}, nil
		}
		tools = append(tools, claude.NewMCPTool(row.Name, row.Description, schema, handler))
	}
	return claude.CreateSdkMcpServer("atoll", "1.0.0", tools...)
}

func errResult(msg string) claude.MCPToolResult {
	return claude.MCPToolResult{
		Content: []claude.MCPContent{{Type: "text", Text: msg}},
		IsError: true,
	}
}
