package base

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/metatool"
)

// mcp.go is 期10 S2 — the心智 binding 物理接口, the public component
// claudecode's private mcp.go升格 into. The contract面 is single: the agent
// skeleton出一个 MCP 目录 (the 7 meta-tool 工具表); how a given engine ingests
// it — native MCP直连, or适配件把目录机械翻译成引擎自家工具形 (go-kimi's
// AdditionalTools) — is适配件内政, downstream of this neutral catalog. transport
// (in-process/stdio/HTTP) is a HOST concern, dispatched机械 by the provider, not
// a developer knob.
//
// SDK-NEUTRAL by construction (archtest TestEngineQuarantine): this file names
// NO engine SDK type. It emits []MCPTool — plain {name, description, schema,
// handler} rows — which a provider wraps into claude.NewMCPTool /
// gokimitools.Tool. The one contract every agent shares是"骨架只出 MCP 目录".

// MCPResult is the neutral outcome of one meta-tool invocation: the rendered
// tool-message text plus the error flag. A provider materialises it into its
// engine's own tool-result type (claude.MCPToolResult / go-kimi
// types.ToolResult).
type MCPResult struct {
	Text    string
	IsError bool
}

// MCPTool is one row of the neutral MCP catalog: the protocol-layer spec
// (name/description/schema, straight from metatool.ToolSpec) plus a Handler that
// executes the tool given its raw params. A provider iterates BuildMCPCatalog's
// output and wraps each row in one generic engine-side adapter.
type MCPTool struct {
	Name        string
	Description string
	Schema      json.RawMessage
	// Handler runs the tool. params is the engine-supplied argument JSON.
	Handler func(ctx context.Context, params json.RawMessage) MCPResult
}

// ToolExecutor is the closure one meta-tool Execute runs through — the provider
// supplies it, binding the incarnation's metatool.Exec face (ExecFace(sys, …):
// the substrate JobTable + sys.Call) and the current turn's RuntimeContext
// (derived from base.Trigger — the curTurn/RuntimeContext 合一). This neutral
// catalog names neither the engine nor the tool params, keeping BuildMCPCatalog
// engine-SDK-free (§2 S2).
type ToolExecutor func(ctx context.Context, mt metatool.MetaTool, params json.RawMessage) metatool.ResultValue

// BuildMCPCatalog projects the substrate's 7-tool meta catalog
// (metatool.MetaTools) into the neutral MCP工具表. Each row's Handler bridges to
// exec and renders the ResultValue as tool-message text. Authored ONCE here (§2:
// the catalog is data-driven, not restated per provider).
func BuildMCPCatalog(exec ToolExecutor) []MCPTool {
	catalog := metatool.MetaTools()
	tools := make([]MCPTool, 0, len(catalog))
	for _, mt := range catalog {
		mt := mt // capture per iteration
		var schema json.RawMessage
		if len(mt.Spec.Schema) > 0 {
			schema = metatool.CloneRawJSON(mt.Spec.Schema)
		}
		handler := func(ctx context.Context, params json.RawMessage) MCPResult {
			return RenderMCPResult(exec(ctx, mt, params))
		}
		tools = append(tools, MCPTool{
			Name:        mt.Spec.Name,
			Description: mt.Spec.Description,
			Schema:      schema,
			Handler:     handler,
		})
	}
	return tools
}

// RenderMCPResult materialises a metatool.ResultValue as an MCPResult: the
// structured result map rendered to JSON text (the model consumes a tool
// message), carrying the error flag through unchanged.
func RenderMCPResult(rv metatool.ResultValue) MCPResult {
	var text string
	if b, err := json.Marshal(rv.Value); err == nil {
		text = string(b)
	}
	return MCPResult{Text: text, IsError: rv.IsError}
}
