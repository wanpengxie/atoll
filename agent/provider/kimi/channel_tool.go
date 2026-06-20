package kimi

import (
	"context"
	"encoding/json"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/lib/metatool"
)

// channelToolRuntimeKey carries the current turn's trigger item through the
// tool-execution context so meta tools can thread parent/correlation.
type channelToolRuntimeKey struct{}

// channelTools returns the meta-tool surface the LLM sees. It iterates the
// data-driven metatool.MetaTools() catalog and wraps each entry in one
// generic go-kimi adapter bound to the agent's held shell — the shared
// actor-invocation machinery, not agent-private state.
func (b *Bridge) channelTools() []gokimitools.Tool {
	if b == nil {
		return nil
	}
	catalog := metatool.MetaTools()
	tools := make([]gokimitools.Tool, 0, len(catalog))
	for _, mt := range catalog {
		tools = append(tools, &kimiTool{mt: mt, shell: b.shellRef})
	}
	return tools
}

// extractRuntimeContext pulls the trigger item from ctx for metatool
// Execute functions. A missing item yields a zero RuntimeContext, which
// the Execute functions reject as "outside a turn".
func extractRuntimeContext(ctx context.Context) metatool.RuntimeContext {
	item, ok := ctx.Value(channelToolRuntimeKey{}).(turnItem)
	if !ok {
		return metatool.RuntimeContext{}
	}
	return metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope:      item.env,
			CorrelationID: item.env.CorrelationID,
		},
	}
}

// shellRef returns the bridge's held metatool.Shell, or nil before Start. The
// Execute functions treat a nil shell as "tool not configured".
func (b *Bridge) shellRef() *metatool.Shell {
	if b == nil {
		return nil
	}
	return b.shell
}

// kimiTool is the one generic go-kimi adapter over a metatool.MetaTool. Name /
// description / schema come straight from the spec; Execute threads the turn's
// RuntimeContext and materialises the ResultValue into a go-kimi ToolResult.
//
// shell is a LAZY resolver (b.shellRef), not a captured *Shell: the tool
// surface is installed into the LLM loop during Start BEFORE b.shell is
// assigned, so capturing the value would freeze a nil shell. Resolving at
// Execute time — which only runs once the loop is live — always sees the real
// shell, making the binding order-independent by construction.
type kimiTool struct {
	mt    metatool.MetaTool
	shell func() *metatool.Shell
}

var _ gokimitools.Tool = (*kimiTool)(nil)

func (t *kimiTool) Name() string        { return t.mt.Spec.Name }
func (t *kimiTool) Description() string { return t.mt.Spec.Description }

func (t *kimiTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(t.mt.Spec.Schema)
}

func (t *kimiTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := t.mt.Execute(ctx, params, t.shell(), rc)
	return toKimiToolResult(rv), nil
}

// toKimiToolResult converts a metatool.ResultValue to a go-kimi types.ToolResult.
func toKimiToolResult(rv metatool.ResultValue) types.ToolResult {
	return types.ToolResult{
		Name:    rv.Name,
		Value:   types.ToolReturnValue{Value: rv.Value},
		IsError: rv.IsError,
	}
}
