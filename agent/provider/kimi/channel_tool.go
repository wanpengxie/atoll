package kimi

import (
	"context"
	"encoding/json"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/atoll/agent/base"
	"github.com/wanpengxie/atoll/lib/metatool"
)

// channelToolRuntimeKey carries the current turn's RuntimeContext through the
// tool-execution context so meta tools can thread parent/correlation.
type channelToolRuntimeKey struct{}

// rcFromTrigger builds the metatool RuntimeContext threaded to the tools for one
// turn (the curTurn/RC 合一 — one value per turn, carried on the tool ctx).
func rcFromTrigger(trigger base.Trigger) metatool.RuntimeContext {
	return metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope:      trigger.Envelope,
			CorrelationID: trigger.CorrelationID,
		},
	}
}

// channelTools returns the meta-tool surface the LLM sees. It iterates the
// data-driven metatool.MetaTools() catalog and wraps each entry in one generic
// go-kimi adapter bound to the engine's Exec face — the shared actor-invocation
// machinery (the substrate JobTable + sys.Call), not agent-private state.
func (e *engine) channelTools() []gokimitools.Tool {
	catalog := metatool.MetaTools()
	tools := make([]gokimitools.Tool, 0, len(catalog))
	for _, mt := range catalog {
		tools = append(tools, &kimiTool{mt: mt, x: e.x})
	}
	return tools
}

// extractRuntimeContext pulls the turn's RuntimeContext from ctx. A missing
// value yields a zero RuntimeContext, which the Execute functions reject as
// "outside a turn".
func extractRuntimeContext(ctx context.Context) metatool.RuntimeContext {
	rc, ok := ctx.Value(channelToolRuntimeKey{}).(metatool.RuntimeContext)
	if !ok {
		return metatool.RuntimeContext{}
	}
	return rc
}

// kimiTool is the one generic go-kimi adapter over a metatool.MetaTool. Name /
// description / schema come straight from the spec; Execute threads the turn's
// RuntimeContext and materialises the ResultValue into a go-kimi ToolResult.
type kimiTool struct {
	mt metatool.MetaTool
	x  *metatool.Exec
}

var _ gokimitools.Tool = (*kimiTool)(nil)

func (t *kimiTool) Name() string        { return t.mt.Spec.Name }
func (t *kimiTool) Description() string { return t.mt.Spec.Description }

func (t *kimiTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(t.mt.Spec.Schema)
}

func (t *kimiTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := t.mt.Execute(ctx, params, t.x, rc)
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
