package agent

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

// channelTools returns the meta-tool surface the LLM sees. Each tool is a thin
// go-kimi wrapper over a metatool Execute function, driven against the agent's
// held shell — the shared actor-invocation machinery, not agent-private state.
func (b *Bridge) channelTools() []gokimitools.Tool {
	if b == nil {
		return nil
	}
	return []gokimitools.Tool{
		&CallActorTool{bridge: b},
		&ListActorsTool{bridge: b},
		&DescribeActorTool{bridge: b},
		&DescribeTypeTool{bridge: b},
		&AwaitResultTool{bridge: b},
		&AbandonTool{bridge: b},
		&ListPendingTool{bridge: b},
	}
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

// shell returns the bridge's held metatool.Shell, or nil before Start. The
// Execute functions treat a nil shell as "tool not configured".
func (b *Bridge) shellRef() *metatool.Shell {
	if b == nil {
		return nil
	}
	return b.shell
}

// --- go-kimi thin wrappers ---

// CallActorTool is the go-kimi wrapper for metatool.ExecuteCallActor.
type CallActorTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*CallActorTool)(nil)

func (t *CallActorTool) Name() string        { return metatool.CallActorSpec.Name }
func (t *CallActorTool) Description() string { return metatool.CallActorSpec.Description }
func (t *CallActorTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.CallActorSpec.Schema)
}
func (t *CallActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := metatool.ExecuteCallActor(ctx, params, t.bridge.shellRef(), rc)
	return toKimiToolResult(rv), nil
}

// ListActorsTool is the go-kimi wrapper for metatool.ExecuteListActors.
type ListActorsTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*ListActorsTool)(nil)

func (t *ListActorsTool) Name() string        { return metatool.ListActorsSpec.Name }
func (t *ListActorsTool) Description() string { return metatool.ListActorsSpec.Description }
func (t *ListActorsTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.ListActorsSpec.Schema)
}
func (t *ListActorsTool) Execute(ctx context.Context, _ json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := metatool.ExecuteListActors(ctx, t.bridge.shellRef(), rc)
	return toKimiToolResult(rv), nil
}

// DescribeActorTool is the go-kimi wrapper for metatool.ExecuteDescribeActor.
type DescribeActorTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*DescribeActorTool)(nil)

func (t *DescribeActorTool) Name() string        { return metatool.DescribeActorSpec.Name }
func (t *DescribeActorTool) Description() string { return metatool.DescribeActorSpec.Description }
func (t *DescribeActorTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.DescribeActorSpec.Schema)
}
func (t *DescribeActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := metatool.ExecuteDescribeActor(ctx, params, t.bridge.shellRef(), rc)
	return toKimiToolResult(rv), nil
}

// DescribeTypeTool is the go-kimi wrapper for metatool.ExecuteDescribeType.
type DescribeTypeTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*DescribeTypeTool)(nil)

func (t *DescribeTypeTool) Name() string        { return metatool.DescribeTypeSpec.Name }
func (t *DescribeTypeTool) Description() string { return metatool.DescribeTypeSpec.Description }
func (t *DescribeTypeTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.DescribeTypeSpec.Schema)
}
func (t *DescribeTypeTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc := extractRuntimeContext(ctx)
	rv := metatool.ExecuteDescribeType(ctx, params, t.bridge.shellRef(), rc)
	return toKimiToolResult(rv), nil
}

// AwaitResultTool is the go-kimi wrapper for metatool.ExecuteAwaitResult.
type AwaitResultTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*AwaitResultTool)(nil)

func (t *AwaitResultTool) Name() string        { return metatool.AwaitResultSpec.Name }
func (t *AwaitResultTool) Description() string { return metatool.AwaitResultSpec.Description }
func (t *AwaitResultTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.AwaitResultSpec.Schema)
}
func (t *AwaitResultTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rv := metatool.ExecuteAwaitResult(ctx, params, t.bridge.shellRef())
	return toKimiToolResult(rv), nil
}

// AbandonTool is the go-kimi wrapper for metatool.ExecuteAbandon.
type AbandonTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*AbandonTool)(nil)

func (t *AbandonTool) Name() string        { return metatool.AbandonSpec.Name }
func (t *AbandonTool) Description() string { return metatool.AbandonSpec.Description }
func (t *AbandonTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.AbandonSpec.Schema)
}
func (t *AbandonTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rv := metatool.ExecuteAbandon(ctx, params, t.bridge.shellRef())
	return toKimiToolResult(rv), nil
}

// ListPendingTool is the go-kimi wrapper for metatool.ExecuteListPending.
type ListPendingTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*ListPendingTool)(nil)

func (t *ListPendingTool) Name() string        { return metatool.ListPendingSpec.Name }
func (t *ListPendingTool) Description() string { return metatool.ListPendingSpec.Description }
func (t *ListPendingTool) ParameterSchema() json.RawMessage {
	return metatool.CloneRawJSON(metatool.ListPendingSpec.Schema)
}
func (t *ListPendingTool) Execute(_ context.Context, _ json.RawMessage) (types.ToolResult, error) {
	rv := metatool.ExecuteListPending(context.Background(), t.bridge.shellRef())
	return toKimiToolResult(rv), nil
}

// --- go-kimi result converter ---

// toKimiToolResult converts a metatool.ResultValue to a go-kimi types.ToolResult.
func toKimiToolResult(rv metatool.ResultValue) types.ToolResult {
	return types.ToolResult{
		Name:    rv.Name,
		Value:   types.ToolReturnValue{Value: rv.Value},
		IsError: rv.IsError,
	}
}
