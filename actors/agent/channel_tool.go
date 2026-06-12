package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// channelToolRuntimeKey carries the current turn's trigger item through the
// tool-execution context so meta tools can thread parent/correlation.
type channelToolRuntimeKey struct{}

const (
	waitFastPath  = metatool.WaitFastPath
	waitUnbounded = metatool.WaitUnbounded
	waitNone      = metatool.WaitNone
)

// channelTools returns the meta-tool surface the LLM sees.
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

// --- metatool.Executor implementation on Bridge ---

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

// ExecuteRequest implements metatool.Executor.
func (b *Bridge) ExecuteRequest(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
	tr := b.executeChannelRequest(ctx, turnItem{env: rc.Trigger.Envelope}, spec)
	return toResultValue(tr)
}

// ExecuteReservedRaw implements metatool.Executor.
func (b *Bridge) ExecuteReservedRaw(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) (json.RawMessage, bool) {
	return b.executeReservedRequestRaw(ctx, turnItem{env: rc.Trigger.Envelope}, spec)
}

// AwaitRequest implements metatool.Executor (await_result semantics).
func (b *Bridge) AwaitRequest(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	return b.futures.Await(ctx, id, window)
}

// AbandonRequest implements metatool.Executor (abandon semantics).
func (b *Bridge) AbandonRequest(id message.ID) {
	b.futures.Cancel(id)
}

// PendingRequests implements metatool.Executor (list_pending semantics).
func (b *Bridge) PendingRequests() []message.ID {
	return b.futures.Pending()
}

// RequestInFlight implements metatool.Executor.
func (b *Bridge) RequestInFlight(id message.ID) bool {
	return b.futures.Registered(id)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteCallActor(ctx, params, exec, rc)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteListActors(ctx, exec, rc)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteDescribeActor(ctx, params, exec, rc)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteDescribeType(ctx, params, exec, rc)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteAwaitResult(ctx, params, exec)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteAbandon(ctx, params, exec)
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
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteListPending(context.Background(), exec)
	return toKimiToolResult(rv), nil
}

// --- go-kimi result converters ---

// toKimiToolResult converts a metatool.ResultValue to a go-kimi types.ToolResult.
func toKimiToolResult(rv metatool.ResultValue) types.ToolResult {
	return types.ToolResult{
		Name:    rv.Name,
		Value:   types.ToolReturnValue{Value: rv.Value},
		IsError: rv.IsError,
	}
}

// toResultValue converts a go-kimi types.ToolResult to a metatool.ResultValue.
func toResultValue(tr types.ToolResult) metatool.ResultValue {
	var m map[string]any
	if v, ok := tr.Value.Value.(map[string]any); ok {
		m = v
	} else {
		// Wrap non-map values so ResultValue.Value stays map[string]any.
		m = map[string]any{"result": tr.Value.Value}
	}
	return metatool.ResultValue{
		Name:    tr.Name,
		Value:   m,
		IsError: tr.IsError,
	}
}

// --- channel request execution (the call face in action) ---

// submitChannelRequest is the three-step call: register the future
// (subscribe-before-send), commit the request through the harness, and
// Arm closure author#2. Any failure unwinds the future registration.
func (b *Bridge) submitChannelRequest(ctx context.Context, env message.Envelope, expectsAwait bool) error {
	b.futures.Register(env.ID, expectsAwait)
	if err := b.write(ctx, env); err != nil {
		b.futures.Cancel(env.ID)
		return err
	}
	b.armCaller(&env)
	return nil
}

// executeChannelRequest is the generic envelope-dispatch fast-path.
func (b *Bridge) executeChannelRequest(
	ctx context.Context,
	item turnItem,
	spec metatool.RequestSpec,
) types.ToolResult {
	if spec.Timeout <= 0 {
		spec.Timeout = metatool.DefaultTimeout
	}
	env, buildErr := b.buildChannelRequest(item, spec)
	if buildErr != nil {
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("build channel request %s: %v", spec.EnvelopeType, buildErr))
	}

	expectsAwait := spec.WaitMode != waitNone
	if err := b.submitChannelRequest(ctx, env, expectsAwait); err != nil {
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("emit channel request %s: %v", spec.EnvelopeType, err))
	}
	ack := metatool.AckDescriptor{
		RequestID: env.ID,
		Accepted:  true,
		Status:    "accepted",
		EstWaitMs: int64(spec.Timeout / time.Millisecond),
	}

	var window time.Duration
	switch spec.WaitMode {
	case waitNone:
		window = 0
	case waitUnbounded:
		window = metatool.ResolveFastPathWindow(spec.Timeout, metatool.DefaultTimeout, true)
	default: // waitFastPath
		window = metatool.ResolveFastPathWindow(spec.Timeout, metatool.DefaultTimeout, false)
	}
	if window > b.cfg.FastPathWindow && spec.WaitMode == waitFastPath {
		window = b.cfg.FastPathWindow
	}

	if window <= 0 {
		return ackToToolResult(spec.ToolName, ack)
	}

	finalEnv, ok, awaitErr := b.futures.Await(ctx, env.ID, window)
	if awaitErr != nil {
		b.futures.Cancel(env.ID)
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("channel request %s wait failed: %v", spec.EnvelopeType, awaitErr))
	}
	if ok {
		return channelToolResultFromResponse(spec.ToolName, *finalEnv)
	}
	return ackToToolResult(spec.ToolName, ack)
}

// executeReservedRequestRaw emits a reserved-type request and returns the
// FINAL response payload as raw JSON.
func (b *Bridge) executeReservedRequestRaw(
	ctx context.Context,
	item turnItem,
	spec metatool.RequestSpec,
) (json.RawMessage, bool) {
	if spec.Timeout <= 0 {
		spec.Timeout = metatool.DefaultTimeout
	}
	env, buildErr := b.buildChannelRequest(item, spec)
	if buildErr != nil {
		return nil, false
	}
	if err := b.submitChannelRequest(ctx, env, true); err != nil {
		return nil, false
	}
	window := metatool.ResolveFastPathWindow(spec.Timeout, metatool.DefaultTimeout, true)
	finalEnv, ok, awaitErr := b.futures.Await(ctx, env.ID, window)
	if awaitErr != nil {
		b.futures.Cancel(env.ID)
		return nil, false
	}
	if !ok || finalEnv == nil || finalEnv.Kind != message.KindResponse {
		return nil, false
	}
	if metatool.ResponseFailureReason(finalEnv.Payload) != "" {
		return nil, false
	}
	return finalEnv.Payload, true
}

// --- helper functions ---

// ackToToolResult converts a metatool AckDescriptor to a go-kimi ToolResult.
func ackToToolResult(toolName string, ack metatool.AckDescriptor) types.ToolResult {
	id := ack.RequestID.String()
	ack.Guidance = "Accepted. To wait, call await_result(request_id=" + id +
		"). If you do not wait, the result returns as a new message (parent_id=" + id + ")."
	ack.ToWait = metatool.ToWaitHint{
		Tool:   "await_result",
		Params: map[string]any{"request_id": id},
	}
	ack.NotWaitng = "result returns as kind=response, parent_id=" + id + " new turn trigger"
	rv := metatool.AckResult(toolName, ack)
	return types.ToolResult{
		Name:  rv.Name,
		Value: types.ToolReturnValue{Value: rv.Value},
	}
}

func channelToolResultFromResponse(toolName string, env message.Envelope) types.ToolResult {
	rv, isErr := metatool.ResultFromResponse(toolName, env)
	if isErr {
		return types.ToolResult{
			Name:    rv.Name,
			Value:   types.ToolReturnValue{Value: rv.Value},
			IsError: true,
		}
	}
	// Success: return the raw payload value (map, string, etc.) — not
	// wrapped in a map — for go-kimi compatibility.
	value := metatool.PayloadValue(env.Payload)
	return types.ToolResult{
		Name:  toolName,
		Value: types.ToolReturnValue{Value: value},
	}
}

func channelToolErrorResult(toolName, msg string) types.ToolResult {
	return types.ToolResult{
		Name:    toolName,
		Value:   types.ToolReturnValue{Value: strings.TrimSpace(msg)},
		IsError: true,
	}
}


// buildChannelRequest assembles the kind=request envelope for a channel tool
// call through the behavior call-face builder (ONE home for request
// defaults), then stamps the binding-edge fields this path owns
// (deterministic per-actor id, TSReceived). ExpiresAt carries the caller's
// deadline so author#2's Arm has a timer to set.
func (b *Bridge) buildChannelRequest(
	item turnItem,
	spec metatool.RequestSpec,
) (message.Envelope, error) {
	now := b.cfg.NowFn()
	expiresAt := now + int64(spec.Timeout/time.Millisecond)
	env, err := behavior.BuildRequest(b.chID, b.sender(),
		func() time.Time { return time.UnixMilli(now) },
		behavior.RequestSpec{
			ID:            b.envelopeID(now),
			Type:          spec.EnvelopeType,
			Payload:       spec.Payload,
			Audience:      message.Audience{actor.ActorID(spec.HandlerActorID)},
			Visibility:    message.VisibilityPublic,
			ParentID:      item.env.ID,
			CorrelationID: item.correlationID(),
			ExpiresAt:     &expiresAt,
		})
	if err != nil {
		return message.Envelope{}, err
	}
	env.TSReceived = now
	return *env, nil
}
