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
	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

type channelToolRuntimeKey struct{}

type channelToolRuntime struct {
	ipc     IPCFacade
	trigger TriggerPayload
}

const (
	waitFastPath  = callkit.WaitFastPath
	waitUnbounded = callkit.WaitUnbounded
	waitNone      = callkit.WaitNone
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

// extractRuntimeContext pulls IPC + Trigger from ctx for metatool Execute functions.
func extractRuntimeContext(ctx context.Context) (metatool.RuntimeContext, bool) {
	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return metatool.RuntimeContext{}, false
	}
	return metatool.RuntimeContext{
		IPC: runtime.ipc,
		Trigger: metatool.Trigger{
			Envelope:      runtime.trigger.Envelope,
			CorrelationID: runtime.trigger.CorrelationID,
		},
	}, true
}

// ExecuteRequest implements metatool.Executor.
func (b *Bridge) ExecuteRequest(ctx context.Context, rc metatool.RuntimeContext, spec callkit.RequestSpec) callkit.ResultValue {
	ipc := rc.IPC.(IPCFacade)
	trigger := TriggerPayload{
		Envelope:      rc.Trigger.Envelope,
		CorrelationID: rc.Trigger.CorrelationID,
	}
	tr := b.executeChannelRequest(ctx, ipc, trigger, spec)
	return toResultValue(tr)
}

// ExecuteReservedRaw implements metatool.Executor.
func (b *Bridge) ExecuteReservedRaw(ctx context.Context, rc metatool.RuntimeContext, spec callkit.RequestSpec) (json.RawMessage, bool) {
	ipc := rc.IPC.(IPCFacade)
	trigger := TriggerPayload{
		Envelope:      rc.Trigger.Envelope,
		CorrelationID: rc.Trigger.CorrelationID,
	}
	return b.executeReservedRequestRaw(ctx, ipc, trigger, spec)
}

// CallerInstance implements metatool.Executor.
func (b *Bridge) CallerInstance() *callkit.Caller {
	return b.caller()
}

// --- go-kimi thin wrappers ---

// CallActorTool is the go-kimi wrapper for metatool.ExecuteCallActor.
type CallActorTool struct{ bridge *Bridge }

var _ gokimitools.Tool = (*CallActorTool)(nil)

func (t *CallActorTool) Name() string                      { return metatool.CallActorSpec.Name }
func (t *CallActorTool) Description() string               { return metatool.CallActorSpec.Description }
func (t *CallActorTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.CallActorSpec.Schema) }
func (t *CallActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc, ok := extractRuntimeContext(ctx)
	if !ok {
		rc = metatool.RuntimeContext{} // IPC==nil triggers internal_error inside ExecuteCallActor
	}
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

func (t *ListActorsTool) Name() string                      { return metatool.ListActorsSpec.Name }
func (t *ListActorsTool) Description() string               { return metatool.ListActorsSpec.Description }
func (t *ListActorsTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.ListActorsSpec.Schema) }
func (t *ListActorsTool) Execute(ctx context.Context, _ json.RawMessage) (types.ToolResult, error) {
	rc, ok := extractRuntimeContext(ctx)
	if !ok {
		rc = metatool.RuntimeContext{}
	}
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

func (t *DescribeActorTool) Name() string                      { return metatool.DescribeActorSpec.Name }
func (t *DescribeActorTool) Description() string               { return metatool.DescribeActorSpec.Description }
func (t *DescribeActorTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.DescribeActorSpec.Schema) }
func (t *DescribeActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc, ok := extractRuntimeContext(ctx)
	if !ok {
		rc = metatool.RuntimeContext{}
	}
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

func (t *DescribeTypeTool) Name() string                      { return metatool.DescribeTypeSpec.Name }
func (t *DescribeTypeTool) Description() string               { return metatool.DescribeTypeSpec.Description }
func (t *DescribeTypeTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.DescribeTypeSpec.Schema) }
func (t *DescribeTypeTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	rc, ok := extractRuntimeContext(ctx)
	if !ok {
		rc = metatool.RuntimeContext{}
	}
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

func (t *AwaitResultTool) Name() string                      { return metatool.AwaitResultSpec.Name }
func (t *AwaitResultTool) Description() string               { return metatool.AwaitResultSpec.Description }
func (t *AwaitResultTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.AwaitResultSpec.Schema) }
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

func (t *AbandonTool) Name() string                      { return metatool.AbandonSpec.Name }
func (t *AbandonTool) Description() string               { return metatool.AbandonSpec.Description }
func (t *AbandonTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.AbandonSpec.Schema) }
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

func (t *ListPendingTool) Name() string                      { return metatool.ListPendingSpec.Name }
func (t *ListPendingTool) Description() string               { return metatool.ListPendingSpec.Description }
func (t *ListPendingTool) ParameterSchema() json.RawMessage  { return callkit.CloneRawJSON(metatool.ListPendingSpec.Schema) }
func (t *ListPendingTool) Execute(_ context.Context, _ json.RawMessage) (types.ToolResult, error) {
	var exec metatool.Executor
	if t != nil && t.bridge != nil {
		exec = t.bridge
	}
	rv := metatool.ExecuteListPending(context.Background(), exec)
	return toKimiToolResult(rv), nil
}

// --- go-kimi result converters ---

// toKimiToolResult converts a callkit.ResultValue to a go-kimi types.ToolResult.
func toKimiToolResult(rv callkit.ResultValue) types.ToolResult {
	return types.ToolResult{
		Name:    rv.Name,
		Value:   types.ToolReturnValue{Value: rv.Value},
		IsError: rv.IsError,
	}
}

// toResultValue converts a go-kimi types.ToolResult to a callkit.ResultValue.
func toResultValue(tr types.ToolResult) callkit.ResultValue {
	var m map[string]any
	if v, ok := tr.Value.Value.(map[string]any); ok {
		m = v
	} else {
		// Wrap non-map values so ResultValue.Value stays map[string]any.
		m = map[string]any{"result": tr.Value.Value}
	}
	return callkit.ResultValue{
		Name:    tr.Name,
		Value:   m,
		IsError: tr.IsError,
	}
}

// --- routeTriggers + executeChannelRequest / executeReservedRequestRaw ---
// These stay on Bridge because they need Bridge internals (envelopeID, cfg.NowFn, caller).

// routeTriggers fans the daemon->worker trigger stream through the
// worker-side caller helper.
func (b *Bridge) routeTriggers(ctx context.Context, ipc IPCFacade, in <-chan TriggerPayload) <-chan TriggerPayload {
	out := make(chan TriggerPayload, 32)
	go func() {
		defer close(out)
		caller := b.caller()
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-in:
				if !ok {
					return
				}
				if payload.Envelope.Kind == message.KindResponse && payload.Envelope.ParentID != "" {
					env := payload.Envelope
					_, final := behavior.ParseFinalStatus(env.Payload)
					disp := caller.Deliver(&env)
					surfaceAsTrigger := disp == callkit.NoActiveWaiter && final
					if !surfaceAsTrigger {
						if err := ackTrigger(ctx, ipc, payload, true, ""); err != nil {
							return
						}
						continue
					}
				}
				select {
				case out <- payload:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// executeChannelRequest is the generic envelope-dispatch fast-path.
func (b *Bridge) executeChannelRequest(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	spec callkit.RequestSpec,
) types.ToolResult {
	if spec.Timeout <= 0 {
		spec.Timeout = callkit.DefaultTimeout
	}
	now := b.cfg.NowFn()
	env := message.Envelope{
		ID:            b.envelopeID(ipc, now),
		ChannelID:     ipc.ChannelID(),
		Type:          strings.TrimSpace(spec.EnvelopeType),
		Kind:          message.KindRequest,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{actor.ActorID(spec.HandlerActorID)},
		Payload:       spec.Payload,
		ParentID:      trigger.Envelope.ID,
		CorrelationID: channelToolCorrelationID(trigger),
		TS:            now,
		TSReceived:    now,
	}

	caller := b.caller()
	est := int64(spec.Timeout / time.Millisecond)
	expectsAwait := spec.WaitMode != waitNone
	res, err := caller.Submit(ctx, ipc, env, est, expectsAwait)
	if err != nil {
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("emit channel request %s: %v", spec.EnvelopeType, err))
	}

	var window time.Duration
	switch spec.WaitMode {
	case waitNone:
		window = 0
	case waitUnbounded:
		window = callkit.ResolveFastPathWindow(spec.Timeout, callkit.DefaultTimeout, true)
	default: // waitFastPath
		window = callkit.ResolveFastPathWindow(spec.Timeout, callkit.DefaultTimeout, false)
	}

	if window <= 0 {
		return ackToToolResult(spec.ToolName, res.Ack)
	}

	finalEnv, ok, awaitErr := caller.Await(ctx, res.RequestID, window)
	if awaitErr != nil {
		caller.Abandon(res.RequestID)
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("channel request %s wait failed: %v", spec.EnvelopeType, awaitErr))
	}
	if ok {
		return channelToolResultFromResponse(spec.ToolName, *finalEnv)
	}
	return ackToToolResult(spec.ToolName, res.Ack)
}

// executeReservedRequestRaw emits a reserved-type request and returns the
// FINAL response payload as raw JSON.
func (b *Bridge) executeReservedRequestRaw(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	spec callkit.RequestSpec,
) (json.RawMessage, bool) {
	if spec.Timeout <= 0 {
		spec.Timeout = callkit.DefaultTimeout
	}
	now := b.cfg.NowFn()
	env := message.Envelope{
		ID:            b.envelopeID(ipc, now),
		ChannelID:     ipc.ChannelID(),
		Type:          strings.TrimSpace(spec.EnvelopeType),
		Kind:          message.KindRequest,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{actor.ActorID(spec.HandlerActorID)},
		Payload:       spec.Payload,
		ParentID:      trigger.Envelope.ID,
		CorrelationID: channelToolCorrelationID(trigger),
		TS:            now,
		TSReceived:    now,
	}
	caller := b.caller()
	est := int64(spec.Timeout / time.Millisecond)
	res, err := caller.Submit(ctx, ipc, env, est, true)
	if err != nil {
		return nil, false
	}
	window := callkit.ResolveFastPathWindow(spec.Timeout, callkit.DefaultTimeout, true)
	finalEnv, ok, awaitErr := caller.Await(ctx, res.RequestID, window)
	if awaitErr != nil {
		caller.Abandon(res.RequestID)
		return nil, false
	}
	if !ok || finalEnv == nil || finalEnv.Kind != message.KindResponse {
		return nil, false
	}
	if callkit.ResponseFailureReason(finalEnv.Payload) != "" {
		return nil, false
	}
	return finalEnv.Payload, true
}

// --- helper functions ---

// ackToToolResult converts a metatool AckDescriptor to a go-kimi ToolResult.
func ackToToolResult(toolName string, ack callkit.AckDescriptor) types.ToolResult {
	id := ack.RequestID.String()
	ack.Guidance = "Accepted. To wait, call await_result(request_id=" + id +
		"). If you do not wait, the result returns as a new message (parent_id=" + id + ")."
	ack.ToWait = callkit.ToWaitHint{
		Tool:   "await_result",
		Params: map[string]any{"request_id": id},
	}
	ack.NotWaitng = "result returns as kind=response, parent_id=" + id + " new turn trigger"
	rv := callkit.AckResult(toolName, ack)
	return types.ToolResult{
		Name:  rv.Name,
		Value: types.ToolReturnValue{Value: rv.Value},
	}
}

func channelToolCorrelationID(trigger TriggerPayload) message.ID {
	return behavior.CorrelationID(trigger.CorrelationID, trigger.Envelope.CorrelationID, trigger.Envelope.ID)
}


func channelToolResultFromResponse(toolName string, env message.Envelope) types.ToolResult {
	rv, isErr := callkit.ResultFromResponse(toolName, env)
	if isErr {
		return types.ToolResult{
			Name:    rv.Name,
			Value:   types.ToolReturnValue{Value: rv.Value},
			IsError: true,
		}
	}
	// Success: the original code returned `toolPayloadValue(env.Payload)` which
	// could be any type (map, string, etc.) — not wrapped in a map. Preserve
	// that behavior for go-kimi compatibility.
	value := toolPayloadValue(env.Payload)
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

func toolPayloadValue(raw json.RawMessage) any {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return map[string]any{}
	}
	var value any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return text
	}
	if value == nil {
		return map[string]any{}
	}
	return value
}
