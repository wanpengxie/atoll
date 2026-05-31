package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// DescribeActorTool returns one actor's skill document and type summary.
type DescribeActorTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*DescribeActorTool)(nil)

func (t *DescribeActorTool) Name() string { return "describe_actor" }

func (t *DescribeActorTool) Description() string {
	return strings.TrimSpace(`
Returns full skill doc plus all types for one actor. Call this after list_actors
when you have identified which actor matches your need. Returns:
  - actor_id, description, kind, binding
  - skill_doc: markdown with typical workflows and error handling
  - types: full type list with one-line descriptions
`)
}

var describeActorSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Actor id returned by list_actors."}
  },
  "required": ["actor_id"]
}`)

func (t *DescribeActorTool) ParameterSchema() json.RawMessage {
	return cloneRawJSON(describeActorSchema)
}

type describeActorParams struct {
	ActorID string `json:"actor_id"`
}

func (t *DescribeActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("describe_actor", actorCLIInternalError, "describe_actor tool not configured", "Retry after the bridge is configured", nil), nil
	}
	var p describeActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return payloadInvalidError("describe_actor", "", "", fmt.Sprintf("invalid params: %v", err)), nil
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	if p.ActorID == "" {
		return payloadInvalidError("describe_actor", "", "", "actor_id is required (call list_actors to discover)"), nil
	}

	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return actorCLIErrorResult("describe_actor", actorCLIInternalError, "describe_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil), nil
	}
	// Live only: actor.describe is framework-intercepted on the target
	// actor's dispatch path and answers from the actor's current
	// declaration. There is no frozen fallback — a stale local copy would
	// be worse than a clean error.
	return t.bridge.executeChannelRequest(ctx, runtime.ipc, runtime.trigger, channelRequestSpec{
		ToolName:       "describe_actor",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        cloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        channelToolDefaultTimeout,
		// describe_* are synchronous reserved-type lookups the agent
		// always needs inline; wait the full timeout, no fast-path ack.
		WaitMode: waitUnbounded,
	}), nil
}

// DescribeTypeTool returns detailed product-layer guidance for one type.
type DescribeTypeTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*DescribeTypeTool)(nil)

func (t *DescribeTypeTool) Name() string { return "describe_type" }

func (t *DescribeTypeTool) Description() string {
	return strings.TrimSpace(`
Returns payload schema hints plus error codes and examples for one type. Call this
before call_actor when you need to know payload shape. Returns:
  - actor_id, type, description, allowed_kinds, max_pending_ms
  - payload_example: a typical payload object you can adapt
  - payload_fields: structured field-by-field documentation
  - error_codes: type-specific error codes with recovery hints
  - notes: additional markdown notes
`)
}

var describeTypeSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Actor id returned by list_actors."},
    "type": {"type": "string", "description": "Envelope type returned by list_actors or describe_actor."}
  },
  "required": ["actor_id", "type"]
}`)

func (t *DescribeTypeTool) ParameterSchema() json.RawMessage {
	return cloneRawJSON(describeTypeSchema)
}

type describeTypeParams struct {
	ActorID string `json:"actor_id"`
	Type    string `json:"type"`
}

func (t *DescribeTypeTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("describe_type", actorCLIInternalError, "describe_type tool not configured", "Retry after the bridge is configured", nil), nil
	}
	var p describeTypeParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return payloadInvalidError("describe_type", "", "", fmt.Sprintf("invalid params: %v", err)), nil
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return payloadInvalidError("describe_type", p.ActorID, p.Type, "actor_id is required (call list_actors to discover)"), nil
	}
	if p.Type == "" {
		return payloadInvalidError("describe_type", p.ActorID, p.Type, "type is required (call describe_actor to discover)"), nil
	}

	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return actorCLIErrorResult("describe_type", actorCLIInternalError, "describe_type invoked outside a bridge turn", "Retry from inside an active bridge turn", nil), nil
	}
	// Live only: actor.describe with a {"type": ...} filter returns the
	// target type's current declaration projection (payload example /
	// fields / error codes). No frozen fallback.
	payload, _ := json.Marshal(map[string]string{"type": p.Type})
	return t.bridge.executeChannelRequest(ctx, runtime.ipc, runtime.trigger, channelRequestSpec{
		ToolName:       "describe_type",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        channelToolDefaultTimeout,
		// describe_* are synchronous reserved-type lookups the agent
		// always needs inline; wait the full timeout, no fast-path ack.
		WaitMode: waitUnbounded,
	}), nil
}
