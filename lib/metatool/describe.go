package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/lib/callkit"
)

// DescribeActorSpec is the protocol-layer definition of describe_actor.
var DescribeActorSpec = ToolSpec{
	Name: "describe_actor",
	Description: strings.TrimSpace(`
Returns full skill doc plus all types for one actor. Call this after list_actors
when you have identified which actor matches your need. Returns:
  - actor_id, description, kind, binding
  - skill_doc: markdown with typical workflows and error handling
  - types: full type list with one-line descriptions
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Actor id returned by list_actors."}
  },
  "required": ["actor_id"]
}`),
}

type describeActorParams struct {
	ActorID string `json:"actor_id"`
}

// ExecuteDescribeActor is the protocol-layer execute function for describe_actor.
func ExecuteDescribeActor(ctx context.Context, params json.RawMessage, exec Executor, rc RuntimeContext) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("describe_actor", callkit.InternalError, "describe_actor tool not configured", "Retry after the bridge is configured", nil)
	}
	var p describeActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return callkit.PayloadInvalidError("describe_actor", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	if p.ActorID == "" {
		return callkit.PayloadInvalidError("describe_actor", "actor_id is required (call list_actors to discover)", "")
	}

	if rc.IPC == nil {
		return callkit.NewError("describe_actor", callkit.InternalError, "describe_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	return exec.ExecuteRequest(ctx, rc, callkit.RequestSpec{
		ToolName:       "describe_actor",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        callkit.CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        callkit.DefaultTimeout,
		WaitMode:       callkit.WaitUnbounded,
	})
}

// DescribeTypeSpec is the protocol-layer definition of describe_type.
var DescribeTypeSpec = ToolSpec{
	Name: "describe_type",
	Description: strings.TrimSpace(`
Returns payload schema hints plus error codes and examples for one type. Call this
before call_actor when you need to know payload shape. Returns:
  - actor_id, type, description, allowed_kinds, max_pending_ms
  - payload_example: a typical payload object you can adapt
  - payload_fields: structured field-by-field documentation
  - error_codes: type-specific error codes with recovery hints
  - notes: additional markdown notes
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Actor id returned by list_actors."},
    "type": {"type": "string", "description": "Envelope type returned by list_actors or describe_actor."}
  },
  "required": ["actor_id", "type"]
}`),
}

type describeTypeParams struct {
	ActorID string `json:"actor_id"`
	Type    string `json:"type"`
}

// ExecuteDescribeType is the protocol-layer execute function for describe_type.
func ExecuteDescribeType(ctx context.Context, params json.RawMessage, exec Executor, rc RuntimeContext) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("describe_type", callkit.InternalError, "describe_type tool not configured", "Retry after the bridge is configured", nil)
	}
	var p describeTypeParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return callkit.PayloadInvalidError("describe_type", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return callkit.PayloadInvalidError("describe_type", "actor_id is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}
	if p.Type == "" {
		return callkit.PayloadInvalidError("describe_type", "type is required (call describe_actor to discover)", payloadHint(p.ActorID, p.Type))
	}

	if rc.IPC == nil {
		return callkit.NewError("describe_type", callkit.InternalError, "describe_type invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	payload, _ := json.Marshal(map[string]string{"type": p.Type})
	return exec.ExecuteRequest(ctx, rc, callkit.RequestSpec{
		ToolName:       "describe_type",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        callkit.DefaultTimeout,
		WaitMode:       callkit.WaitUnbounded,
	})
}
