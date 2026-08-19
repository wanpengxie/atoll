package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// DescribeActorSpec is the protocol-layer definition of describe_actor.
var DescribeActorSpec = ToolSpec{
	Name: "describe_actor",
	Description: strings.TrimSpace(`
Returns the actor's live D21 manifest: {class, interfaces, capabilities, words}.
Each word contains only its description, input/output schemas, error codes and
examples. Call this after list_actors for a member you may use. Directory facts
(kind, presence, uptime and device status) stay in list_actors.
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
func ExecuteDescribeActor(ctx context.Context, params json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Call == nil {
		return NewError("describe_actor", InternalError, "describe_actor tool not configured", "Retry after the bridge is configured", nil)
	}
	var p describeActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return PayloadInvalidError("describe_actor", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	if p.ActorID == "" {
		return PayloadInvalidError("describe_actor", "actor_id is required (call list_actors to discover)", "")
	}

	if !rc.InTurn() {
		return NewError("describe_actor", InternalError, "describe_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	result := x.CallSyncResult(ctx, rc, RequestSpec{
		ToolName:       "describe_actor",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
	return NormalizeCallActorResult(result, p.ActorID, "")
}

// DescribeTypeSpec is the protocol-layer definition of describe_type.
var DescribeTypeSpec = ToolSpec{
	Name: "describe_type",
	Description: strings.TrimSpace(`
Returns one word selected from the actor's live D21 manifest: description,
input/output schemas, error codes and examples. It does not report allowed
envelope kinds or a wait budget. Call this before call_actor when you need one
payload shape; describe_actor returns the complete words map.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Actor id returned by list_actors."},
    "type": {"type": "string", "description": "Envelope type returned by describe_actor."}
  },
  "required": ["actor_id", "type"]
}`),
}

type describeTypeParams struct {
	ActorID string `json:"actor_id"`
	Type    string `json:"type"`
}

// ExecuteDescribeType is the protocol-layer execute function for describe_type.
func ExecuteDescribeType(ctx context.Context, params json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Call == nil {
		return NewError("describe_type", InternalError, "describe_type tool not configured", "Retry after the bridge is configured", nil)
	}
	var p describeTypeParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return PayloadInvalidError("describe_type", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return PayloadInvalidError("describe_type", "actor_id is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}
	if p.Type == "" {
		return PayloadInvalidError("describe_type", "type is required (call describe_actor to discover)", payloadHint(p.ActorID, p.Type))
	}

	if !rc.InTurn() {
		return NewError("describe_type", InternalError, "describe_type invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	payload, _ := json.Marshal(map[string]string{"type": p.Type})
	result := x.CallSyncResult(ctx, rc, RequestSpec{
		ToolName:       "describe_type",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
	return NormalizeCallActorResult(result, p.ActorID, p.Type)
}
