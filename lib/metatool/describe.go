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
Returns the actor's live self-answer: its one-line description, a markdown
skill_doc (typical workflows and error handling), and a types map documenting
every request type it serves (payload docs, error codes, allowed kinds, wait
budget). Call this after list_actors when you have identified which actor
matches your need. Kind/binding/presence are directory facts — read them from
list_actors, not from here.
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
func ExecuteDescribeActor(ctx context.Context, params json.RawMessage, sh *Shell, rc RuntimeContext) ResultValue {
	if sh == nil {
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
	return sh.ExecuteRequest(ctx, rc, RequestSpec{
		ToolName:       "describe_actor",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
}

// DescribeTypeSpec is the protocol-layer definition of describe_type.
var DescribeTypeSpec = ToolSpec{
	Name: "describe_type",
	Description: strings.TrimSpace(`
Returns one type's metadata from the actor's live self-answer: description,
payload documentation (example + field-by-field docs), error codes with
recovery hints, allowed envelope kinds, and the wait-budget hint
(max_pending_ms). Call this before call_actor when you need the payload shape
for a single type; describe_actor returns the same metadata for ALL types at
once.
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
func ExecuteDescribeType(ctx context.Context, params json.RawMessage, sh *Shell, rc RuntimeContext) ResultValue {
	if sh == nil {
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
	return sh.ExecuteRequest(ctx, rc, RequestSpec{
		ToolName:       "describe_type",
		EnvelopeType:   "actor.describe",
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
}
