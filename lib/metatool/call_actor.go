package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// CallActorSpec is the protocol-layer definition of call_actor.
var CallActorSpec = ToolSpec{
	Name: "call_actor",
	Description: strings.TrimSpace(`
Invoke any tool actor or sub-agent in this channel by emitting a kind=request envelope.
This is the universal invocation primitive — every adapter (browser automation, business APIs,
device controllers) and every sub-agent is reached through this single tool.

Workflow:
  1. Call list_actors first to see who is in the channel (thin directory: no types).
  2. Call describe_actor for the chosen actor's skill doc and full type list.
  3. Call describe_type when you need payload_example, payload_fields, notes, or
     adapter-specific error codes for the selected type.
  4. Call call_actor.

Result shapes (fast-path, default):
  - Short calls (the downstream finishes within ~15s): the response payload
    arrives INLINE in your tool result, exactly as if it were synchronous.
    On failure the result is {ok:false,error:{code,message,recovery_hint,detail?}}
    where code is the actor-CLI closed set.
  - Long calls (still running after ~15s): you get an ACK instead —
    {status:"accepted", request_id, est_wait_ms, guidance, to_wait, if_not_waiting}.
    The call keeps running. To collect it, call await_result(request_id) to block,
    or do other work — the result will return on its own as a NEW message
    (parent_id = request_id) you can react to in a later turn. Use list_pending()
    to see what is still in flight, and abandon(request_id) to stop waiting on one.

wait parameter:
  - omit / true (default behaviour above is bounded; pass wait=true for sync):
    wait=true waits up to the type's full timeout before degrading to an ack.
  - wait=false: returns the ack IMMEDIATELY without waiting at all. Use this to
    FAN OUT several calls in parallel, then await_result / abandon each as needed.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Target actor id, e.g. tool:xhs or agent:research-assistant. Look up via list_actors."},
    "type": {"type": "string", "description": "Envelope type to send, e.g. xhs.publish or kimi.command. MUST be a request-allowed type for the chosen actor."},
    "payload": {"type": "object", "description": "Type-specific payload. Shape is per-adapter convention; consult list_actors output for hints."},
    "wait": {"type": "boolean", "description": "Optional. Omit for bounded fast-path (final inline within ~15s, else ack). true = wait up to the type timeout (sync). false = return ack immediately without waiting (fan-out)."}
  },
  "required": ["actor_id", "type"]
}`),
}

type callActorParams struct {
	ActorID string          `json:"actor_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Wait    *bool           `json:"wait,omitempty"`
}

// ExecuteCallActor is the protocol-layer execute function for call_actor.
// It validates params, normalises the payload, and delegates to the
// Executor for envelope dispatch.
func ExecuteCallActor(ctx context.Context, params json.RawMessage, exec Executor, rc RuntimeContext) ResultValue {
	if exec == nil {
		return NewError("call_actor", InternalError, "call_actor tool not configured", "Retry after the bridge is configured", nil)
	}
	var p callActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return PayloadInvalidError("call_actor", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return PayloadInvalidError("call_actor", "actor_id is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}
	if p.Type == "" {
		return PayloadInvalidError("call_actor", "type is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}

	if rc.IPC == nil {
		return NewError("call_actor", InternalError, "call_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}

	payload, err := NormalizePayload(p.Payload)
	if err != nil {
		return PayloadInvalidError("call_actor", err.Error(), payloadHint(p.ActorID, p.Type))
	}

	mode := WaitFastPath
	if p.Wait != nil {
		if *p.Wait {
			mode = WaitUnbounded
		} else {
			mode = WaitNone
		}
	}

	result := exec.ExecuteRequest(ctx, rc, RequestSpec{
		ToolName:       "call_actor",
		EnvelopeType:   p.Type,
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        DefaultTimeout,
		WaitMode:       mode,
	})
	return NormalizeCallActorResult(result, p.ActorID, p.Type)
}
