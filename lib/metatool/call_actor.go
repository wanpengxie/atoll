package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/lib/callkit"
)

// CallActorSpec is the protocol-layer definition of call_actor.
var CallActorSpec = ToolSpec{
	Name: "call_actor",
	Description: strings.TrimSpace(`
Invoke any tool actor or sub-agent in this channel by emitting a kind=request envelope.
This is the universal invocation primitive — every adapter (browser automation, business APIs,
device controllers) and every sub-agent is reached through this single tool.

Workflow:
  1. Call list_actors first to inspect the daemon-provided bootstrap actor/type
     display snapshot.
  2. Call describe_actor when you need the actor's workflows.
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
func ExecuteCallActor(ctx context.Context, params json.RawMessage, exec Executor, rc RuntimeContext) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("call_actor", callkit.InternalError, "call_actor tool not configured", "Retry after the bridge is configured", nil)
	}
	var p callActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return callkit.PayloadInvalidError("call_actor", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return callkit.PayloadInvalidError("call_actor", "actor_id is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}
	if p.Type == "" {
		return callkit.PayloadInvalidError("call_actor", "type is required (call list_actors to discover)", payloadHint(p.ActorID, p.Type))
	}

	if rc.IPC == nil {
		return callkit.NewError("call_actor", callkit.InternalError, "call_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}

	payload, err := callkit.NormalizePayload(p.Payload)
	if err != nil {
		return callkit.PayloadInvalidError("call_actor", err.Error(), payloadHint(p.ActorID, p.Type))
	}

	mode := callkit.WaitFastPath
	if p.Wait != nil {
		if *p.Wait {
			mode = callkit.WaitUnbounded
		} else {
			mode = callkit.WaitNone
		}
	}

	result := exec.ExecuteRequest(ctx, rc, callkit.RequestSpec{
		ToolName:       "call_actor",
		EnvelopeType:   p.Type,
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        callkit.DefaultTimeout,
		WaitMode:       mode,
	})
	return callkit.NormalizeCallActorResult(result, p.ActorID, p.Type)
}
