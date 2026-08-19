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
Invoke a discovered member actor in this channel by emitting a request envelope.
This channel's own fixed system door has first-class system_describe/system_call
tools and is not reached through this generic member primitive. A peer member is
different: it is an ordinary member that happens to be a door onto another
channel, so every word it accepts — including system words, when describe_actor
lists them — is sent through call_actor, addressed to the peer.

Workflow:
  1. Call list_actors first to see who is in the channel (thin directory:
     actor_id, kind, present, uptime_ms, and optional device; no types).
  2. Call describe_actor for the chosen actor's D21 manifest and words map.
  3. Call describe_type when you need the input/output schemas, examples, or
     adapter-specific error codes for the selected type.
  4. Call call_actor.

Result shapes (fast-path, default):
  - Short calls (the downstream finishes within the fast-path window, ~15s by
    default): the response payload arrives INLINE in your tool result, exactly
    as if it were synchronous.
    On failure the result is {ok:false,error:{code,message,recovery_hint,detail?}}
    where code is the actor-CLI closed set.
  - Long calls (still running past the fast-path window): you get an ACK instead —
    {status:"accepted", request_id, est_wait_ms, guidance, to_wait, if_not_waiting}.
    The call keeps running. Its final is retained by the caller job table and is
    claimed explicitly with await_result(request_id). Use list_pending() to see
    what is still in flight, and cancel(request_id) to stop one.

wait parameter:
  - omit / true (default behaviour above is bounded; pass wait=true for sync):
    wait=true waits up to the request deadline before degrading to an ack.
  - wait=false: returns the ack IMMEDIATELY without waiting at all. Use this to
    FAN OUT several calls in parallel, then await_result / cancel each as needed.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Target actor id, e.g. tool:xhs or agent:research-assistant. Look up via list_actors."},
    "type": {"type": "string", "description": "Envelope type to send, e.g. xhs.publish or kimi.command. MUST be a request-allowed type for the chosen actor."},
    "payload": {"type": "object", "description": "Type-specific payload. Consult describe_type for its documented shape."},
    "wait": {"type": "boolean", "description": "Optional. Omit for bounded fast-path (final inline within ~15s, else ack). true = wait up to the request deadline (sync). false = return ack immediately without waiting (fan-out)."}
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
// Exec face (the substrate JobTable) for envelope dispatch.
func ExecuteCallActor(ctx context.Context, params json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Jobs == nil {
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

	if !rc.InTurn() {
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

	result := x.ExecuteRequest(ctx, rc, RequestSpec{
		ToolName:       "call_actor",
		EnvelopeType:   p.Type,
		HandlerActorID: p.ActorID,
		Payload:        payload,
		// Timeout left unset: call_actor uses the default request deadline.
		WaitMode: mode,
	})
	return NormalizeCallActorResult(result, p.ActorID, p.Type)
}
