package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Meta-tool strategy (substrate-native): instead of injecting one
// gokimi.Tool per channel-local type (which scales linearly with the
// adapter / type count + bloats every prompt with redundant schema), we
// inject FOUR generic tools that exploit the envelope protocol's
// uniformity:
//
//   list_actors — returns the daemon-provided bootstrap actor/type
//                 display snapshot as structured JSON. LLM calls this
//                 when it doesn't know what's available. The snapshot
//                 is advisory; live truth still flows through envelope
//                 calls and framework validation.
//
//   describe_actor — returns one actor's skill doc and type summary.
//
//   describe_type  — returns one request-callable type's payload
//                    example, field docs, error catalog, and notes.
//
//   call_actor  — emits one kind=request envelope to the named
//                 (actor_id, type) and waits for the response.
//
// Spec ref:
//   - proto-foundation §2.5 Adapter Pattern: all adapters share
//     envelope+kind=request as the uniform invocation primitive
//   - impl-vocabulary §3 adapter-specific type catalog (productized
//     schema lives in product-layer docs, not in protocol)
//   - vision §1.2 self-reference: actor/type catalogs can be surfaced
//     as prompt context while current actor truth remains envelope-only
//
// Token / scaling story:
//   - Old: N type-tools × ~200 token each = O(N) prompt baseline
//   - New: 4 meta tools × ~150 token = O(1) baseline; +1 list_actors
//     payload per task (~1.5k for 30 types, cached across turns)

// CallActorTool is the generic envelope dispatch primitive.
type CallActorTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*CallActorTool)(nil)

func (t *CallActorTool) Name() string { return "call_actor" }

func (t *CallActorTool) Description() string {
	return strings.TrimSpace(`
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
`)
}

// callActorSchema is intentionally permissive on `payload`. Per Level A
// (proto-layer0 §1.4.1) the protocol layer does not validate payload
// schemas; the adapter handler is the boundary that enforces shape.
var callActorSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "actor_id": {"type": "string", "description": "Target actor id, e.g. tool:xhs or agent:research-assistant. Look up via list_actors."},
    "type": {"type": "string", "description": "Envelope type to send, e.g. xhs.publish or kimi.command. MUST be a request-allowed type for the chosen actor."},
    "payload": {"type": "object", "description": "Type-specific payload. Shape is per-adapter convention; consult list_actors output for hints."},
    "wait": {"type": "boolean", "description": "Optional. Omit for bounded fast-path (final inline within ~15s, else ack). true = wait up to the type timeout (sync). false = return ack immediately without waiting (fan-out)."}
  },
  "required": ["actor_id", "type"]
}`)

func (t *CallActorTool) ParameterSchema() json.RawMessage { return cloneRawJSON(callActorSchema) }

type callActorParams struct {
	ActorID string          `json:"actor_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	// Wait is a tri-state: nil = bounded fast-path, true = sync (unbounded to
	// type timeout), false = immediate ack (fan-out).
	Wait *bool `json:"wait,omitempty"`
}

func (t *CallActorTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("call_actor", actorCLIInternalError, "call_actor tool not configured", "Retry after the bridge is configured", nil), nil
	}
	var p callActorParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return payloadInvalidError("call_actor", "", "", fmt.Sprintf("invalid params: %v", err)), nil
		}
	}
	p.ActorID = strings.TrimSpace(p.ActorID)
	p.Type = strings.TrimSpace(p.Type)
	if p.ActorID == "" {
		return payloadInvalidError("call_actor", p.ActorID, p.Type, "actor_id is required (call list_actors to discover)"), nil
	}
	if p.Type == "" {
		return payloadInvalidError("call_actor", p.ActorID, p.Type, "type is required (call list_actors to discover)"), nil
	}

	// No local validation gates. The daemon harness is the single
	// authority on actor/type existence + kind + handler binding: it
	// validates EVERY envelope on the way in and closure (harness Step 8 +
	// framework F3) guarantees a terminal response even when the target is
	// unknown / offline. A worker-side gate could only ever consult a
	// stale local copy and reject an actor that joined the channel after
	// spawn (the tool:xhs-after-spawn bug). We emit directly and let the
	// daemon answer with the real result or a real terminal error.
	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return actorCLIErrorResult("call_actor", actorCLIInternalError, "call_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil), nil
	}
	// The type's max_pending_ms is owned + enforced by the daemon: it stamps
	// expires_at from the type registry when the request leaves it unset
	// (which executeChannelRequest deliberately does). The caller-side wait
	// window is a SEPARATE concern — this timeout feeds only the fast-path
	// Await window (resolveFastPathWindow), so we use the SDK default ceiling
	// and let the substrate pending + F3 govern the true persisted deadline.
	timeout := channelToolDefaultTimeout

	payload, err := normalizeToolPayload(p.Payload)
	if err != nil {
		return payloadInvalidError("call_actor", p.ActorID, p.Type, err.Error()), nil
	}

	mode := waitFastPath
	if p.Wait != nil {
		if *p.Wait {
			mode = waitUnbounded
		} else {
			mode = waitNone
		}
	}

	result := t.bridge.executeChannelRequest(ctx, runtime.ipc, runtime.trigger, channelRequestSpec{
		ToolName:       "call_actor",
		EnvelopeType:   p.Type,
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        timeout,
		WaitMode:       mode,
	})
	return normalizeCallActorError(result, p.ActorID, p.Type), nil
}

// ListActorsTool returns the channel's LIVE actor + request-type catalog
// by emitting an actor.list reserved-type request to the channel system
// actor. The daemon reads its registry + type registry on every call, so
// an actor that joined after the worker spawned is visible immediately —
// there is no frozen bootstrap snapshot.
type ListActorsTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*ListActorsTool)(nil)

func (t *ListActorsTool) Name() string { return "list_actors" }

func (t *ListActorsTool) Description() string {
	return strings.TrimSpace(`
Discover what actors (tool adapters, agents, system actors) and request-callable types
exist in this channel RIGHT NOW. Returns:
  - actors: each with actor_id, kind, binding, readiness, and the types it handles
  - types per actor: name, description, allowed_kinds, max_pending_ms hint

This is a LIVE query — the catalog reflects the channel's current membership at the moment
you call. An actor that came online after this task started will appear here. Call it
whenever you need the current set of (actor_id, type) pairs; call_actor and describe_* also
go through the envelope path so the daemon always validates against live state.
`)
}

func (t *ListActorsTool) ParameterSchema() json.RawMessage {
	return cloneRawJSON(json.RawMessage(`{"type":"object","properties":{}}`))
}

func (t *ListActorsTool) Execute(ctx context.Context, _ json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return channelToolErrorResult("list_actors", "list_actors tool not configured"), nil
	}
	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return channelToolErrorResult("list_actors", "list_actors invoked outside a bridge turn"), nil
	}
	// actor.list is a channel-wide reserved type answered by the channel
	// system actor (the daemon, the registry truth owner). It is a
	// synchronous catalog lookup the agent always needs inline, so wait the
	// full timeout with no fast-path degrade-to-ack.
	raw, ok := t.bridge.executeReservedRequestRaw(ctx, runtime.ipc, runtime.trigger, channelRequestSpec{
		ToolName:       "list_actors",
		EnvelopeType:   "actor.list",
		HandlerActorID: string(actor.SystemActorID),
		Payload:        cloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        channelToolDefaultTimeout,
		WaitMode:       waitUnbounded,
	})
	if !ok {
		return channelToolErrorResult("list_actors", "list_actors did not receive a live catalog (request still pending or failed); retry"), nil
	}
	var snapshot ChannelContext
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return channelToolErrorResult("list_actors", fmt.Sprintf("decode actor.list catalog: %v", err)), nil
	}
	return types.ToolResult{
		Name:  "list_actors",
		Value: types.ToolReturnValue{Value: formatActorRegistryForLLM(snapshot)},
	}, nil
}

// formatActorRegistryForLLM projects a ChannelContext into the JSON
// shape the LLM consumes. Groups types by handler_actor_id so the
// model reasons "this actor handles these types" naturally instead of
// having to correlate flat lists.
//
// Orphan types (handler_actor_id non-empty but no matching actor row,
// or handler_actor_id empty) appear under a sentinel "_orphan" key —
// rare in healthy channels but kept visible for debugging.
func formatActorRegistryForLLM(snapshot ChannelContext) map[string]any {
	actorIndex := make(map[string]ActorInfo, len(snapshot.Actors))
	for _, a := range snapshot.Actors {
		actorIndex[strings.TrimSpace(a.ActorID)] = a
	}
	typesByActor := map[string][]map[string]any{}
	for _, ty := range snapshot.Types {
		handler := strings.TrimSpace(ty.HandlerActorID)
		entry := map[string]any{
			"type":          ty.Type,
			"description":   ty.Description,
			"allowed_kinds": ty.AllowedKinds,
		}
		if ty.MaxPendingMs > 0 {
			entry["max_pending_ms"] = ty.MaxPendingMs
		}
		if handler == "" {
			typesByActor["_orphan"] = append(typesByActor["_orphan"], entry)
			continue
		}
		typesByActor[handler] = append(typesByActor[handler], entry)
	}

	actorKeys := make([]string, 0, len(actorIndex))
	for k := range actorIndex {
		actorKeys = append(actorKeys, k)
	}
	sort.Strings(actorKeys)

	out := make([]map[string]any, 0, len(actorKeys))
	for _, id := range actorKeys {
		a := actorIndex[id]
		entry := map[string]any{
			"actor_id":    a.ActorID,
			"description": a.Description,
			"kind":        a.Kind,
			"ready":       a.Ready,
		}
		if a.Binding != "" {
			entry["binding"] = a.Binding
		}
		if a.ReadyReason != "" {
			entry["ready_reason"] = a.ReadyReason
		}
		if a.LastReadyAt > 0 {
			entry["last_ready_at"] = a.LastReadyAt
		}
		if a.LastStateChangeAt > 0 {
			entry["last_state_change_at"] = a.LastStateChangeAt
		}
		if a.DisplayName != "" {
			entry["display_name"] = a.DisplayName
		}
		ts := typesByActor[id]
		// Sort types deterministically so identical snapshots produce
		// identical responses (stable cache for the LLM).
		sort.Slice(ts, func(i, j int) bool {
			return fmt.Sprint(ts[i]["type"]) < fmt.Sprint(ts[j]["type"])
		})
		entry["types"] = ts
		out = append(out, entry)
	}

	result := map[string]any{
		"actors":     out,
		"channel_id": snapshot.ChannelID,
	}
	if snapshot.ChannelType != "" {
		result["channel_type"] = snapshot.ChannelType
	}
	if orphan := typesByActor["_orphan"]; len(orphan) > 0 {
		result["orphan_types"] = orphan
	}
	return result
}

// channelRequestSpec is the bag of fields a single call_actor
// invocation needs to emit + wait. Lets executeChannelRequest stay
// generic so meta tools and any future specialized tool share one
// wire/response/timeout path.
type channelRequestSpec struct {
	ToolName       string
	EnvelopeType   string
	HandlerActorID string
	Payload        json.RawMessage
	Timeout        time.Duration
	// WaitMode selects the caller-side wait policy (§2.3.2). Zero value
	// (waitFastPath) is the default bounded fast-path.
	WaitMode waitMode
}
