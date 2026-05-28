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

	"github.com/wanpengxie/ActOS/kernel/message"
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
  4. Call call_actor — the response payload arrives in your tool result.
     On failure the result is {ok:false,error:{code,message,recovery_hint,detail?}}
     where code is the actor-CLI closed set.
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
    "payload": {"type": "object", "description": "Type-specific payload. Shape is per-adapter convention; consult list_actors output for hints."}
  },
  "required": ["actor_id", "type"]
}`)

func (t *CallActorTool) ParameterSchema() json.RawMessage { return cloneRawJSON(callActorSchema) }

type callActorParams struct {
	ActorID string          `json:"actor_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
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

	snapshot := t.bridge.channelContext()
	if _, ok := findActor(snapshot, p.ActorID); !ok {
		return unknownActorError("call_actor", p.ActorID), nil
	}
	typeInfo, found := findType(snapshot, p.Type)
	if !found {
		return unknownTypeError("call_actor", p.ActorID, p.Type), nil
	}
	timeout := channelToolDefaultTimeout
	if strings.TrimSpace(typeInfo.HandlerActorID) != p.ActorID {
		return actorTypeMismatchError("call_actor", p.ActorID, p.Type, typeInfo.HandlerActorID), nil
	}
	if !typeAllowsKind(typeInfo, string(message.KindRequest)) {
		return kindDisallowedError("call_actor", p.Type, typeInfo.AllowedKinds), nil
	}
	timeout = channelToolTimeout(typeInfo.MaxPendingMs)

	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return actorCLIErrorResult("call_actor", actorCLIInternalError, "call_actor invoked outside a bridge turn", "Retry from inside an active bridge turn", nil), nil
	}

	payload, err := normalizeToolPayload(p.Payload)
	if err != nil {
		return payloadInvalidError("call_actor", p.ActorID, p.Type, err.Error()), nil
	}

	result := t.bridge.executeChannelRequest(ctx, runtime.ipc, runtime.trigger, channelRequestSpec{
		ToolName:       "call_actor",
		EnvelopeType:   p.Type,
		HandlerActorID: p.ActorID,
		Payload:        payload,
		Timeout:        timeout,
	})
	return normalizeCallActorError(result, p.ActorID, p.Type), nil
}

// ListActorsTool returns the daemon-provided bootstrap actor + type
// display snapshot as structured JSON. It is not current operational
// truth; call_actor uses the envelope path and lets the daemon/harness
// validate current actor/type state.
type ListActorsTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*ListActorsTool)(nil)

func (t *ListActorsTool) Name() string { return "list_actors" }

func (t *ListActorsTool) Description() string {
	return strings.TrimSpace(`
Discover what actors (tool adapters, agents, system actors) and request-callable types
exist in this channel. Returns:
  - actors: each with actor_id, kind, binding, and the types it handles
  - types per actor: name, description, allowed_kinds, max_pending_ms hint

Call this once at the start of a task. The response is a daemon-built bootstrap snapshot,
stable enough to cache in your reasoning context across multiple turns. Use it as a hint
for (actor_id, type) pairs; call_actor and describe_* use the envelope path for live
validation.
`)
}

func (t *ListActorsTool) ParameterSchema() json.RawMessage {
	return cloneRawJSON(json.RawMessage(`{"type":"object","properties":{}}`))
}

func (t *ListActorsTool) Execute(ctx context.Context, _ json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return channelToolErrorResult("list_actors", "list_actors tool not configured"), nil
	}
	snapshot := t.bridge.channelContext()
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

func (b *Bridge) channelContext() ChannelContext {
	if b == nil {
		return ChannelContext{}
	}
	return b.cfg.ChannelContext
}

// findActor returns the ActorInfo whose ActorID matches actorID.
func findActor(snapshot ChannelContext, actorID string) (ActorInfo, bool) {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return ActorInfo{}, false
	}
	for _, a := range snapshot.Actors {
		if strings.TrimSpace(a.ActorID) == actorID {
			return a, true
		}
	}
	return ActorInfo{}, false
}

// findType returns the TypeInfo whose Type matches name.
func findType(snapshot ChannelContext, name string) (TypeInfo, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return TypeInfo{}, false
	}
	for _, ty := range snapshot.Types {
		if strings.TrimSpace(ty.Type) == name {
			return ty, true
		}
	}
	return TypeInfo{}, false
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
}
