package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/kernel/message"
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

	snapshot := t.bridge.refreshChannelContext(ctx)
	actorInfo, ok := findActor(snapshot, p.ActorID)
	if !ok {
		return unknownActorError("describe_actor", p.ActorID), nil
	}

	typeSummaries := make([]map[string]any, 0)
	for _, ty := range snapshot.Types {
		if strings.TrimSpace(ty.HandlerActorID) != p.ActorID {
			continue
		}
		typeSummaries = append(typeSummaries, map[string]any{
			"type":           ty.Type,
			"description":    ty.Description,
			"allowed_kinds":  ty.AllowedKinds,
			"max_pending_ms": ty.MaxPendingMs,
		})
	}
	sort.Slice(typeSummaries, func(i, j int) bool {
		return fmt.Sprint(typeSummaries[i]["type"]) < fmt.Sprint(typeSummaries[j]["type"])
	})

	value := map[string]any{
		"actor_id":             actorInfo.ActorID,
		"description":          actorInfo.Description,
		"kind":                 actorInfo.Kind,
		"binding":              actorInfo.Binding,
		"skill_doc":            actorInfo.SkillDoc,
		"ready":                actorInfo.Ready,
		"ready_reason":         actorInfo.ReadyReason,
		"last_ready_at":        actorInfo.LastReadyAt,
		"last_state_change_at": actorInfo.LastStateChangeAt,
		"types":                typeSummaries,
	}
	if actorInfo.DisplayName != "" {
		value["display_name"] = actorInfo.DisplayName
	}
	return types.ToolResult{
		Name:  "describe_actor",
		Value: types.ToolReturnValue{Value: value},
	}, nil
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

	snapshot := t.bridge.refreshChannelContext(ctx)
	if _, ok := findActor(snapshot, p.ActorID); !ok {
		return unknownActorError("describe_type", p.ActorID), nil
	}
	typeInfo, found := findType(snapshot, p.Type)
	if !found {
		return unknownTypeError("describe_type", p.ActorID, p.Type), nil
	}
	if strings.TrimSpace(typeInfo.HandlerActorID) != p.ActorID {
		return actorTypeMismatchError("describe_type", p.ActorID, p.Type, typeInfo.HandlerActorID), nil
	}
	if !typeAllowsKind(typeInfo, string(message.KindRequest)) {
		return kindDisallowedError("describe_type", p.Type, typeInfo.AllowedKinds), nil
	}

	value := map[string]any{
		"actor_id":        p.ActorID,
		"type":            typeInfo.Type,
		"description":     typeInfo.Description,
		"allowed_kinds":   typeInfo.AllowedKinds,
		"max_pending_ms":  typeInfo.MaxPendingMs,
		"payload_example": toolPayloadValue(typeInfo.PayloadExample),
		"payload_fields":  typeInfo.PayloadFields,
		"error_codes":     typeInfo.ErrorCodes,
		"notes":           typeInfo.Notes,
	}
	return types.ToolResult{
		Name:  "describe_type",
		Value: types.ToolReturnValue{Value: value},
	}, nil
}
