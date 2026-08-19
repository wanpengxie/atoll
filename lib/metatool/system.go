package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

var SystemDescribeSpec = ToolSpec{
	Name: "system_describe",
	Description: strings.TrimSpace(`
Describe the fixed system door and all system words it accepts. The result is
grouped by locus: words answered by this channel's membrane, and words forwarded
to the space registry where caller permissions are checked. Use list_actors for
the member roster; the door is not a member and never appears there.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
}

func ExecuteSystemDescribe(ctx context.Context, _ json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Call == nil {
		return NewError("system_describe", InternalError, "system_describe tool not configured", "Retry after the bridge is configured", nil)
	}
	if !rc.InTurn() {
		return NewError("system_describe", InternalError, "system_describe invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	result := x.CallSyncResult(ctx, rc, RequestSpec{
		ToolName:       "system_describe",
		EnvelopeType:   introspect.QueryDescribe,
		HandlerActorID: string(actor.SystemActorID),
		Payload:        CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
	result = NormalizeCallActorResult(result, string(actor.SystemActorID), introspect.QueryDescribe)
	if result.IsError {
		return result
	}
	raw, err := json.Marshal(result.Value)
	if err != nil {
		return NewError("system_describe", InternalError, err.Error(), "Inspect adapter logs", nil)
	}
	var manifest introspect.Describe
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return NewError("system_describe", InternalError, "decode system manifest: "+err.Error(), "Inspect adapter logs", nil)
	}
	local := map[string]introspect.WordSpec{}
	space := map[string]introspect.WordSpec{}
	for name, spec := range manifest.Words {
		entry, ok := message.Parse(name)
		if !ok || entry.Kind != message.KindRequest {
			continue
		}
		if entry.Locus == message.SystemLocusMembrane {
			local[name] = spec
		} else {
			space[name] = spec
		}
	}
	return ResultValue{Name: "system_describe", Value: map[string]any{
		"class": manifest.Class,
		"loci": map[string]any{
			"channel_membrane": map[string]any{"description": "answered by this channel's system door", "words": local},
			"space_registry":   map[string]any{"description": "forwarded to the space registry and subject to caller permissions", "words": space},
		},
	}}
}

var SystemCallSpec = ToolSpec{
	Name: "system_call",
	Description: strings.TrimSpace(`
Invoke one request word exposed by system_describe through this channel's fixed
system door. Pass the word and its bare payload object. For the member roster use
list_actors, not system_call. Failures are returned as structured tool results.
`),
	Schema: systemCallSchema(),
}

type systemCallParams struct {
	Word    string          `json:"word"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func ExecuteSystemCall(ctx context.Context, params json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Jobs == nil {
		return NewError("system_call", InternalError, "system_call tool not configured", "Retry after the bridge is configured", nil)
	}
	var p systemCallParams
	if err := decodeToolObject(params, &p); err != nil {
		return PayloadInvalidError("system_call", "invalid params: "+err.Error(), "Call system_describe to inspect system words and payload schemas")
	}
	p.Word = strings.TrimSpace(p.Word)
	entry, ok := message.Parse(p.Word)
	if !ok || entry.Kind != message.KindRequest {
		return PayloadInvalidError("system_call", fmt.Sprintf("unknown system request word %q", p.Word), "Call system_describe and choose one of its request words")
	}
	if !rc.InTurn() {
		return NewError("system_call", InternalError, "system_call invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	payload, err := NormalizePayload(p.Payload)
	if err != nil {
		return PayloadInvalidError("system_call", err.Error(), "Call system_describe to inspect the selected word's payload schema")
	}
	result := x.ExecuteRequest(ctx, rc, RequestSpec{
		ToolName:       "system_call",
		EnvelopeType:   p.Word,
		HandlerActorID: string(actor.SystemActorID),
		Payload:        payload,
		WaitMode:       WaitFastPath,
	})
	return NormalizeCallActorResult(result, string(actor.SystemActorID), p.Word)
}

func decodeToolObject(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return fmt.Errorf("parameters must be an object")
	}
	return nil
}

func systemCallSchema() json.RawMessage {
	docs := introspect.SystemWordSpecs()
	variants := make([]any, 0)
	for _, entry := range message.SystemEntries() {
		if entry.Kind == message.KindRequest {
			var payload any
			if err := json.Unmarshal(docs[entry.Name].InputSchema, &payload); err != nil {
				payload = map[string]any{"type": "object"}
			}
			variants = append(variants, map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"word":    map[string]any{"const": entry.Name},
					"payload": payload,
				},
				"required": []string{"word"},
			})
		}
	}
	shape := map[string]any{
		"type":        "object",
		"description": "Choose one system request word and pass its bare parameter object.",
		"oneOf":       variants,
	}
	raw, _ := json.Marshal(shape)
	return raw
}
