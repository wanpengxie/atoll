package contract

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

type frameSchema struct {
	Direction string `json:"direction"`
	Payload   string `json:"payload_schema"`
}

type activitySchema struct {
	Payload string `json:"payload_schema"`
}

type goldenSchema struct {
	Schema          string                    `json:"$schema"`
	ID              string                    `json:"$id"`
	Title           string                    `json:"title"`
	ContractVersion string                    `json:"x-contract-version"`
	EnvelopeVersion int                       `json:"x-envelope-version"`
	REST            []Method                  `json:"x-rest-methods"`
	ErrorCodes      []ErrorCode               `json:"x-error-codes"`
	Frames          map[string]frameSchema    `json:"x-websocket-frames"`
	Activities      map[string]activitySchema `json:"x-activity-types"`
	Defs            map[string]any            `json:"$defs"`
}

// GenerateSchema aggregates the app-owned REST contract and the
// subjectgate-owned websocket contract into one deterministic JSON Schema.
func GenerateSchema() ([]byte, error) {
	methods := Methods()
	sort.Slice(methods, func(i, j int) bool {
		if methods[i].Path == methods[j].Path {
			return methods[i].Method < methods[j].Method
		}
		return methods[i].Path < methods[j].Path
	})

	defs := schemaDefinitions()
	seen := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		if method.Since == "" {
			return nil, fmt.Errorf("contract registry: %s %s has no since version", method.Method, method.Path)
		}
		key := method.Method + " " + method.Path
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("contract registry: duplicate method %s", key)
		}
		seen[key] = struct{}{}
		isExperimentalPath := strings.HasPrefix(method.Path, "/api/experimental/")
		if method.Experimental != isExperimentalPath {
			return nil, fmt.Errorf("contract registry: %s experimental=%v does not match its namespace", key, method.Experimental)
		}
		references := []string{method.PathSchema, method.QuerySchema, method.BodySchema, method.Response}
		for _, name := range append(references, method.Errors...) {
			if _, ok := defs[name]; !ok {
				return nil, fmt.Errorf("contract registry: %s references undefined schema %q", key, name)
			}
		}
	}

	frames, err := frameTable()
	if err != nil {
		return nil, err
	}

	doc := goldenSchema{
		Schema:          "https://json-schema.org/draft/2020-12/schema",
		ID:              "https://atoll.local/schema/engine-api-1.0.json",
		Title:           "Atoll engine API contract",
		ContractVersion: Version,
		EnvelopeVersion: subjectgate.FrameVersion,
		REST:            methods,
		ErrorCodes:      ErrorCodes(),
		Frames:          frames,
		Activities:      activityTable(),
		Defs:            defs,
	}
	return json.MarshalIndent(doc, "", "  ")
}

func activityTable() map[string]activitySchema {
	out := make(map[string]activitySchema, len(registry.ActivityTypes()))
	for _, decl := range registry.ActivityTypes() {
		out[string(decl.Type)] = activitySchema{Payload: decl.SchemaName}
	}
	return out
}

// framePayloads maps each frame type to its payload schema name. The frame
// LIST itself comes from subjectgate.FrameTypesByDirection (the one machine
// registry) — adding a frame type in subjectgate without a payload mapping
// here fails generation loudly (and therefore reddens the golden test) instead
// of silently omitting the frame from the contract.
var framePayloads = map[string]string{
	"attach": "AttachPayload", "submit": "SubmitPayload", "resolve": "ResolvePayload",
	"cancel": "CancelPayload", "after": "AfterPayload", "cancel_timer": "CancelTimerPayload",
	"resource": "ResourcePayload", "observe": "ObservePayload", "unobserve": "UnobservePayload",
	"feed": "FeedPayload", "receipt": "ReceiptPayload", "error": "ErrorPayload",
	"observe_ended": "ObserveEndedPayload",
}

func frameTable() (map[string]frameSchema, error) {
	frames := map[string]frameSchema{}
	for _, dir := range []subjectgate.FrameDirection{subjectgate.DirUpstream, subjectgate.DirDownstream} {
		for _, name := range subjectgate.FrameTypesByDirection(dir) {
			payload, ok := framePayloads[name]
			if !ok {
				return nil, fmt.Errorf("contract frames: frame type %q has no payload schema mapping", name)
			}
			frames[name] = frameSchema{Direction: string(dir), Payload: payload}
		}
	}
	if len(frames) != len(framePayloads) {
		return nil, fmt.Errorf("contract frames: payload map lists %d frames but the subjectgate registry carries %d", len(framePayloads), len(frames))
	}
	return frames, nil
}

func schemaDefinitions() map[string]any {
	defs := map[string]any{
		"none":                   false,
		"Binary":                 map[string]any{"type": "string", "contentEncoding": "binary"},
		"EventStream":            map[string]any{"type": "string", "contentMediaType": "text/event-stream"},
		"SubjectgateFrameStream": map[string]any{"$ref": "#/$defs/DownstreamEnvelope"},
	}

	// REST request DTOs are fail-closed.
	for name, value := range map[string]any{
		"RegisterRequest": RegisterRequest{}, "LoginRequest": LoginRequest{},
		"CreateChannelRequest": CreateChannelRequest{}, "CreateDaemonRequest": CreateDaemonRequest{},
		"AttachDaemonRequest": AttachDaemonRequest{}, "IntroduceActorRequest": IntroduceActorRequest{},
		"DeclarationOverlayRequest": DeclarationOverlayRequest{},
		"DeclarationCreateRequest":  DeclarationCreateRequest{},
		"DeclarationUpdateRequest":  DeclarationUpdateRequest{},
		"ChannelPath":               ChannelPath{}, "ChannelResourcePath": ChannelResourcePath{},
		"ChannelActorPath": ChannelActorPath{}, "ChannelDeclarationPath": ChannelDeclarationPath{},
		"DeclarationPath": DeclarationPath{}, "DaemonPath": DaemonPath{},
		"ChannelDaemonPath": ChannelDaemonPath{}, "ChannelListQuery": ChannelListQuery{},
		"MessagePageQuery": MessagePageQuery{}, "ResourceListQuery": ResourceListQuery{},
	} {
		defs[name] = schemaForType(reflect.TypeOf(value), false)
	}
	// REST outputs are open to additive fields but retain all known field types.
	for name, value := range map[string]any{
		"Error": Error{}, "Meta": Meta{}, "Principal": Principal{}, "OK": OK{},
		"Channel": Channel{}, "ChannelList": ChannelList{}, "ChannelDeletion": ChannelDeletion{},
		"CandidateList": CandidateList{}, "MessagePage": MessagePage{}, "Membership": Membership{},
		"IntroduceActorResponse": IntroduceActorResponse{}, "RemoveActorResponse": RemoveActorResponse{},
		"Declaration": Declaration{}, "DeclarationList": DeclarationList{},
		"DeclarationMutation": DeclarationMutation{}, "DeclarationOverlay": DeclarationOverlay{},
		"Daemon": Daemon{}, "DaemonList": DaemonList{}, "DaemonDeletion": DaemonDeletion{},
		"DaemonBinding": DaemonBinding{}, "ResourcePage": channel.ResourcePage{},
		"ResourceMeta": channel.ResourceMeta{},
	} {
		defs[name] = schemaForType(reflect.TypeOf(value), true)
	}
	errorDef := defs["Error"].(map[string]any)
	errorProperties := errorDef["properties"].(map[string]any)
	errorProperties["code"] = map[string]any{"type": "string", "x-known-values": ErrorCodes()}
	agentPayload := schemaForType(reflect.TypeOf(AgentMessagePayload{}), true)
	agentPayload["description"] = "Open payload convention for messages addressed to agents; absent intent means steer. Intent and expected_turn_id stay inside payload and are opaque to the substrate."
	agentPayloadProperties := agentPayload["properties"].(map[string]any)
	agentPayloadProperties["intent"] = map[string]any{
		"type": "string", "x-known-values": []AgentIntent{AgentIntentSteer, AgentIntentInterrupt},
		"description": "Provider delivery intent. Omitted means steer; the vocabulary grows additively.",
	}
	defs["AgentMessagePayload"] = agentPayload

	for _, decl := range registry.ActivityTypes() {
		defs[decl.SchemaName] = schemaForType(reflect.TypeOf(decl.Payload), false)
		props := defs[decl.SchemaName].(map[string]any)["properties"].(map[string]any)
		switch decl.Type {
		case registry.ActivityTurnStarted, registry.ActivityToolStarted:
			props["status"] = map[string]any{"type": "string", "const": registry.ActivityStatusStarted}
		case registry.ActivityTurnEnded, registry.ActivityToolEnded:
			props["status"] = map[string]any{"type": "string", "enum": []string{
				registry.ActivityStatusCompleted, registry.ActivityStatusFailed,
			}}
		}
	}

	// Chain-link schemas retain their substrate source. Upstream definitions are
	// closed; downstream definitions are open for must-ignore evolution.
	for name, value := range map[string]any{
		"AttachPayload": subjectgate.AttachPayload{}, "SubmitPayload": subjectgate.SubmitPayload{},
		"ResolvePayload": subjectgate.ResolvePayload{}, "CancelPayload": subjectgate.CancelPayload{},
		"AfterPayload": subjectgate.AfterPayload{}, "CancelTimerPayload": subjectgate.CancelTimerPayload{},
		"ResourcePayload": subjectgate.ResourcePayload{}, "ObservePayload": subjectgate.ObservePayload{},
		"UnobservePayload": subjectgate.UnobservePayload{},
	} {
		defs[name] = schemaForType(reflect.TypeOf(value), false)
	}
	defs["SubmitPayload"].(map[string]any)["description"] = "Idempotency key is (channel_id,id). The canonical client fingerprint covers msg_type, normalized kind, JSON-semantic payload, explicit audience, normalized visibility, parent_id, and explicit expires_at_ms; it excludes ref, id, generated deadlines, and default request-audience completion. Omitted kind/visibility/payload and their explicit default values are equivalent; an omitted event audience is the canonical empty array."
	for name, value := range map[string]any{
		"FeedPayload": subjectgate.FeedPayload{}, "ErrorPayload": subjectgate.ErrorPayload{},
		"AttachReceipt": subjectgate.AttachReceipt{}, "SubmitReceipt": subjectgate.SubmitReceipt{},
		"ResolveReceipt": subjectgate.ResolveReceipt{}, "CancelReceipt": subjectgate.CancelReceipt{},
		"AfterReceipt": subjectgate.AfterReceipt{}, "CancelTimerReceipt": subjectgate.CancelTimerReceipt{},
		"ObserveReceipt": subjectgate.ObserveReceipt{}, "UnobserveReceipt": subjectgate.UnobserveReceipt{},
		"ResourceOutcome": subjectgate.ResourceOutcome{}, "ResourceStat": subjectgate.ResourceStat{},
		"SubjectResourcePage": subjectgate.ResourcePage{}, "ObserveEndedPayload": subjectgate.ObserveEndedPayload{},
	} {
		defs[name] = schemaForType(reflect.TypeOf(value), true)
	}
	wsError := defs["ErrorPayload"].(map[string]any)
	wsErrorProperties := wsError["properties"].(map[string]any)
	wsErrorProperties["code"] = map[string]any{
		"type": "string", "x-known-values": subjectgate.ErrorCodes(),
	}
	observeEnded := defs["ObserveEndedPayload"].(map[string]any)
	observeEndedProperties := observeEnded["properties"].(map[string]any)
	observeEndedProperties["reason"] = map[string]any{
		"type": "string", "x-known-values": subjectgate.ObserveEndedReasons(),
	}
	defs["ReceiptPayload"] = map[string]any{"oneOf": []any{
		map[string]any{"$ref": "#/$defs/AttachReceipt"}, map[string]any{"$ref": "#/$defs/SubmitReceipt"},
		map[string]any{"$ref": "#/$defs/ResolveReceipt"}, map[string]any{"$ref": "#/$defs/CancelReceipt"},
		map[string]any{"$ref": "#/$defs/AfterReceipt"}, map[string]any{"$ref": "#/$defs/CancelTimerReceipt"},
		map[string]any{"$ref": "#/$defs/ObserveReceipt"}, map[string]any{"$ref": "#/$defs/UnobserveReceipt"},
		map[string]any{"$ref": "#/$defs/ResourceOutcome"}, map[string]any{"$ref": "#/$defs/ResourceStat"},
		map[string]any{"$ref": "#/$defs/SubjectResourcePage"},
	}}
	defs["UpstreamEnvelope"] = envelopeSchema(false, subjectgate.FrameTypesByDirection(subjectgate.DirUpstream))
	defs["DownstreamEnvelope"] = envelopeSchema(true, subjectgate.FrameTypesByDirection(subjectgate.DirDownstream))
	return defs
}

func envelopeSchema(open bool, known []string) map[string]any {
	frameType := map[string]any{"type": "string"}
	if open {
		frameType["x-known-values"] = known
	} else {
		frameType["enum"] = known
	}
	return map[string]any{
		"type": "object", "required": []string{"v", "frame_type"}, "additionalProperties": open,
		"properties": map[string]any{
			"v":          map[string]any{"type": "integer", "const": subjectgate.FrameVersion},
			"frame_type": frameType,
			"ref":        map[string]any{"type": "string"}, "payload": map[string]any{},
		},
	}
}

func schemaForType(t reflect.Type, open bool) map[string]any {
	if t.Kind() == reflect.Pointer {
		return map[string]any{"anyOf": []any{
			schemaForType(t.Elem(), open), map[string]any{"type": "null"},
		}}
	}
	if t == reflect.TypeOf(json.RawMessage{}) {
		return map[string]any{}
	}
	switch t.Kind() {
	case reflect.Struct:
		properties := map[string]any{}
		var required []string
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" {
				name = field.Name
			}
			if name == "-" {
				continue
			}
			properties[name] = schemaForType(field.Type, open)
			if !strings.Contains(options, "omitempty") {
				required = append(required, name)
			}
		}
		out := map[string]any{"type": "object", "properties": properties, "additionalProperties": open}
		if len(required) != 0 {
			out["required"] = required
		}
		return out
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaForType(t.Elem(), open)}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaForType(t.Elem(), open)}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Interface:
		return map[string]any{}
	default:
		return map[string]any{"type": "string"}
	}
}
