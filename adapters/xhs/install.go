package xhs

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// fallbackResponseSchema is the response-schema "failure branch"
// projection install requires. The framework's
// ValidateFallbackResponseSchema feeds it three sample failed payloads
// and asserts they all parse — the schema below permits any object
// (status + reason fields are not required at validation time, only
// well-formed JSON object shape).
var fallbackResponseSchema = json.RawMessage(`{"type":"object"}`)

// requestSchema is the per-type request payload schema — kept lenient
// for T2's mock path (object only).
var requestSchema = json.RawMessage(`{"type":"object"}`)

// responseSchema mirrors requestSchema — the mock Handle emits
// {status, reason, ...} which always satisfies an object shape.
var responseSchema = json.RawMessage(`{"type":"object"}`)

// declarationTypeSchemas builds the kernel/adapter.TypeSchema map the
// scaffold attaches to its Declaration. R/R types get
// AllowedKinds={request, response}, the event-only type gets
// AllowedKinds={event}.
func declarationTypeSchemas() map[string]adapter.TypeSchema {
	out := make(map[string]adapter.TypeSchema, len(AllTypes))
	for _, t := range RequestResponseTypes {
		out[t] = adapter.TypeSchema{
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  requestSchema,
				message.KindResponse: responseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		}
	}
	out[TypeNoteArchived] = adapter.TypeSchema{
		AllowedKinds: []message.Kind{message.KindEvent},
		SchemasByKind: map[message.Kind]json.RawMessage{
			message.KindEvent: requestSchema,
		},
		TerminalConvention: string(adapter.TerminalPayloadStatus),
	}
	return out
}

// DefaultActorSeed returns the kernel/actor.Record the saga inserts so
// framework.Manager.Install can locate tool:xhs-adapter at boot. T3
// will need to flip the Binding to ViaServerTransit when the device
// adapter replaces this scaffold.
func DefaultActorSeed() actor.Record {
	return actor.Record{
		ID:          DefaultAdapterActorID,
		Kind:        actor.KindTool,
		Binding:     actor.BindingInProcess,
		DisplayName: "xhs",
	}
}
