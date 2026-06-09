package metatool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// ToolSpec is the protocol-layer definition of one meta tool: its name,
// description, and JSON schema. This lives in lib/metatool because meta
// tools are a substrate concept — any actor runtime (go-kimi, future
// runtimes) wraps ToolSpec into its own tool type.
type ToolSpec struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

// Executor provides the request-execution surface that meta tool Execute
// functions need. The actors/agent.Bridge implements this; future actor
// runtimes implement it too.
//
// The interface is derived from what the meta-tool protocol requires, not
// from any downstream Bridge shape.
type Executor interface {
	// ExecuteRequest emits an envelope request through the fast-path and
	// returns the result (inline final, ack, or error).
	ExecuteRequest(ctx context.Context, rc RuntimeContext, spec callkit.RequestSpec) callkit.ResultValue

	// ExecuteReservedRaw emits a reserved-type request and returns the
	// raw JSON of the final response payload. Returns (nil, false) on
	// failure or timeout.
	ExecuteReservedRaw(ctx context.Context, rc RuntimeContext, spec callkit.RequestSpec) (json.RawMessage, bool)

	// CallerInstance returns the worker-side caller helper for direct
	// future inspection (await_result / abandon / list_pending).
	CallerInstance() *callkit.Client
}

// IPC is the minimal envelope-writing surface meta tools need. It is a
// subset of the worker IPC facade.
type IPC interface {
	WriteEnvelope(ctx context.Context, env message.Envelope) error
	ChannelID() channel.ID
	WorkerActorID() actor.ActorID
}

// Trigger carries the envelope + correlation id that triggered the
// current turn.
type Trigger struct {
	Envelope      message.Envelope
	CorrelationID message.ID
}

// RuntimeContext is the per-turn context passed into every meta tool
// Execute function.
type RuntimeContext struct {
	IPC     IPC
	Trigger Trigger
}

// payloadHint builds the recovery hint for a payload_invalid error.
// Tool names (list_actors, describe_type) live here in metatool, not in callkit.
func payloadHint(actorID, typeName string) string {
	if actorID != "" && typeName != "" {
		return fmt.Sprintf("Call describe_type(%q, %q) to see payload_example", actorID, typeName)
	}
	return "Call list_actors to see actors, then describe_actor for their types"
}
