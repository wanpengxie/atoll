package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
// from any downstream Bridge shape. The futures mechanism behind the async
// methods is the executor's PRIVATE internals (an agent-side collector
// behind a non-blocking Receive) — metatool only names the semantics.
type Executor interface {
	// ExecuteRequest emits an envelope request through the fast-path and
	// returns the result (inline final, ack, or error).
	ExecuteRequest(ctx context.Context, rc RuntimeContext, spec RequestSpec) ResultValue

	// ExecuteReservedRaw emits a reserved-type request and returns the
	// raw JSON of the final response payload. Returns (nil, false) on
	// failure or timeout.
	ExecuteReservedRaw(ctx context.Context, rc RuntimeContext, spec RequestSpec) (json.RawMessage, bool)

	// AwaitRequest blocks up to window for the final response of an
	// in-flight request (await_result semantics).
	AwaitRequest(ctx context.Context, id message.ID, window time.Duration) (final *message.Envelope, ok bool, err error)

	// AbandonRequest drops the local waiter for id (abandon semantics —
	// no downstream cancel; the result still returns as a new message).
	AbandonRequest(id message.ID)

	// PendingRequests returns the in-flight request ids (list_pending).
	PendingRequests() []message.ID

	// RequestInFlight reports whether id is currently tracked.
	RequestInFlight(id message.ID) bool
}

// Trigger carries the envelope + correlation id that triggered the
// current turn.
type Trigger struct {
	Envelope      message.Envelope
	CorrelationID message.ID
}

// RuntimeContext is the per-turn context passed into every meta tool
// Execute function. A zero Trigger (empty envelope id) marks an
// invocation outside a live turn.
type RuntimeContext struct {
	Trigger Trigger
}

// InTurn reports whether this context belongs to a live turn.
func (rc RuntimeContext) InTurn() bool { return rc.Trigger.Envelope.ID != "" }

// payloadHint builds the recovery hint for a payload_invalid error.
// Tool names (list_actors, describe_type) live here in metatool, not in
func payloadHint(actorID, typeName string) string {
	if actorID != "" && typeName != "" {
		return fmt.Sprintf("Call describe_type(%q, %q) to see payload_example", actorID, typeName)
	}
	return "Call list_actors to see actors, then describe_actor for their types"
}
