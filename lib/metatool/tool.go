package metatool

import (
	"encoding/json"
	"fmt"

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
