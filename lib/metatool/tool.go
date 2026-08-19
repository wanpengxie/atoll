package metatool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/message"
)

// ToolSpec is the protocol-layer definition of one meta tool: its name,
// description, and JSON schema. This lives in lib/metatool because meta
// tools are a substrate concept — any actor runtime wraps ToolSpec into its own
// tool type.
type ToolSpec struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

// MetaTool pairs a tool's spec with its Execute binding onto the Exec face.
// Every meta tool shares the one Execute shape so a runtime can wrap them
// uniformly by iterating MetaTools() — no per-tool wiring. The Exec face is the
// substrate's out-station JobTable + a synchronous sys.Call face (期10 S5: the
// historical private Shell collapsed into the engine's ledger).
type MetaTool struct {
	Spec    ToolSpec
	Execute func(ctx context.Context, params json.RawMessage, x *Exec, rc RuntimeContext) ResultValue
}

// MetaTools returns the channel's fixed nine-tool catalog: six member
// discovery/invocation tools plus the three first-class system-door tools.
// tools (invoke, collect, discover) each paired with its Execute binding.
// This is the data-driven surface a runtime iterates to build its own
// tool type — the catalog is authored once here, not restated per binding.
func MetaTools() []MetaTool {
	return []MetaTool{
		{Spec: ListActorsSpec, Execute: ExecuteListActors},
		{Spec: SystemDescribeSpec, Execute: ExecuteSystemDescribe},
		{Spec: SystemCallSpec, Execute: ExecuteSystemCall},
		{Spec: DescribeActorSpec, Execute: ExecuteDescribeActor},
		{Spec: DescribeTypeSpec, Execute: ExecuteDescribeType},
		{Spec: CallActorSpec, Execute: ExecuteCallActor},
		{Spec: AwaitResultSpec, Execute: ExecuteAwaitResult},
		{Spec: CancelSpec, Execute: ExecuteCancel},
		{Spec: ListPendingSpec, Execute: ExecuteListPending},
	}
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
// Tool names (list_actors, describe_type) live here in metatool, not in a
// separate names package — the tool vocabulary is owned by this layer.
func payloadHint(actorID, typeName string) string {
	if actorID != "" && typeName != "" {
		return fmt.Sprintf("Call describe_type(%q, %q) to see payload_example", actorID, typeName)
	}
	return "Call list_actors to see actors, then describe_actor for their types"
}
