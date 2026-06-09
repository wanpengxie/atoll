package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
)

// ListActorsSpec is the protocol-layer definition of list_actors.
var ListActorsSpec = ToolSpec{
	Name: "list_actors",
	Description: strings.TrimSpace(`
Discover what actors (tool adapters, agents, system actors) and request-callable types
exist in this channel RIGHT NOW. Returns:
  - actors: each with actor_id, kind, binding, readiness, and the types it handles
  - types per actor: name, description, allowed_kinds, max_pending_ms hint

This is a LIVE query — the catalog reflects the channel's current membership at the moment
you call. An actor that came online after this task started will appear here. Call it
whenever you need the current set of (actor_id, type) pairs; call_actor and describe_* also
go through the envelope path so the daemon always validates against live state.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{}}`),
}

// ExecuteListActors is the protocol-layer execute function for list_actors.
func ExecuteListActors(ctx context.Context, exec Executor, rc RuntimeContext) callkit.ResultValue {
	if exec == nil {
		return errorResultValue("list_actors", "list_actors tool not configured")
	}
	if rc.IPC == nil {
		return errorResultValue("list_actors", "list_actors invoked outside a bridge turn")
	}
	raw, ok := exec.ExecuteReservedRaw(ctx, rc, callkit.RequestSpec{
		ToolName:       "list_actors",
		EnvelopeType:   "actor.list",
		HandlerActorID: string(actor.SystemActorID),
		Payload:        callkit.CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        callkit.DefaultTimeout,
		WaitMode:       callkit.WaitUnbounded,
	})
	if !ok {
		return errorResultValue("list_actors", "list_actors did not receive a live catalog (request still pending or failed); retry")
	}
	var catalog introspect.Catalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return errorResultValue("list_actors", fmt.Sprintf("decode actor.list catalog: %v", err))
	}
	return callkit.ResultValue{
		Name:  "list_actors",
		Value: FormatCatalog(catalog),
	}
}

// errorResultValue builds a simple error ResultValue (not the actor-CLI
// closed set — just a plain error string for framework-level failures).
func errorResultValue(toolName, msg string) callkit.ResultValue {
	return callkit.ResultValue{
		Name:    toolName,
		Value:   map[string]any{"error": strings.TrimSpace(msg)},
		IsError: true,
	}
}
