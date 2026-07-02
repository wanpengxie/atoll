package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
)

// ListActorsSpec is the protocol-layer definition of list_actors.
var ListActorsSpec = ToolSpec{
	Name: "list_actors",
	Description: strings.TrimSpace(`
Discover what actors (tool adapters, agents, system actors) exist in this
channel RIGHT NOW. Returns a thin directory: per actor its actor_id, kind,
binding, present (live cell/port bound now), and uptime_ms. Types and payload docs are NOT
listed here — drill into one actor with describe_actor / describe_type.

This is a LIVE query — the catalog reflects the channel's current membership at
the moment you call. An actor that came online after this task started will
appear here. Call it whenever you need the current actor set.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{}}`),
}

// ExecuteListActors is the protocol-layer execute function for list_actors.
func ExecuteListActors(ctx context.Context, _ json.RawMessage, sh *Shell, rc RuntimeContext) ResultValue {
	if sh == nil {
		return errorResultValue("list_actors", "list_actors tool not configured")
	}
	if !rc.InTurn() {
		return errorResultValue("list_actors", "list_actors invoked outside a bridge turn")
	}
	raw, ok := sh.ExecuteReservedRaw(ctx, rc, RequestSpec{
		ToolName:       "list_actors",
		EnvelopeType:   "actor.list",
		HandlerActorID: string(actor.SystemActorID),
		Payload:        CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
	if !ok {
		return errorResultValue("list_actors", "list_actors did not receive a live catalog (request still pending or failed); retry")
	}
	var catalog introspect.Catalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return errorResultValue("list_actors", fmt.Sprintf("decode actor.list catalog: %v", err))
	}
	return ResultValue{
		Name:  "list_actors",
		Value: FormatCatalog(catalog),
	}
}

// errorResultValue builds a simple error ResultValue (not the actor-CLI
// closed set — just a plain error string for framework-level failures).
func errorResultValue(toolName, msg string) ResultValue {
	return ResultValue{
		Name:    toolName,
		Value:   map[string]any{"error": strings.TrimSpace(msg)},
		IsError: true,
	}
}
