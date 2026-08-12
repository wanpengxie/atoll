package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// ListActorsSpec is the protocol-layer definition of list_actors.
var ListActorsSpec = ToolSpec{
	Name: "list_actors",
	Description: strings.TrimSpace(`
Discover what actors (tool adapters, agents, system actors) exist in this
channel RIGHT NOW. Returns a thin directory: per actor its actor_id, kind,
present (live cell/port bound now), uptime_ms when live, and optional device
status. Types and payload docs are NOT
listed here — drill into one actor with describe_actor / describe_type.

This is a LIVE query — the catalog reflects the channel's current membership at
the moment you call. An actor that came online after this task started will
appear here. Call it whenever you need the current actor set.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{}}`),
}

// ExecuteListActors is the protocol-layer execute function for list_actors.
func ExecuteListActors(ctx context.Context, _ json.RawMessage, x *Exec, rc RuntimeContext) ResultValue {
	if x == nil || x.Call == nil {
		return NewError("list_actors", InternalError, "list_actors tool not configured", "Retry after the bridge is configured", nil)
	}
	if !rc.InTurn() {
		return NewError("list_actors", InternalError, "list_actors invoked outside a bridge turn", "Retry from inside an active bridge turn", nil)
	}
	raw, failure := x.CallSyncRaw(ctx, rc, RequestSpec{
		ToolName:       "list_actors",
		EnvelopeType:   "actor.list",
		HandlerActorID: string(actor.SystemActorID),
		Payload:        CloneRawJSON(json.RawMessage(`{}`)),
		Timeout:        DefaultTimeout,
		WaitMode:       WaitUnbounded,
	})
	if failure != nil {
		// CallSyncRaw already categorised the failure into the actor-CLI
		// closed set (timeout vs internal_error vs terminal-reason mapping).
		return *failure
	}
	var catalog introspect.Catalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return NewError("list_actors", InternalError,
			fmt.Sprintf("decode actor.list catalog: %v", err),
			"Inspect adapter logs and retry", nil)
	}
	return ResultValue{
		Name:  "list_actors",
		Value: FormatCatalog(catalog),
	}
}
