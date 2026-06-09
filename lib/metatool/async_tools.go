package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// AwaitResultSpec is the protocol-layer definition of await_result.
var AwaitResultSpec = ToolSpec{
	Name: "await_result",
	Description: strings.TrimSpace(`
Block until a previously-submitted call_actor request reaches its final result, then
return that result. Use this to collect a long call that returned an ack
(status:"accepted") instead of an inline result.

  - request_id: the id from the ack's request_id / to_wait.params.request_id.
  - timeout_ms: optional bound on how long to wait. On timeout you get a
    still-pending ack back (the call keeps running; try again later or let the
    result return as a new message).

If the request is unknown (already collected, abandoned, or lost to a worker
restart), the result is an error explaining so.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."},
    "timeout_ms": {"type": "integer", "description": "Optional max wait in milliseconds. Defaults to the R5 default. On timeout you get a still-pending ack."}
  },
  "required": ["request_id"]
}`),
}

type awaitResultParams struct {
	RequestID string `json:"request_id"`
	TimeoutMs int64  `json:"timeout_ms,omitempty"`
}

// ExecuteAwaitResult is the protocol-layer execute function for await_result.
func ExecuteAwaitResult(ctx context.Context, params json.RawMessage, exec Executor) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("await_result", callkit.InternalError, "await_result tool not configured", "Retry after the bridge is configured", nil)
	}
	var p awaitResultParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return callkit.PayloadInvalidError("await_result", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return callkit.PayloadInvalidError("await_result", "request_id is required (from a prior call_actor ack)", "")
	}

	caller := exec.CallerInstance()
	if !caller.Futures.Registered(reqID) {
		return callkit.NewError(
			"await_result",
			callkit.InternalError,
			fmt.Sprintf("request_id %q is not in flight (already collected, abandoned, or lost to a worker restart)", reqID),
			"Call list_pending() to see in-flight request ids; resubmit the call if needed",
			nil,
		)
	}

	timeout := callkit.DefaultTimeout
	if p.TimeoutMs > 0 {
		timeout = time.Duration(p.TimeoutMs) * time.Millisecond
	}

	finalEnv, ok, err := caller.Await(ctx, reqID, timeout)
	if err != nil {
		caller.Abandon(reqID)
		return callkit.NewError("await_result", callkit.InternalError,
			fmt.Sprintf("await_result %q failed: %v", reqID, err),
			"Inspect adapter logs; the request may have been abandoned", nil)
	}
	if !ok {
		// Still pending after the window — hand control back with an ack.
		return callkit.AckResult("await_result", callkit.AckDescriptor{
			RequestID: reqID,
			Accepted:  true,
			Status:    "accepted",
			EstWaitMs: int64(timeout / time.Millisecond),
			Guidance: "Still running after the wait window. The call keeps running; try await_result again, " +
				"or do other work and react to the result when it returns as a new message.",
			ToWait:    callkit.ToWaitHint{Tool: "await_result", Params: map[string]any{"request_id": reqID.String()}},
			NotWaitng: "result returns as kind=response, parent_id=" + reqID.String() + " new turn trigger",
		})
	}
	rv, _ := callkit.ResultFromResponse("await_result", *finalEnv)
	return rv
}

// AbandonSpec is the protocol-layer definition of abandon.
var AbandonSpec = ToolSpec{
	Name: "abandon",
	Description: strings.TrimSpace(`
Stop waiting locally on a previously-submitted call_actor request. Use this in a
fan-out when one sibling already gave you what you need, or when you no longer care
about a result.

This does NOT cancel the downstream work (there is no protocol-level cancel) — the
call keeps running in the daemon. If it later produces a result, that result will
still come back to you as a new message (parent_id = request_id). Abandon only
releases your local wait so list_pending() no longer shows it and await_result()
will report it as not in flight.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."}
  },
  "required": ["request_id"]
}`),
}

type abandonParams struct {
	RequestID string `json:"request_id"`
}

// ExecuteAbandon is the protocol-layer execute function for abandon.
func ExecuteAbandon(_ context.Context, params json.RawMessage, exec Executor) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("abandon", callkit.InternalError, "abandon tool not configured", "Retry after the bridge is configured", nil)
	}
	var p abandonParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return callkit.PayloadInvalidError("abandon", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return callkit.PayloadInvalidError("abandon", "request_id is required", "")
	}
	exec.CallerInstance().Abandon(reqID)
	return callkit.ResultValue{
		Name: "abandon",
		Value: map[string]any{
			"abandoned":  reqID.String(),
			"note":       "local wait released; downstream work is not cancelled (no protocol-level cancel). A later result returns as a new message.",
			"request_id": reqID.String(),
		},
	}
}

// ListPendingSpec is the protocol-layer definition of list_pending.
var ListPendingSpec = ToolSpec{
	Name: "list_pending",
	Description: strings.TrimSpace(`
List the request_ids of call_actor requests you have submitted that have not yet
reached their final result. Returns only the id list — no status — as a decision aid
for which await_result / abandon to issue next.

Note: this view is per-process. If the worker restarted, the list is empty even
though earlier calls may still be running in the daemon; their results will return
as new messages.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{}}`),
}

// ExecuteListPending is the protocol-layer execute function for list_pending.
func ExecuteListPending(_ context.Context, exec Executor) callkit.ResultValue {
	if exec == nil {
		return callkit.NewError("list_pending", callkit.InternalError, "list_pending tool not configured", "Retry after the bridge is configured", nil)
	}
	ids := exec.CallerInstance().Pending()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return callkit.ResultValue{
		Name: "list_pending",
		Value: map[string]any{
			"pending": out,
			"count":   len(out),
		},
	}
}
