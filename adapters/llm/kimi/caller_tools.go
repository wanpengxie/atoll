package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// The three async-decision meta tools (§2.3.4) that complement call_actor's
// fast-path. They are the agent's JIT control-flow surface over the
// worker-side caller helper's futurereg:
//
//   await_result(request_id, timeout_ms?) — block until that request's final.
//   abandon(request_id)                   — drop the local waiter (substrate
//                                           pending + F3 untouched).
//   list_pending()                        — the ephemeral in-flight id list.
//
// Mechanism vs policy (§0.1): these are pure mechanism. WHEN to await, WHICH
// to abandon, whether to fan-out — all agent policy, computed per turn.

// defaultAwaitTimeout is the fallback await_result window when the agent does
// not specify timeout_ms. It matches the R5 default so an agent that awaits
// without thinking about a bound still fails fast rather than parking the turn
// for minutes.
const defaultAwaitTimeout = channelToolDefaultTimeout

// AwaitResultTool blocks until the named in-flight request reaches its final
// response, then returns the final payload (same shape as a fast-path inline
// final). On timeout it returns a still-pending ack so the agent can move on.
type AwaitResultTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*AwaitResultTool)(nil)

func (t *AwaitResultTool) Name() string { return "await_result" }

func (t *AwaitResultTool) Description() string {
	return strings.TrimSpace(`
Block until a previously-submitted call_actor request reaches its final result, then
return that result. Use this to collect a long call that returned an ack
(status:"accepted") instead of an inline result.

  - request_id: the id from the ack's request_id / to_wait.params.request_id.
  - timeout_ms: optional bound on how long to wait. On timeout you get a
    still-pending ack back (the call keeps running; try again later or let the
    result return as a new message).

If the request is unknown (already collected, abandoned, or lost to a worker
restart), the result is an error explaining so.
`)
}

var awaitResultSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."},
    "timeout_ms": {"type": "integer", "description": "Optional max wait in milliseconds. Defaults to the R5 default. On timeout you get a still-pending ack."}
  },
  "required": ["request_id"]
}`)

func (t *AwaitResultTool) ParameterSchema() json.RawMessage { return cloneRawJSON(awaitResultSchema) }

type awaitResultParams struct {
	RequestID string `json:"request_id"`
	TimeoutMs int64  `json:"timeout_ms,omitempty"`
}

func (t *AwaitResultTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("await_result", actorCLIInternalError, "await_result tool not configured", "Retry after the bridge is configured", nil), nil
	}
	var p awaitResultParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return payloadInvalidError("await_result", "", "", fmt.Sprintf("invalid params: %v", err)), nil
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return payloadInvalidError("await_result", "", "", "request_id is required (from a prior call_actor ack)"), nil
	}

	caller := t.bridge.caller()
	if !caller.futures.Registered(reqID) {
		return actorCLIErrorResult(
			"await_result",
			actorCLIInternalError,
			fmt.Sprintf("request_id %q is not in flight (already collected, abandoned, or lost to a worker restart)", reqID),
			"Call list_pending() to see in-flight request ids; resubmit the call if needed",
			nil,
		), nil
	}

	timeout := defaultAwaitTimeout
	if p.TimeoutMs > 0 {
		timeout = time.Duration(p.TimeoutMs) * time.Millisecond
	}

	finalEnv, ok, err := caller.Await(ctx, reqID, timeout)
	if err != nil {
		caller.Abandon(reqID)
		return actorCLIErrorResult("await_result", actorCLIInternalError,
			fmt.Sprintf("await_result %q failed: %v", reqID, err),
			"Inspect adapter logs; the request may have been abandoned", nil), nil
	}
	if !ok {
		// Still pending after the window — hand control back with an ack.
		return ackResultToToolResult(ackToolResult("await_result", ackDescriptor{
			requestID: reqID,
			accepted:  true,
			status:    "accepted",
			estWaitMs: int64(timeout / time.Millisecond),
			guidance: "Still running after the wait window. The call keeps running; try await_result again, " +
				"or do other work and react to the result when it returns as a new message.",
			toWait:    toWaitHint{tool: "await_result", params: map[string]any{"request_id": reqID.String()}},
			notWaitng: "result returns as kind=response, parent_id=" + reqID.String() + " new turn trigger",
		})), nil
	}
	return channelToolResultFromResponse("await_result", *finalEnv), nil
}

// AbandonTool drops the worker-side waiter for a request without touching the
// substrate. A final that arrives later loops back through routeTriggers as
// NoActiveWaiter → a new turn trigger.
type AbandonTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*AbandonTool)(nil)

func (t *AbandonTool) Name() string { return "abandon" }

func (t *AbandonTool) Description() string {
	return strings.TrimSpace(`
Stop waiting locally on a previously-submitted call_actor request. Use this in a
fan-out when one sibling already gave you what you need, or when you no longer care
about a result.

This does NOT cancel the downstream work (there is no protocol-level cancel) — the
call keeps running in the daemon. If it later produces a result, that result will
still come back to you as a new message (parent_id = request_id). Abandon only
releases your local wait so list_pending() no longer shows it and await_result()
will report it as not in flight.
`)
}

var abandonSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."}
  },
  "required": ["request_id"]
}`)

func (t *AbandonTool) ParameterSchema() json.RawMessage { return cloneRawJSON(abandonSchema) }

type abandonParams struct {
	RequestID string `json:"request_id"`
}

func (t *AbandonTool) Execute(_ context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("abandon", actorCLIInternalError, "abandon tool not configured", "Retry after the bridge is configured", nil), nil
	}
	var p abandonParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return payloadInvalidError("abandon", "", "", fmt.Sprintf("invalid params: %v", err)), nil
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return payloadInvalidError("abandon", "", "", "request_id is required"), nil
	}
	t.bridge.caller().Abandon(reqID)
	return types.ToolResult{
		Name: "abandon",
		Value: types.ToolReturnValue{Value: map[string]any{
			"abandoned":  reqID.String(),
			"note":       "local wait released; downstream work is not cancelled (no protocol-level cancel). A later result returns as a new message.",
			"request_id": reqID.String(),
		}},
	}, nil
}

// ListPendingTool returns the in-flight request ids only (no status
// aggregation, §2.3.4). The view is ephemeral — a worker restart yields an
// empty list even though the daemon-side pending + F3 are intact (M4).
type ListPendingTool struct {
	bridge *Bridge
}

var _ gokimitools.Tool = (*ListPendingTool)(nil)

func (t *ListPendingTool) Name() string { return "list_pending" }

func (t *ListPendingTool) Description() string {
	return strings.TrimSpace(`
List the request_ids of call_actor requests you have submitted that have not yet
reached their final result. Returns only the id list — no status — as a decision aid
for which await_result / abandon to issue next.

Note: this view is per-process. If the worker restarted, the list is empty even
though earlier calls may still be running in the daemon; their results will return
as new messages.
`)
}

func (t *ListPendingTool) ParameterSchema() json.RawMessage {
	return cloneRawJSON(json.RawMessage(`{"type":"object","properties":{}}`))
}

func (t *ListPendingTool) Execute(_ context.Context, _ json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil {
		return actorCLIErrorResult("list_pending", actorCLIInternalError, "list_pending tool not configured", "Retry after the bridge is configured", nil), nil
	}
	ids := t.bridge.caller().Pending()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return types.ToolResult{
		Name: "list_pending",
		Value: types.ToolReturnValue{Value: map[string]any{
			"pending": out,
			"count":   len(out),
		}},
	}, nil
}
