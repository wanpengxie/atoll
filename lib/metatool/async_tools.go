package metatool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// AwaitResultSpec is the protocol-layer definition of await_result.
var AwaitResultSpec = ToolSpec{
	Name: "await_result",
	Description: strings.TrimSpace(`
Block until a previously-submitted call_actor request reaches its final result, then
return that result. Use this to collect a long call that returned an ack
(status:"accepted") instead of an inline result.

  - request_id: the id from the ack's request_id / to_wait.params.request_id.
  - timeout_ms: optional bound on how long to wait, capped at 115000 ms. A
    larger value is clipped and reported in the still-pending ack, not rejected.

A worker restart preserves the cell's JobTable, so an in-flight request remains
awaitable. A cell restart loses that table while downstream work may continue;
an unknown id therefore returns result_unknown. First inspect target facts or use
an application idempotency key; never assume it is safe to submit the action again.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."},
	"timeout_ms": {"type": "integer", "minimum": 1, "description": "Optional requested wait in milliseconds. Values above 115000 are accepted but clipped and reported in the still-pending ack."}
  },
  "required": ["request_id"]
}`),
}

type awaitResultParams struct {
	RequestID string `json:"request_id"`
	TimeoutMs int64  `json:"timeout_ms,omitempty"`
}

// ExecuteAwaitResult is the protocol-layer execute function for await_result.
func ExecuteAwaitResult(ctx context.Context, params json.RawMessage, x *Exec, _ RuntimeContext) ResultValue {
	if x == nil || x.Jobs == nil {
		return NewError("await_result", InternalError, "await_result tool not configured", "Retry after the bridge is configured", nil)
	}
	var p awaitResultParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return PayloadInvalidError("await_result", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return PayloadInvalidError("await_result", "request_id is required (from a prior call_actor ack)", "")
	}

	timeout := DefaultTimeout
	requestedTimeoutMs := p.TimeoutMs
	clipped := false
	if p.TimeoutMs > 0 {
		if p.TimeoutMs > int64(MaxSynchronousWait/time.Millisecond) {
			timeout = MaxSynchronousWait
			clipped = true
		} else {
			timeout = time.Duration(p.TimeoutMs) * time.Millisecond
		}
	}

	finalEnv, ok, observed, err := awaitWithProgress(ctx, x.Jobs, reqID, timeout)
	if err != nil {
		if errors.Is(err, actorbase.ErrCallClosed) {
			// The ledger row is gone (already collected, cancelled, or lost to a
			// cell restart) — the JobTable equivalent of the old InFlight guard.
			return attachProgress(NewError(
				"await_result",
				ResultUnknown,
				fmt.Sprintf("request_id %q has no row in this cell's JobTable; it was collected or cancelled, or the cell restarted while downstream work may still be running", reqID),
				"Check list_pending and inspect the target's current facts or query by an application idempotency key; do not submit the action again merely because its result is unknown",
				nil,
			), observed)
		}
		// A ctx / wait error releases YOUR wait; it does NOT drop the ledger
		// entry (author#2 still owns the request's terminal) — the request keeps
		// running and stays awaitable. No implicit cancel (an await error is not
		// the caller deciding to abandon the work).
		return attachProgress(NewError("await_result", InternalError,
			fmt.Sprintf("await_result %q failed: %v", reqID, err),
			"Inspect adapter logs; the wait was released but the call keeps running", nil), observed)
	}
	if !ok {
		// Still pending after the window — hand control back with an ack.
		toWait, notWaiting := newCollectHint(reqID.String())
		ack := AckResult("await_result", AckDescriptor{
			RequestID:  reqID,
			Accepted:   true,
			Status:     "accepted",
			EstWaitMs:  int64(timeout / time.Millisecond),
			Guidance:   "Still running after the wait window. The call keeps running; claim it later with await_result.",
			ToWait:     toWait,
			NotWaiting: notWaiting,
		})
		if clipped {
			ack.Value["wait_truncated"] = true
			ack.Value["requested_timeout_ms"] = requestedTimeoutMs
			ack.Value["effective_timeout_ms"] = int64(timeout / time.Millisecond)
			ack.Value["guidance"] = "Still running after the provider-safe 115s wait cap. The request remains in flight; await it again later."
		}
		return attachProgress(ack, observed)
	}
	rv, _ := ResultFromResponse("await_result", *finalEnv)
	// Stage two of the render→normalize pipeline (the same law every other
	// collector applies — call_actor:114, describe.go:61/122): await_result is
	// call_actor's second half, and the SAME actor-returned failure must reach
	// the LLM in the SAME closed-set error shape whichever half collected it.
	// This mount was the pipeline's one missing outlet (purity 手动档 B3).
	// The answering actor's identity and the response type ride on the final
	// envelope itself.
	return attachProgress(NormalizeCallActorResult(rv, string(finalEnv.Sender.ID), finalEnv.Type), observed)
}

// CancelSpec is the protocol-layer definition of cancel.
var CancelSpec = ToolSpec{
	Name: "cancel",
	Description: strings.TrimSpace(`
Cancel a previously-submitted call_actor request. Use this in a fan-out when one
sibling already gave you what you need, or when you no longer want the work done.

Closes the request NOW with a cancel terminal (a legal self-close): list_pending()
no longer shows it, await_result() reports it as not in flight, and the receiver is
signalled to stop the work (if the assembly root wired the protocol-level cancel).
The request is DONE — a late answer the receiver may still produce is rejected as a
duplicate terminal and never lands. This is the definitive stop, not merely
"stop waiting": use it only when you actually want the request cancelled.
`),
	Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "request_id": {"type": "string", "description": "The request_id returned in a prior call_actor ack."}
  },
  "required": ["request_id"]
}`),
}

type cancelParams struct {
	RequestID string `json:"request_id"`
}

// ExecuteCancel is the protocol-layer execute function for cancel.
func ExecuteCancel(_ context.Context, params json.RawMessage, x *Exec, _ RuntimeContext) ResultValue {
	if x == nil || x.Jobs == nil {
		return NewError("cancel", InternalError, "cancel tool not configured", "Retry after the bridge is configured", nil)
	}
	var p cancelParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return PayloadInvalidError("cancel", fmt.Sprintf("invalid params: %v", err), "")
		}
	}
	reqID := message.ID(strings.TrimSpace(p.RequestID))
	if reqID == "" {
		return PayloadInvalidError("cancel", "request_id is required", "")
	}
	_ = x.Jobs.Cancel(reqID)
	return ResultValue{
		Name: "cancel",
		Value: map[string]any{
			"cancelled":  reqID.String(),
			"note":       "request closed with a cancel terminal; the receiver was signalled to stop (if wired). A late answer is rejected as a duplicate and never lands.",
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
for which await_result / cancel to issue next.

Note: this view is per-process. If the cell restarted, the list is empty even
though earlier calls may still be running in the daemon; this incarnation can no
longer claim those rows.
`),
	Schema: json.RawMessage(`{"type":"object","properties":{}}`),
}

// ExecuteListPending is the protocol-layer execute function for list_pending.
func ExecuteListPending(_ context.Context, _ json.RawMessage, x *Exec, _ RuntimeContext) ResultValue {
	if x == nil || x.Jobs == nil {
		return NewError("list_pending", InternalError, "list_pending tool not configured", "Retry after the bridge is configured", nil)
	}
	ids := x.Jobs.List()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return ResultValue{
		Name: "list_pending",
		Value: map[string]any{
			"pending": out,
			"count":   len(out),
		},
	}
}
