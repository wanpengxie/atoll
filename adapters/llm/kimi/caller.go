package kimi

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// bridgeCaller is the worker-side caller helper that backs the kimi
// fast-path. It owns its OWN futurereg.FutureRegistry instance — the kimi
// agent runs in an independent worker process, so it cannot use the
// in-daemon Manager-level router (that registry lives inside the daemon
// process). The transport binding here is IPC: requests go out through
// ipc.WriteEnvelope, and the daemon → worker Triggers() stream is fed back
// into the registry via Deliver.
//
// Why a separate instance rather than the shared in-daemon router: §1 of
// framework-sync-async-construction-spec.md — three transports, one shared
// futurereg core. The matching / buffering / disposition semantics never
// drift because all three bind to the SAME futurereg package; only "how a
// response envelope is received" differs per transport.
//
// Lifecycle: one bridgeCaller per Bridge (created lazily under Bridge.mu).
// The registry is pure in-memory; on worker restart it is empty, which is
// exactly the M4 semantics (§5.2): a final that arrives with no local waiter
// is surfaced as a new turn trigger rather than quarantined or dropped.
type bridgeCaller struct {
	futures *futurereg.FutureRegistry
}

func newBridgeCaller() *bridgeCaller {
	return &bridgeCaller{futures: futurereg.New()}
}

// submitResult is the worker-side analogue of adapter.SubmitResult: the
// request id plus the substrate-level ack descriptor synthesised at write
// accept time. The ack guidance is a framework template (no receiver
// semantics) — receiver-authored guidance arrives later as a provisional.
type submitResult struct {
	requestID message.ID
	ack       ackDescriptor
}

// ackDescriptor mirrors adapter.AckDescriptor's dual form (§2.3.3) in the
// worker-side shape the meta tools render into a tool result.
type ackDescriptor struct {
	requestID message.ID
	accepted  bool
	status    string // substrate-level, always "accepted" on the immediate ack
	estWaitMs int64  // source: type.max_pending_ms (R5)
	guidance  string // framework template
	toWait    toWaitHint
	notWaitng string
}

type toWaitHint struct {
	tool   string
	params map[string]any
}

// Submit registers the future BEFORE writing the envelope
// (subscribe-before-send, §3.2): in the worker process the register-then-
// write ordering inside one goroutine guarantees the waiterSet exists by the
// time the response loops back through Triggers(). Returns the request id +
// ack once the write is accepted by the daemon harness (via IPC).
func (c *bridgeCaller) Submit(
	ctx context.Context,
	ipc IPCFacade,
	env message.Envelope,
	estWaitMs int64,
	expectsAwait bool,
) (submitResult, error) {
	// subscribe-before-send: register first.
	c.futures.Register(env.ID, futurereg.RegisterOpts{ExpectsAwait: expectsAwait})
	if err := ipc.WriteEnvelope(ctx, env); err != nil {
		c.futures.Cancel(env.ID)
		return submitResult{}, err
	}
	return submitResult{
		requestID: env.ID,
		ack: ackDescriptor{
			requestID: env.ID,
			accepted:  true,
			status:    "accepted",
			estWaitMs: estWaitMs,
			guidance: "Accepted. To wait, call await_result(request_id=" + env.ID.String() +
				"). If you do not wait, the result returns as a new message (parent_id=" + env.ID.String() + ").",
			toWait: toWaitHint{
				tool:   "await_result",
				params: map[string]any{"request_id": env.ID.String()},
			},
			notWaitng: "result returns as kind=response, parent_id=" + env.ID.String() + " new turn trigger",
		},
	}, nil
}

// Await blocks until the final for id arrives, the window elapses, or ctx is
// done. window <= 0 means "do not wait at all" (used by wait=false fan-out:
// the registry stays registered so a later final routes through Deliver as
// NoActiveWaiter → new turn trigger). A timeout/cancel leaves the waiterSet
// registered (futurereg detaches the parked channel) so the eventual final is
// not lost.
//
// ok=false with err==nil means the window expired without a final (fast-path
// super-window → ack); err!=nil is a hard wait error (ctx cancel / closed).
func (c *bridgeCaller) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if window <= 0 {
		// Do not park at all — leave the future registered for Deliver.
		return nil, false, nil
	}
	handle := c.futures.Register(id) // idempotent: rebinds the existing set
	env, err := handle.Await(ctx, window)
	if err != nil {
		// Window expiry surfaces as DeadlineExceeded — treat as "no final yet"
		// (fast-path degrade to ack), NOT a hard error. Hard errors (ctx
		// cancel, closed/abandoned) propagate.
		if err == context.DeadlineExceeded {
			return nil, false, nil
		}
		return nil, false, err
	}
	return env, true, nil
}

// Watch returns a provisional+final stream over the future for id (§2.2 Watch).
// It is the worker-side projection of the shared futurereg Watch affordance —
// the same mechanism the in-daemon router exposes — so the worker caller stays
// feature-equivalent with the other transports. The future must already be
// registered (via Submit); Watch on an unregistered/closed id returns an error.
func (c *bridgeCaller) Watch(id message.ID) (futurereg.Watcher, error) {
	return c.futures.Register(id).Watch()
}

// Abandon drops the local waiter for id (fan-out early failure / agent gives
// up). It does NOT touch the substrate — the daemon-side pending + F3 remain,
// so a later final still loops back and routes through Deliver as
// NoActiveWaiter → new turn trigger.
func (c *bridgeCaller) Abandon(id message.ID) {
	c.futures.Cancel(id)
}

// Pending returns the in-flight request ids (the ephemeral list_pending view,
// §2.3.4). No status aggregation — only the id list. The view is ephemeral:
// a worker restart yields an empty registry (M4 limitation, documented).
func (c *bridgeCaller) Pending() []message.ID {
	return c.futures.Pending()
}

// Deliver feeds one inbound response envelope into the registry and returns
// the single-lock-atomic disposition (M2). The caller (routeTriggers) acts on
// the returned value:
//
//   - DeliveredToAwait / DeliveredToWatch → consumed by an active waiter;
//     the response MUST NOT also surface as a new turn trigger.
//   - NoActiveWaiter (await already timed out / abandoned / worker restarted
//     with no waiter, M4) → the caller surfaces it as a new turn trigger
//     (never quarantined, never dropped).
func (c *bridgeCaller) Deliver(env *message.Envelope) futurereg.Disposition {
	return c.futures.Deliver(env)
}

// fastPathWindow is the default bounded-wait window for call_actor (§2.3.2,
// decision ⑤=(c)). Within the window a final returns inline (short calls feel
// synchronous, no ack→await double hop); past the window the call degrades to
// an ack descriptor (long calls go async, the agent decides whether to
// await_result / abandon / let the result return as a new trigger).
//
// Layering invariant (§2.3.2): window (15s) < type timeout (30s default /
// per-type max_pending_ms) < F3 substrate fallback (5min). The window is a
// pure caller-side mechanism; the receiver is unaware whether the caller is
// doing a bounded or unbounded wait. The window is also clamped to the type
// timeout so a tiny max_pending_ms type cannot be waited on past its own
// deadline.
const fastPathWindow = 15 * time.Second

// resolveFastPathWindow computes the Await window for a call given the wait
// mode and the type-level timeout (max_pending_ms, R5):
//
//   - waitUnbounded (call_actor wait=true): the full type timeout — sync.
//   - default fast-path: min(fastPathWindow, typeTimeout) — bounded, then ack.
//
// wait=false (window 0) is handled by the caller before reaching here.
func resolveFastPathWindow(typeTimeout time.Duration, waitUnbounded bool) time.Duration {
	if typeTimeout <= 0 {
		typeTimeout = channelToolDefaultTimeout
	}
	if waitUnbounded {
		return typeTimeout
	}
	if fastPathWindow < typeTimeout {
		return fastPathWindow
	}
	return typeTimeout
}

// ackToolResult renders an ack descriptor as a kimi tool result. It is NOT an
// error result — a super-window ack is a normal "still running, here's how to
// get the result" outcome. The shape is structured so the agent's JIT decision
// program can branch on it, and carries the NL guidance template.
func ackToolResult(toolName string, ack ackDescriptor) toolResultValue {
	return toolResultValue{
		name: toolName,
		value: map[string]any{
			"status":         ack.status, // "accepted" (substrate-level, not business)
			"request_id":     ack.requestID.String(),
			"accepted":       ack.accepted,
			"est_wait_ms":    ack.estWaitMs,
			"guidance":       ack.guidance,
			"to_wait":        map[string]any{"tool": ack.toWait.tool, "params": ack.toWait.params},
			"if_not_waiting": ack.notWaitng,
		},
	}
}

// toolResultValue is a tiny carrier so caller.go does not import the go-kimi
// types package directly (kept assembly-only in channel_tool.go /
// meta_tool.go where the result is materialised into types.ToolResult).
type toolResultValue struct {
	name  string
	value map[string]any
}

// parseResponseStatus is a defensive `payload.status` extractor. An empty
// string return means "status absent / payload unparseable" — callers treat
// that as a non-final response so a malformed status field cannot accidentally
// resolve a future. Final-status enforcement lives upstream in the daemon
// harness (runtime/harness step_response_pairing); by the time an envelope
// reaches the kimi bridge the harness has already validated payload.status is
// in the {Layer 1 final, Layer 2 core, Layer 3 `<ns>.<name>`} closed sets.
func parseResponseStatus(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	return strings.TrimSpace(obj.Status)
}

// parseFinalStatus extracts payload.status and reports whether it is a Layer 1
// final (completed/failed) per the kernel single source of truth.
func parseFinalStatus(raw []byte) (string, bool) {
	status := parseResponseStatus(raw)
	return status, message.IsFinalStatus(strings.TrimSpace(status))
}
