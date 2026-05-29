package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gokimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// channelToolDefaultTimeout is the fallback Execute timeout when a
// type's MaxPendingMs is absent. R5 invariant (actor-adapter.md §7.2):
// "sane default timeout — SDK 30s / max_pending_ms 30s". Long-running
// adapter types MUST declare an explicit MaxPendingMs override; the
// default is deliberately tight so a misconfigured type fails fast
// instead of blocking the agent loop for minutes.
const channelToolDefaultTimeout = 30 * time.Second

type channelToolRuntimeKey struct{}

type channelToolRuntime struct {
	ipc     IPCFacade
	trigger TriggerPayload
}

// waitMode selects the caller-side wait policy for one channel request
// (§2.3.2). It is purely a caller-side mechanism; the receiver is unaware.
type waitMode int

const (
	// waitFastPath is the default: Submit + Await(window~15s). Within the
	// window a final returns inline; past it the call degrades to an ack.
	waitFastPath waitMode = iota
	// waitUnbounded is call_actor(wait=true): Await to the type timeout
	// (sync opt-in, no degrade to ack short of the type deadline).
	waitUnbounded
	// waitNone is call_actor(wait=false): window 0, immediate ack (fan-out).
	waitNone
)

// channelTools returns the meta-tool surface the LLM sees: a fixed
// actor-CLI verb set that exploits the envelope
// protocol's uniformity instead of fanning out one tool per
// channel-local type.
//
// Substrate rationale (vision §1.1 + proto-foundation §2.5):
//   - Every adapter exposes the same wire shape (kind=request envelope
//   - handler actor_id + payload). Direct per-type tool injection
//     was a transitional shim borrowed from no-standardization worlds
//     (legacy tool APIs / OpenAI function-calling) where each tool had
//     a distinct RPC. Once standardization is in, a single invocation
//     primitive carries them all.
//   - Bootstrap discovery via list_actors keeps the LLM surface O(1)
//     in the channel's type count. Live validation still happens in
//     the daemon/harness/adapter path on each envelope call.
//
// Trade-off: LLMs trained on direct injection need a system-prompt
// nudge ("call list_actors first") to use this pattern reliably. See
// buildChannelContextSection in bridge.go for the prompt phrasing.
func (b *Bridge) channelTools() []gokimitools.Tool {
	if b == nil {
		return nil
	}
	return []gokimitools.Tool{
		&CallActorTool{bridge: b},
		&ListActorsTool{bridge: b},
		&DescribeActorTool{bridge: b},
		&DescribeTypeTool{bridge: b},
		&AwaitResultTool{bridge: b},
		&AbandonTool{bridge: b},
		&ListPendingTool{bridge: b},
	}
}

// routeTriggers fans the daemon → worker trigger stream through the
// worker-side caller helper. Every kind=response envelope is delivered to the
// caller's futurereg (M2 single-lock atomic disposition); the disposition plus
// the response's finality decide whether it was consumed, swallowed, or must
// surface as a new turn trigger.
//
// Decision table for a kind=response (parent_id set) — driven PURELY off the
// futurereg Disposition that Deliver returns (single-lock atomic, M2). It must
// NOT consult Registered(): a super-window fast-path Await leaves the
// waiterSet registered, so "Registered()==true ⇒ swallow" would silently eat
// the eventual final and the long-call result would never come back (F1).
//
//   - DeliveredToAwait → the final was handed to an active fast-path Await /
//     await_result. Ack the trigger and DO NOT forward to the LLM.
//   - DeliveredToWatch → consumed by a Watch stream (provisional or final).
//     Ack + swallow.
//   - NoActiveWaiter + provisional → an interim on an in-flight call with no
//     Watch attached. v1 swallows provisionals (response-multitype-refactor.md
//     §3.4 D: "ignore provisional, wait final"); the fast-path Await stays
//     parked. Ack + swallow.
//   - NoActiveWaiter + FINAL → a final with no active awaiter: the fast-path
//     Await already timed out (super-window degrade-to-ack), or the call was
//     abandoned, or a worker-restart orphan (M4). This is exactly the
//     "super-window final" case. Surface it to the LLM as a NEW TURN TRIGGER
//     (never quarantined, never dropped) so the long-call result comes back,
//     and clear the now-consumed future so a later await_result cannot
//     double-consume the buffered final.
//
// Non-response envelopes (events / fresh requests addressed to the agent)
// always forward to the LLM as turn triggers.
func (b *Bridge) routeTriggers(ctx context.Context, ipc IPCFacade, in <-chan TriggerPayload) <-chan TriggerPayload {
	out := make(chan TriggerPayload, 32)
	go func() {
		defer close(out)
		caller := b.caller()
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-in:
				if !ok {
					return
				}
				if payload.Envelope.Kind == message.KindResponse && payload.Envelope.ParentID != "" {
					env := payload.Envelope
					_, final := parseFinalStatus(env.Payload)
					disp := caller.Deliver(&env)
					surfaceAsTrigger := disp == futurereg.NoActiveWaiter && final
					if !surfaceAsTrigger {
						// Consumed by an active waiter / Watch, or a swallowed
						// provisional — ack + drop.
						if err := ackTrigger(ctx, ipc, payload, true, ""); err != nil {
							return
						}
						continue
					}
					// Super-window / M4: a FINAL with no active awaiter →
					// surface as a new turn trigger and clear the consumed
					// future (drop the buffered final so a later await_result
					// does not re-deliver it).
					caller.Abandon(env.ParentID)
				}
				select {
				case out <- payload:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// executeChannelRequest is the generic envelope-dispatch fast-path the meta
// tools (call_actor) and the reserved-type tools (describe_*) share. It builds
// the kind=request envelope, registers + writes via the worker-side caller
// helper (subscribe-before-send, §3.2), then awaits per the spec's wait mode
// (§2.3.2):
//
//   - waitFastPath (default): Await(window = min(15s, type timeout)). A final
//     within the window returns inline (short calls feel synchronous, no
//     ack→await double hop). Past the window the call degrades to an ack
//     descriptor (long calls go async; the agent decides await_result /
//     abandon / let it return as a new trigger).
//   - waitUnbounded (call_actor wait=true): Await(type timeout) — sync opt-in.
//   - waitNone (call_actor wait=false): window 0, immediate ack (fan-out).
//
// The request stays persistently pending in the daemon regardless of the
// caller window; a window expiry only hands control back to the agent — the
// substrate pending + F3 are untouched (§2.3.2).
func (b *Bridge) executeChannelRequest(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	spec channelRequestSpec,
) types.ToolResult {
	if spec.Timeout <= 0 {
		spec.Timeout = channelToolDefaultTimeout
	}
	now := b.cfg.NowFn()
	expiresAt := now + int64(spec.Timeout/time.Millisecond)
	env := message.Envelope{
		ID:            b.envelopeID(ipc, now),
		ChannelID:     ipc.ChannelID(),
		Type:          strings.TrimSpace(spec.EnvelopeType),
		Kind:          message.KindRequest,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{actor.ActorID(spec.HandlerActorID)},
		Payload:       spec.Payload,
		ParentID:      trigger.Envelope.ID,
		CorrelationID: channelToolCorrelationID(trigger),
		ExpiresAt:     &expiresAt,
		TS:            now,
		TSReceived:    now,
	}

	caller := b.caller()
	est := int64(spec.Timeout / time.Millisecond)
	res, err := caller.Submit(ctx, ipc, env, est)
	if err != nil {
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("emit channel request %s: %v", spec.EnvelopeType, err))
	}

	var window time.Duration
	switch spec.WaitMode {
	case waitNone:
		window = 0
	case waitUnbounded:
		window = resolveFastPathWindow(spec.Timeout, true)
	default: // waitFastPath
		window = resolveFastPathWindow(spec.Timeout, false)
	}

	if window <= 0 {
		// wait=false: immediate ack, future left in-flight for a later
		// await_result / new turn trigger.
		return ackResultToToolResult(ackToolResult(spec.ToolName, res.ack))
	}

	finalEnv, ok, awaitErr := caller.Await(ctx, res.requestID, window)
	if awaitErr != nil {
		// Hard wait failure (ctx cancel / closed). Abandon the local waiter;
		// substrate pending + F3 stay intact.
		caller.Abandon(res.requestID)
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("channel request %s wait failed: %v", spec.EnvelopeType, awaitErr))
	}
	if ok {
		return channelToolResultFromResponse(spec.ToolName, *finalEnv)
	}
	// Window elapsed without a final. Fast-path degrades to ack; the request
	// stays in-flight. waitUnbounded reaching here means the type timeout
	// itself elapsed — same degrade-to-ack (the F3 substrate fallback still
	// guarantees eventual closure).
	return ackResultToToolResult(ackToolResult(spec.ToolName, res.ack))
}

// ackResultToToolResult materialises a toolResultValue (built by the
// transport-neutral caller helper) into a go-kimi types.ToolResult. An ack is
// NOT an error result — it is a normal "still running" outcome.
func ackResultToToolResult(v toolResultValue) types.ToolResult {
	return types.ToolResult{
		Name:  v.name,
		Value: types.ToolReturnValue{Value: v.value},
	}
}

func typeAllowsKind(typ TypeInfo, kind string) bool {
	for _, candidate := range typ.AllowedKinds {
		if strings.EqualFold(strings.TrimSpace(candidate), kind) {
			return true
		}
	}
	return false
}

func channelToolTimeout(maxPendingMs int64) time.Duration {
	if maxPendingMs <= 0 {
		return channelToolDefaultTimeout
	}
	return time.Duration(maxPendingMs) * time.Millisecond
}

func channelToolCorrelationID(trigger TriggerPayload) message.ID {
	if trigger.CorrelationID != "" {
		return trigger.CorrelationID
	}
	if trigger.Envelope.CorrelationID != "" {
		return trigger.Envelope.CorrelationID
	}
	return trigger.Envelope.ID
}

func normalizeToolPayload(raw json.RawMessage) (json.RawMessage, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return cloneRawJSON(json.RawMessage(`{}`)), nil
	}
	if !json.Valid([]byte(text)) {
		return nil, fmt.Errorf("channel tool payload is not valid JSON: %q", text)
	}
	return cloneRawJSON(json.RawMessage(text)), nil
}

func cloneRawJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

func channelToolResultFromResponse(toolName string, env message.Envelope) types.ToolResult {
	if env.Kind != message.KindResponse {
		return channelToolErrorResult(toolName, fmt.Sprintf("channel tool got %s envelope %s", env.Kind, env.ID))
	}
	value := toolPayloadValue(env.Payload)
	if reason := responseFailureReason(env.Payload); reason != "" {
		return types.ToolResult{
			Name: toolName,
			Value: types.ToolReturnValue{Value: map[string]any{
				"error":   reason,
				"payload": value,
			}},
			IsError: true,
		}
	}
	return types.ToolResult{
		Name:  toolName,
		Value: types.ToolReturnValue{Value: value},
	}
}

func channelToolErrorResult(toolName, msg string) types.ToolResult {
	return types.ToolResult{
		Name:    toolName,
		Value:   types.ToolReturnValue{Value: strings.TrimSpace(msg)},
		IsError: true,
	}
}

func toolPayloadValue(raw json.RawMessage) any {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return map[string]any{}
	}
	var value any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return text
	}
	if value == nil {
		return map[string]any{}
	}
	return value
}

func responseFailureReason(raw json.RawMessage) string {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	if reason := stringValue(obj["terminal_failure_reason"]); reason != "" {
		return reason
	}
	if status := stringValue(obj["status"]); strings.EqualFold(status, "failed") {
		if reason := stringValue(obj["reason"]); reason != "" {
			return reason
		}
		return "failed"
	}
	return ""
}

func stringValue(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}
