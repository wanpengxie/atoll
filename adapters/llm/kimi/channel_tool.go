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
	"github.com/wanpengxie/ActOS/kernel/message"
)

// channelToolDefaultTimeout is the fallback Execute timeout when a
// type's MaxPendingMs is absent. R5 invariant (actor-adapter.md §7.2):
// "sane default timeout — SDK 30s / max_pending_ms 30s". Long-running
// adapter types MUST declare an explicit MaxPendingMs override; the
// default is deliberately tight so a misconfigured type fails fast
// instead of blocking the agent loop for minutes.
const channelToolDefaultTimeout = 30 * time.Second

var genericObjectSchema = json.RawMessage(`{"type":"object"}`)

type channelToolRuntimeKey struct{}

type channelToolRuntime struct {
	ipc     IPCFacade
	trigger TriggerPayload
}

type toolResponse struct {
	trigger TriggerPayload
}

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
	}
}

func (b *Bridge) routeTriggers(ctx context.Context, ipc IPCFacade, in <-chan TriggerPayload) <-chan TriggerPayload {
	out := make(chan TriggerPayload, 32)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-in:
				if !ok {
					return
				}
				if b.dispatchToolResponse(payload) {
					if err := ackTrigger(ctx, ipc, payload, true, ""); err != nil {
						return
					}
					continue
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

// executeChannelRequest is the generic envelope-dispatch path the
// meta tools (call_actor) use. Builds the kind=request envelope,
// registers a pending wait
// keyed on envelope.id, ships through ipc.WriteEnvelope, and blocks
// until the matching response arrives (or timeout / ctx cancel).
//
// All error / success result shapes go through channelToolResultFromResponse
// and channelToolErrorResult — same wire as before so existing tests
// + observability stay valid.
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

	responseCh := b.registerPendingTool(env.ID)
	defer b.unregisterPendingTool(env.ID)
	if err := ipc.WriteEnvelope(ctx, env); err != nil {
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("emit channel request %s: %v", spec.EnvelopeType, err))
	}

	timer := time.NewTimer(spec.Timeout)
	defer timer.Stop()
	select {
	case response, ok := <-responseCh:
		if !ok {
			return channelToolErrorResult(spec.ToolName, "channel request response wait closed")
		}
		return channelToolResultFromResponse(spec.ToolName, response.trigger.Envelope)
	case <-timer.C:
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("channel request %s timed out after %s", spec.EnvelopeType, spec.Timeout))
	case <-ctx.Done():
		return channelToolErrorResult(spec.ToolName,
			fmt.Sprintf("channel request %s canceled: %v", spec.EnvelopeType, ctx.Err()))
	}
}

func (b *Bridge) registerPendingTool(id message.ID) chan toolResponse {
	ch := make(chan toolResponse, 1)
	b.pendingMu.Lock()
	if b.pendingTools == nil {
		b.pendingTools = map[message.ID]chan toolResponse{}
	}
	b.pendingTools[id] = ch
	b.pendingMu.Unlock()
	return ch
}

func (b *Bridge) unregisterPendingTool(id message.ID) {
	b.pendingMu.Lock()
	if b.pendingTools != nil {
		delete(b.pendingTools, id)
	}
	b.pendingMu.Unlock()
}

// dispatchToolResponse intercepts kind=response envelopes whose
// parent_id matches an in-flight executeChannelRequest pending entry.
//
// Returns true when the envelope belongs to the bridge's tool-response
// machinery (the caller acks the trigger and stops forwarding it to the
// LLM as a fresh turn input). Returns false to let routeTriggers forward
// the envelope to the LLM as a new turn.
//
// Provisional vs final dispatch (response-multitype-refactor.md §3.4 D):
//   - **Final** (payload.status ∈ {completed, failed} — Layer 1 closed
//     set per proto-layer0 §2.5.1): close the pending tool slot so the
//     blocked executeChannelRequest call returns the result to the LLM.
//   - **Provisional** (Layer 2 core: received / queued / processing /
//     deferred / unavailable; Layer 3 namespace: `<adapter>.<name>`):
//     swallow the trigger (treat as bridge-internal progress) but leave
//     pendingTools[parent] alive — the LLM stays parked on the final.
//
// v1 trade-off (deliberate, per spec §3.4 D notes): provisional payloads
// are NOT surfaced back to the LLM as progress events yet. The substrate
// for a progress channel + system-prompt nudge is future work
// (response-multitype-refactor.md §3.4 D: "v1 阶段最简：忽略 provisional,
// 仍等 final"). What matters here is that provisional traffic does NOT
// resolve the future and does NOT leak into the LLM trigger stream where
// it would surface as a spurious new turn.
func (b *Bridge) dispatchToolResponse(trigger TriggerPayload) bool {
	if trigger.Envelope.Kind != message.KindResponse {
		return false
	}
	parentID := message.ID(strings.TrimSpace(trigger.Envelope.ParentID.String()))
	if parentID == "" {
		return false
	}
	status := parseResponseStatus(trigger.Envelope.Payload)
	final := message.IsFinalStatus(status)

	b.pendingMu.Lock()
	ch, ok := b.pendingTools[parentID]
	if ok && final {
		delete(b.pendingTools, parentID)
	}
	b.pendingMu.Unlock()
	if !ok {
		// No pending tool entry: late / duplicate / orphan response.
		// Quarantine identically for provisional + final — neither
		// belongs in the LLM trigger stream.
		return true
	}
	if !final {
		// Provisional: keep the slot alive so the final response (or
		// substrate F3 timeout) resolves executeChannelRequest. The
		// payload is intentionally not pushed to ch — see godoc.
		return true
	}
	select {
	case ch <- toolResponse{trigger: trigger}:
	default:
	}
	close(ch)
	return true
}

// parseResponseStatus is a defensive `payload.status` extractor used on
// the dispatchToolResponse hot path. An empty string return means
// "status absent / payload unparseable" — the caller treats that as a
// non-final response so a malformed status field cannot accidentally
// resolve the LLM's future. Final status enforcement lives upstream in
// the daemon harness (runtime/harness step_response_pairing) — by the
// time an envelope reaches the kimi bridge the harness has already
// validated `payload.status` is in the {Layer 1 final, Layer 2 core,
// Layer 3 `<ns>.<name>`} closed sets.
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

func validRawJSONOrDefault(raw json.RawMessage, fallback json.RawMessage) json.RawMessage {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" || !json.Valid([]byte(text)) {
		return cloneRawJSON(fallback)
	}
	return cloneRawJSON(json.RawMessage(text))
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
