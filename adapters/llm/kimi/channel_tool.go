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

const channelToolDefaultTimeout = 5 * time.Minute

var genericObjectSchema = json.RawMessage(`{"type":"object"}`)

type channelToolRuntimeKey struct{}

type channelToolRuntime struct {
	ipc     IPCFacade
	trigger TriggerPayload
}

type toolResponse struct {
	trigger TriggerPayload
}

// ChannelTypeTool exposes one channel-local type_registry request type as
// a go-kimi function-calling tool.
type ChannelTypeTool struct {
	typeName       string
	handlerActorID string
	description    string
	payloadSchema  json.RawMessage
	timeout        time.Duration
	bridge         *Bridge
}

var _ gokimitools.Tool = (*ChannelTypeTool)(nil)

// Name returns the LLM-tool-facing identifier — a sanitized form of the
// channel type name. Anthropic / OpenAI function-calling APIs require
// tool names matching `^[a-zA-Z0-9_-]+$`, which rejects the canonical
// envelope.type form (e.g. "xhs.publish" — the dot is illegal). We
// substitute `.` → `_` (e.g. "xhs.publish" → "xhs_publish") and keep
// the original typeName for the actual envelope.type when Execute
// emits the envelope downstream.
func (t *ChannelTypeTool) Name() string { return sanitizeToolName(t.typeName) }

// CanonicalType returns the original envelope.type for this tool. The
// LLM never sees this form (Name() exposes the sanitized variant), but
// the bridge uses it when constructing the outbound envelope.
func (t *ChannelTypeTool) CanonicalType() string { return strings.TrimSpace(t.typeName) }

// sanitizeToolName converts a channel type name into a form acceptable
// to the Anthropic / OpenAI function-calling tool-name regex. Idempotent
// for already-sanitized inputs.
func sanitizeToolName(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	if typeName == "" {
		return ""
	}
	// Replace `.` (the only character canonical envelope.type uses that
	// fails the regex) with `_`. Any other illegal character introduced
	// by future type names should be added here as a single source of
	// truth — sanitizeToolName is the only producer of LLM-facing
	// tool ids.
	return strings.ReplaceAll(typeName, ".", "_")
}

func (t *ChannelTypeTool) Description() string {
	if t == nil {
		return ""
	}
	if desc := strings.TrimSpace(t.description); desc != "" {
		return desc
	}
	return fmt.Sprintf("Channel request tool %s. Emits a kind=request envelope to %s and waits for a kind=response.", t.Name(), t.handlerActorID)
}

func (t *ChannelTypeTool) ParameterSchema() json.RawMessage {
	if t == nil {
		return cloneRawJSON(genericObjectSchema)
	}
	return validRawJSONOrDefault(t.payloadSchema, genericObjectSchema)
}

func (t *ChannelTypeTool) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	if t == nil || t.bridge == nil || t.Name() == "" {
		return channelToolErrorResult("", "channel tool is not configured"), nil
	}
	payload, err := normalizeToolPayload(params)
	if err != nil {
		return channelToolErrorResult(t.Name(), err.Error()), nil
	}
	runtime, ok := ctx.Value(channelToolRuntimeKey{}).(channelToolRuntime)
	if !ok || runtime.ipc == nil {
		return channelToolErrorResult(t.Name(), "channel tool called outside a bridge turn"), nil
	}
	return t.bridge.executeChannelTool(ctx, runtime.ipc, runtime.trigger, t, payload), nil
}

// channelTools returns the meta-tool surface the LLM sees: a fixed
// pair (call_actor + list_actors) that exploits the envelope
// protocol's uniformity instead of fanning out one tool per
// channel-local type.
//
// Substrate rationale (vision §1.1 + proto-foundation §2.5):
//   - Every adapter exposes the same wire shape (kind=request envelope
//     + handler actor_id + payload). Direct per-type tool injection
//     was a transitional shim borrowed from no-standardization worlds
//     (raw MCP / OpenAI function-calling) where each tool had a
//     distinct RPC. Once standardization is in, a single invocation
//     primitive carries them all.
//   - Live discovery via list_actors keeps the LLM surface O(1) in
//     the channel's type count + handles dynamic type_registry
//     mutations without a worker respawn.
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
	}
}

func (b *Bridge) routeTriggers(ctx context.Context, in <-chan TriggerPayload) <-chan TriggerPayload {
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

// executeChannelTool is retained as a thin wrapper around
// executeChannelRequest so the legacy ChannelTypeTool path (still used
// by tests + any external embedder that constructs a *ChannelTypeTool
// directly) keeps working with the new generic implementation.
func (b *Bridge) executeChannelTool(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	tool *ChannelTypeTool,
	payload json.RawMessage,
) types.ToolResult {
	timeout := tool.timeout
	if timeout <= 0 {
		timeout = channelToolDefaultTimeout
	}
	return b.executeChannelRequest(ctx, ipc, trigger, channelRequestSpec{
		ToolName:       tool.Name(),
		EnvelopeType:   tool.CanonicalType(),
		HandlerActorID: tool.handlerActorID,
		Payload:        payload,
		Timeout:        timeout,
	})
}

// executeChannelRequest is the generic envelope-dispatch path the
// meta tools (call_actor) and the legacy per-type ChannelTypeTool
// share. Builds the kind=request envelope, registers a pending wait
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

func (b *Bridge) dispatchToolResponse(trigger TriggerPayload) bool {
	if trigger.Envelope.Kind != message.KindResponse {
		return false
	}
	parentID := message.ID(strings.TrimSpace(trigger.Envelope.ParentID.String()))
	if parentID == "" {
		return false
	}
	b.pendingMu.Lock()
	ch, ok := b.pendingTools[parentID]
	if ok {
		delete(b.pendingTools, parentID)
	}
	b.pendingMu.Unlock()
	if !ok {
		return true
	}
	select {
	case ch <- toolResponse{trigger: trigger}:
	default:
	}
	close(ch)
	return true
}

func typeAllowsKind(typ TypeInfo, kind string) bool {
	for _, candidate := range typ.AllowedKinds {
		if strings.EqualFold(strings.TrimSpace(candidate), kind) {
			return true
		}
	}
	return false
}

// requestSchema returns the generic-object payload schema for an LLM
// tool descriptor. Per protocol Level A (proto-layer0 §1.4.1), the
// type_registry no longer carries payload schemas; payload consistency
// is a product-layer concern, so the LLM tool wrapper exposes a
// permissive object schema and leaves payload shape negotiation to the
// caller/handler.
func requestSchema(_ TypeInfo) json.RawMessage {
	return cloneRawJSON(genericObjectSchema)
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
