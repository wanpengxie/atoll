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

func (b *Bridge) channelTools() []gokimitools.Tool {
	if b == nil {
		return nil
	}
	out := make([]gokimitools.Tool, 0, len(b.cfg.ChannelContext.Types))
	seen := map[string]struct{}{}
	for _, typ := range b.cfg.ChannelContext.Types {
		typeName := strings.TrimSpace(typ.Type)
		handler := strings.TrimSpace(typ.HandlerActorID)
		if typeName == "" || handler == "" || !typeAllowsKind(typ, string(message.KindRequest)) {
			continue
		}
		if _, dup := seen[typeName]; dup {
			continue
		}
		seen[typeName] = struct{}{}
		out = append(out, &ChannelTypeTool{
			typeName:       typeName,
			handlerActorID: handler,
			description:    typ.Description,
			payloadSchema:  requestSchema(typ),
			timeout:        channelToolTimeout(typ.MaxPendingMs),
			bridge:         b,
		})
	}
	return out
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
	now := b.cfg.NowFn()
	expiresAt := now + int64(timeout/time.Millisecond)
	env := message.Envelope{
		ID:        b.envelopeID(ipc, now),
		ChannelID: ipc.ChannelID(),
		// envelope.type uses the canonical channel type (e.g.
		// "xhs.publish") — NOT the LLM-tool-name form returned by
		// tool.Name(), which is sanitized for the Anthropic / OpenAI
		// tool-name regex.
		Type:          tool.CanonicalType(),
		Kind:          message.KindRequest,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{actor.ActorID(tool.handlerActorID)},
		Payload:       payload,
		ParentID:      trigger.Envelope.ID,
		CorrelationID: channelToolCorrelationID(trigger),
		ExpiresAt:     &expiresAt,
		TS:            now,
		TSReceived:    now,
	}

	responseCh := b.registerPendingTool(env.ID)
	defer b.unregisterPendingTool(env.ID)
	if err := ipc.WriteEnvelope(ctx, env); err != nil {
		return channelToolErrorResult(tool.Name(), fmt.Sprintf("emit channel request %s: %v", tool.Name(), err))
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case response, ok := <-responseCh:
		if !ok {
			return channelToolErrorResult(tool.Name(), "channel tool response wait closed")
		}
		return channelToolResultFromResponse(tool.Name(), response.trigger.Envelope)
	case <-timer.C:
		return channelToolErrorResult(tool.Name(), fmt.Sprintf("channel tool %s timed out after %s", tool.Name(), timeout))
	case <-ctx.Done():
		return channelToolErrorResult(tool.Name(), fmt.Sprintf("channel tool %s canceled: %v", tool.Name(), ctx.Err()))
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
