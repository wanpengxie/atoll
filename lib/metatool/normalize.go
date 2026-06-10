package metatool

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// NormalizePayload ensures a raw JSON payload is a valid non-nil object.
func NormalizePayload(raw json.RawMessage) (json.RawMessage, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return CloneRawJSON(json.RawMessage(`{}`)), nil
	}
	if !json.Valid([]byte(text)) {
		return nil, fmt.Errorf("channel tool payload is not valid JSON: %q", text)
	}
	return CloneRawJSON(json.RawMessage(text)), nil
}

// CloneRawJSON returns a deep copy of raw.
func CloneRawJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

// ResponseFailureReason extracts the failure reason from a response
// payload, if any.
func ResponseFailureReason(raw json.RawMessage) string {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	if reason := StringValue(obj["terminal_failure_reason"]); reason != "" {
		return reason
	}
	if status := StringValue(obj["status"]); strings.EqualFold(status, "failed") {
		if reason := StringValue(obj["reason"]); reason != "" {
			return reason
		}
		return "failed"
	}
	return ""
}

// StringValue extracts a trimmed string from an arbitrary value.
func StringValue(v any) string {
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

// ResultFromResponse builds a ResultValue from a response envelope. If
// the response is a failure, isError is true.
func ResultFromResponse(toolName string, env message.Envelope) (ResultValue, bool) {
	if env.Kind != message.KindResponse {
		return ResultValue{
			Name:    toolName,
			Value:   map[string]any{"error": fmt.Sprintf("channel tool got %s envelope %s", env.Kind, env.ID)},
			IsError: true,
		}, true
	}
	value := payloadValue(env.Payload)
	if reason := ResponseFailureReason(env.Payload); reason != "" {
		return ResultValue{
			Name: toolName,
			Value: map[string]any{
				"error":   reason,
				"payload": value,
			},
			IsError: true,
		}, true
	}
	// Success: value may be a map or a scalar; wrap in map for consistency.
	if m, ok := value.(map[string]any); ok {
		return ResultValue{Name: toolName, Value: m}, false
	}
	return ResultValue{Name: toolName, Value: map[string]any{"result": value}}, false
}

// payloadValue decodes a raw JSON payload to a Go value.
func payloadValue(raw json.RawMessage) any {
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
