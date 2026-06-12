package metatool

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// ErrorCode is the actor-CLI closed error set.
type ErrorCode string

const (
	PayloadInvalid   ErrorCode = "payload_invalid"
	ActorUnreachable ErrorCode = "actor_unreachable"
	Timeout          ErrorCode = "timeout"
	InternalError    ErrorCode = "internal_error"
)

// NewError builds an error ResultValue in the actor-CLI shape.
func NewError(toolName string, code ErrorCode, msg, recoveryHint string, detail any) ResultValue {
	errObj := map[string]any{
		"code":    string(code),
		"message": strings.TrimSpace(msg),
	}
	if strings.TrimSpace(recoveryHint) != "" {
		errObj["recovery_hint"] = strings.TrimSpace(recoveryHint)
	}
	if detail != nil {
		errObj["detail"] = detail
	}
	return ResultValue{
		Name: toolName,
		Value: map[string]any{
			"ok":    false,
			"error": errObj,
		},
		IsError: true,
	}
}

// PayloadInvalidError builds a payload_invalid error ResultValue. The hint
// parameter is the recovery guidance for the caller — this layer does not
// hardcode tool names (those belong to the metatool layer above).
func PayloadInvalidError(toolName, msg, hint string) ResultValue {
	return NewError(toolName, PayloadInvalid, msg, hint, nil)
}

// NormalizeCallActorError normalises a tool result into the actor-CLI
// closed error set. The isError flag and value come from the upstream
// result; this function operates on pure Go maps, not go-kimi types.
func NormalizeCallActorError(toolName string, isError bool, value any, actorID, typeName string) (bool, any) {
	if !isError {
		return false, value
	}
	if valueMap, ok := value.(map[string]any); ok {
		if reason := StringValue(valueMap["error"]); reason != "" {
			rv := TerminalFailureToActorCLI(toolName, actorID, typeName, reason, valueMap["payload"])
			return true, rv.Value
		}
	}
	msg := strings.TrimSpace(fmt.Sprint(value))
	code := InternalError
	hint := "Inspect error.detail and adapter logs before retrying"
	if strings.Contains(strings.ToLower(msg), "timed out") {
		code = Timeout
		hint = "Increase max_pending_ms or check adapter logs"
	}
	rv := NewError(toolName, code, msg, hint, value)
	return true, rv.Value
}

// TerminalFailureToActorCLI maps a terminal failure reason to the
// actor-CLI closed error set.
func TerminalFailureToActorCLI(toolName, actorID, typeName, reason string, detail any) ResultValue {
	switch reason {
	case string(message.TerminalReceiverUnavailable):
		return NewError(
			toolName,
			ActorUnreachable,
			fmt.Sprintf("Actor %q is unreachable while handling type %q", actorID, typeName),
			"The actor reports its target is offline; check device/network status",
			detail,
		)
	case string(message.TerminalUnansweredTimeout):
		return NewError(
			toolName,
			Timeout,
			fmt.Sprintf("Actor %q timed out while handling type %q", actorID, typeName),
			"Increase max_pending_ms or check adapter logs",
			detail,
		)
	case string(message.TerminalReceiverInternalError):
		return NewError(
			toolName,
			InternalError,
			fmt.Sprintf("Actor %q returned an internal error while handling type %q", actorID, typeName),
			"Inspect error.detail and adapter logs before retrying",
			detail,
		)
	default:
		return NewError(
			toolName,
			InternalError,
			fmt.Sprintf("Actor %q returned failure %q while handling type %q", actorID, reason, typeName),
			"Inspect error.detail and adapter logs before retrying",
			detail,
		)
	}
}

// NormalizeCallActorResult normalises a call_actor ResultValue into the
// actor-CLI closed error set. An actor-RETURNED failure (ResultFromResponse sets
// a string reason under "error") renders as the actor-CLI failure line. A
// structured error (NewError: the CALL itself failed to build/emit/await — a
// {ok:false, error:{code,message}} shape) is ALREADY clean and is passed through
// unchanged; fmt.Sprint-ing that map into "returned failure 'map[code:…]'" would
// double-wrap a non-actor failure as an actor one.
func NormalizeCallActorResult(rv ResultValue, actorID, typeName string) ResultValue {
	if rv.Value == nil {
		return rv
	}
	if reason, ok := rv.Value["error"].(string); ok && strings.TrimSpace(reason) != "" {
		return TerminalFailureToActorCLI(rv.Name, actorID, typeName, strings.TrimSpace(reason), rv.Value["payload"])
	}
	return rv
}
