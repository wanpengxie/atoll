package kimi

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/kernel/message"
)

type actorCLIErrorCode string

const (
	actorCLIUnknownActor      actorCLIErrorCode = "unknown_actor"
	actorCLIUnknownType       actorCLIErrorCode = "unknown_type"
	actorCLIActorTypeMismatch actorCLIErrorCode = "actor_type_mismatch"
	actorCLIKindDisallowed    actorCLIErrorCode = "kind_disallowed"
	actorCLIPayloadInvalid    actorCLIErrorCode = "payload_invalid"
	actorCLIActorUnreachable  actorCLIErrorCode = "actor_unreachable"
	actorCLITimeout           actorCLIErrorCode = "timeout"
	actorCLIInternalError     actorCLIErrorCode = "internal_error"
)

func actorCLIErrorResult(toolName string, code actorCLIErrorCode, msg, recoveryHint string, detail any) types.ToolResult {
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
	return types.ToolResult{
		Name: toolName,
		Value: types.ToolReturnValue{Value: map[string]any{
			"ok":    false,
			"error": errObj,
		}},
		IsError: true,
	}
}

func unknownActorError(toolName, actorID string) types.ToolResult {
	return actorCLIErrorResult(
		toolName,
		actorCLIUnknownActor,
		fmt.Sprintf("Actor %q not found in this channel", actorID),
		"Call list_actors to see available actors",
		nil,
	)
}

func unknownTypeError(toolName, actorID, typeName string) types.ToolResult {
	return actorCLIErrorResult(
		toolName,
		actorCLIUnknownType,
		fmt.Sprintf("Type %q not found in this channel", typeName),
		fmt.Sprintf("Call describe_actor(%q) to see its available types", actorID),
		nil,
	)
}

func actorTypeMismatchError(toolName, actorID, typeName, realHandler string) types.ToolResult {
	return actorCLIErrorResult(
		toolName,
		actorCLIActorTypeMismatch,
		fmt.Sprintf("Type %q is handled by %q, not %q", typeName, realHandler, actorID),
		fmt.Sprintf("Type %q is handled by %q, not %q", typeName, realHandler, actorID),
		nil,
	)
}

func kindDisallowedError(toolName, typeName string, allowed []string) types.ToolResult {
	return actorCLIErrorResult(
		toolName,
		actorCLIKindDisallowed,
		fmt.Sprintf("Type %q is not request-callable (allowed_kinds=%v)", typeName, allowed),
		fmt.Sprintf("Type %q allowed_kinds = %v; only request-callable types support call_actor", typeName, allowed),
		nil,
	)
}

func payloadInvalidError(toolName, actorID, typeName, msg string) types.ToolResult {
	hint := "Call list_actors to see available actors and types"
	if strings.TrimSpace(actorID) != "" && strings.TrimSpace(typeName) != "" {
		hint = fmt.Sprintf("Call describe_type(%q, %q) to see payload_example", actorID, typeName)
	}
	return actorCLIErrorResult(toolName, actorCLIPayloadInvalid, msg, hint, nil)
}

func normalizeCallActorError(result types.ToolResult, actorID, typeName string) types.ToolResult {
	if !result.IsError {
		return result
	}
	if value, ok := result.Value.Value.(map[string]any); ok {
		if reason := stringValue(value["error"]); reason != "" {
			return terminalFailureToActorCLI(result.Name, actorID, typeName, reason, value["payload"])
		}
	}
	msg := strings.TrimSpace(fmt.Sprint(result.Value.Value))
	code := actorCLIInternalError
	hint := "Inspect error.detail and adapter logs before retrying"
	if strings.Contains(strings.ToLower(msg), "timed out") {
		code = actorCLITimeout
		hint = "Increase max_pending_ms or check adapter logs"
	}
	return actorCLIErrorResult(result.Name, code, msg, hint, result.Value.Value)
}

func terminalFailureToActorCLI(toolName, actorID, typeName, reason string, detail any) types.ToolResult {
	switch reason {
	case string(message.TerminalReceiverUnavailable):
		return actorCLIErrorResult(
			toolName,
			actorCLIActorUnreachable,
			fmt.Sprintf("Actor %q is unreachable while handling type %q", actorID, typeName),
			"The actor reports its target is offline; check device/network status",
			detail,
		)
	case string(message.TerminalUnansweredTimeout):
		return actorCLIErrorResult(
			toolName,
			actorCLITimeout,
			fmt.Sprintf("Actor %q timed out while handling type %q", actorID, typeName),
			"Increase max_pending_ms or check adapter logs",
			detail,
		)
	case string(message.TerminalReceiverInternalError):
		return actorCLIErrorResult(
			toolName,
			actorCLIInternalError,
			fmt.Sprintf("Actor %q returned an internal error while handling type %q", actorID, typeName),
			"Inspect error.detail and adapter logs before retrying",
			detail,
		)
	default:
		return actorCLIErrorResult(
			toolName,
			actorCLIInternalError,
			fmt.Sprintf("Actor %q returned failure %q while handling type %q", actorID, reason, typeName),
			"Inspect error.detail and adapter logs before retrying",
			detail,
		)
	}
}
