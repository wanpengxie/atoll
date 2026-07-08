package metatool

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
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

// AccessFailureToActorCLI maps a resource access-door FailureReason
// (protocol/access's own frozen 5-value closed set — a DIFFERENT axis from
// message.TerminalFailureReason above: access failures are the resource
// axis's own verdict, not a call/dispatch failure) to the actor-CLI closed
// error set (期11 spec §6 account item ⑤). No domain tool call site
// exercises this yet this period (§9's own "零应用迁移") — additive, so a
// future tool wrapping sys.Resource() has a ready mapping rather than
// inventing its own ad hoc one. The 4-member ErrorCode set is coarser than
// FailureReason's 5, so this collapses by STAGE (protocol/access/reason.go's
// own resolve→authorize→execute→return framing): resolve/authorize verdicts
// (a bad id, a collision, a denial) are all "the request as posed cannot
// proceed" → PayloadInvalid; execute/return verdicts (the driver faulted, or
// completion went unconfirmed) are internal-side → InternalError. A
// dedicated access_denied ErrorCode is additive if a future caller needs
// finer granularity than this collapse gives.
func AccessFailureToActorCLI(toolName string, reason access.FailureReason, detail any) ResultValue {
	switch reason {
	case access.ResourceNotFound:
		return NewError(toolName, PayloadInvalid, "resource not found", "Check the resource id and retry", detail)
	case access.AlreadyExists:
		return NewError(toolName, PayloadInvalid, "resource already exists", "Use a different id or delete the existing one first", detail)
	case access.AccessDenied:
		return NewError(toolName, PayloadInvalid, "access denied", "The caller is not authorized for this operation on this resource", detail)
	case access.DriverError:
		return NewError(toolName, InternalError, "resource driver failed", "Inspect error.detail and adapter logs before retrying", detail)
	case access.OutcomeUnknown:
		return NewError(toolName, InternalError, "resource operation outcome unconfirmed", "The operation may or may not have landed; verify before retrying", detail)
	default:
		return NewError(toolName, InternalError, fmt.Sprintf("unrecognized access failure reason %q", reason), "Inspect error.detail and adapter logs before retrying", detail)
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
