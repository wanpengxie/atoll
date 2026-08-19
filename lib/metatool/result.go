package metatool

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/message"
)

// ErrorCode is the actor-CLI closed error set.
type ErrorCode string

const (
	PayloadInvalid   ErrorCode = "payload_invalid"
	ActorUnreachable ErrorCode = "actor_unreachable"
	Timeout          ErrorCode = "timeout"
	ResultUnknown    ErrorCode = "result_unknown"
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
	// The actor's own verdict wins when it gave one. `reason` distinguishes
	// only how the request ended (the receiver answered / went silent / timed
	// out); error_code says WHAT was wrong, which is the half an agent can act
	// on. Reading it first is what turns "internal error, inspect logs" into
	// "this argument is wrong" or "this will never be permitted".
	if failure := failureDetailOf(detail); failure.Code != "" {
		if class, known := classifyActorError(failure.Code); known {
			message := fmt.Sprintf("Actor %q refused %q: %s", actorID, typeName, failure.Code)
			if failure.Detail != "" {
				message = fmt.Sprintf("Actor %q refused %q (%s): %s", actorID, typeName, failure.Code, failure.Detail)
			}
			return newClassifiedError(toolName, class, message, detail)
		}
	}
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
			"Check the target's current facts and adapter logs before deciding whether a retry is safe",
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

// (AccessFailureToActorCLI retired — purity v3 C4: minted as the sister of
// TerminalFailureToActorCLI "so a future tool wrapping sys.Resource() has a
// ready mapping", i.e. ahead of any producer (零预留违例). When a real caller
// appears, rebuild it mirroring the sister above; the design that held is the
// STAGE collapse over protocol/access's frozen 5-value FailureReason
// (reason.go's resolve→authorize→execute→return framing): resolve/authorize
// verdicts (ResourceNotFound / AlreadyExists / AccessDenied — "the request as
// posed cannot proceed") → PayloadInvalid; execute/return verdicts
// (DriverError / OutcomeUnknown — internal-side) → InternalError. A dedicated
// access_denied ErrorCode stays additive if a future caller needs finer
// granularity than that collapse.)

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
