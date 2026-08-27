package metatool

import "strings"

// Every actor in the tree answers a failure with a specific error_code —
// invalid_args, no_service_agent, permission_denied, device_offline, and so on.
// Until this table existed none of that reached the model: the normaliser
// switched on the envelope's `reason`, whose whole vocabulary for an actor that
// answered at all is "receiver_internal_error", so a payload it could have
// fixed, a permission it could never get, and a channel with no service agent
// all arrived as "internal error, inspect logs before retrying". One observed
// session shows exactly that cost — the agent read a no_service_agent verdict
// as a transient fault and spent the rest of its turn probing unrelated words.
//
// The classes below are cut by what an agent can DO, not by which subsystem
// spoke. Two codes share a class when they call for the same next move, and
// `retry` says whether repeating the identical request could ever produce a
// different answer. A state fact — nothing is installed, you lack the right,
// the word does not exist — is never retryable; only a genuinely transient
// condition is.
type failureClass struct {
	code  ErrorCode
	hint  string
	retry bool
}

const (
	hintFixPayload   = "The request was understood and rejected as malformed or out of range. Read error.detail for the offending field, get the word's schema (describe_type for a member, system_describe for a system word), then send a corrected payload"
	hintNotFound     = "The named subject does not exist. Re-read the id from a listing (list_actors for members, the matching system.*.list for space objects) rather than reconstructing it"
	hintDenied       = "You lack the right to do this, and repeating it will not change that. Act on something you own, have its owner act, or change the object's visibility or ownership first"
	hintUnsupported  = "The target does not offer this at all, and will not start to on retry. Call describe_actor or system_describe to see what it does accept, and tell the requester plainly if nothing covers what was asked"
	hintNoSvcAgent   = "That channel has no service agent installed, so it answers nothing through agent.ask. This is a fact about how the channel was created, not a fault — do not retry. Membership words sent to its peer still work from the registry channel; anything needing an agent there requires one to be added first"
	hintConflict     = "Something with this identity already exists. Read the existing one and either use it or choose a different id — do not retry the create"
	hintUnreachable  = "The target is registered but not currently reachable. Check its presence with list_actors; retry only once it is present again"
	hintTimeout      = "The call exceeded its budget. The action may still be running — check the target's current facts, or query by an application idempotency key, before deciding whether a retry is safe"
	hintUnavailable  = "A dependency was momentarily unavailable. This one is genuinely transient: a retry may succeed"
	hintResultUnkown = "The outcome is unknown — the action may or may not have taken effect. Do NOT resubmit merely because the result is unclear; establish what happened first"
	hintInternal     = "The target failed internally. Read error.detail; retry only if the detail says the condition was transient"
	hintAddrHeld     = "The address could not be bound because something else is holding it, or it is not an address this host can listen on. Nothing was changed — whatever was serving before still is. Pick a different port, or wait for the current holder to release it, before trying again"
	hintCancelled    = "The call was cancelled before it finished. Something asked for it to stop, so resending it repeats work that was deliberately abandoned — confirm the cancellation was not yours before trying again"
)

// PermissionDenied, NotFound, Unsupported, Conflict and Unavailable are the
// additive widening the original five-value set anticipated. They exist because
// each one leads an agent somewhere different, which "internal_error" for all
// of them could not.
const (
	PermissionDenied ErrorCode = "permission_denied"
	NotFound         ErrorCode = "not_found"
	Unsupported      ErrorCode = "unsupported"
	Conflict         ErrorCode = "conflict"
	Unavailable      ErrorCode = "unavailable"
)

// actorErrorClasses maps the error_code an actor actually returned onto what
// the agent should do next. Codes are gathered from every answering surface:
// lagoon (the registry), the channel membrane, the peer gate, agent base, and
// the tool adapters. An unlisted code falls through to the reason-based
// classification, which is the old behaviour and still correct as a default.
//
// `retry` is decided here rather than carried from upstream on purpose.
// channelspec.OperationError does have a Retryable field, but every site that
// sets it derives the flag from the code it is already returning
// (authority_unavailable → true, decl_not_found / forbidden → false,
// channel_unavailable → true) — so the flag is a function of the code, and the
// verdicts below agree with all of them. Reconstructing it here costs nothing
// and covers the whole vocabulary, whereas plumbing one bool through
// OperateError, message.Failure and the wire would widen the protocol to carry
// something already derivable at the only place that consumes it.
var actorErrorClasses = map[string]failureClass{
	// Malformed or out-of-range arguments: fixable by resending.
	"invalid_args":         {PayloadInvalid, hintFixPayload, false},
	"payload_invalid":      {PayloadInvalid, hintFixPayload, false},
	"bad_payload":          {PayloadInvalid, hintFixPayload, false},
	"invalid_action":       {PayloadInvalid, hintFixPayload, false},
	"invalid_credentials":  {PayloadInvalid, hintFixPayload, false},
	"invalid_desired_host": {PayloadInvalid, hintFixPayload, false},
	"limit_exceeded":       {PayloadInvalid, hintFixPayload, false},
	"empty_input":          {PayloadInvalid, hintFixPayload, false},
	"invalid_input":        {PayloadInvalid, hintFixPayload, false},
	"output_limit":         {PayloadInvalid, hintFixPayload, false},

	// The subject named does not exist.
	"not_found":      {NotFound, hintNotFound, false},
	"decl_not_found": {NotFound, hintNotFound, false},

	// A waiting task that somebody dropped before it ran. Nothing is wrong and
	// nothing is retryable by itself: the work was withdrawn, so a caller that
	// still wants it asks again rather than retrying this one.
	"dismissed": {PermissionDenied, hintDenied, false},

	// An endpoint that could not take the address it was asked to take. Unlike
	// the other conflicts this one CAN clear on its own — a predecessor holding
	// the port releases it — so it is retryable, and the failing side is left
	// exactly as it was rather than half-moved.
	"bind_failed": {Conflict, hintAddrHeld, true},

	// Authority facts. Retrying identically can never succeed.
	"permission_denied":   {PermissionDenied, hintDenied, false},
	"forbidden":           {PermissionDenied, hintDenied, false},
	"unauthorized_sender": {PermissionDenied, hintDenied, false},
	"protected_actor":     {PermissionDenied, hintDenied, false},
	"not_accepted_source": {PermissionDenied, hintDenied, false},
	"bad_origin":          {PermissionDenied, hintDenied, false},

	// Capability facts: the thing asked for is not on offer here.
	"type_unsupported":   {Unsupported, hintUnsupported, false},
	"endpoint_not_found": {Unsupported, hintUnsupported, false},
	"unknown_class":      {Unsupported, hintUnsupported, false},
	"reserved":           {Unsupported, hintUnsupported, false},
	"no_service_agent":   {Unsupported, hintNoSvcAgent, false},

	// Identity collisions.
	"conflict_exists": {Conflict, hintConflict, false},

	// Present in the registry, absent right now.
	"receiver_inactive":  {ActorUnreachable, hintUnreachable, true},
	"member_inactive":    {ActorUnreachable, hintUnreachable, true},
	"device_offline":     {ActorUnreachable, hintUnreachable, true},
	"mcp_unreachable":    {ActorUnreachable, hintUnreachable, true},
	"dependency_missing": {ActorUnreachable, hintUnreachable, true},

	// Genuinely transient dependencies.
	"channel_unavailable":   {Unavailable, hintUnavailable, true},
	"authority_unavailable": {Unavailable, hintUnavailable, true},
	"provider_failed":       {Unavailable, hintUnavailable, true},

	"mcp_timeout": {Timeout, hintTimeout, false},
	"timeout":     {Timeout, hintTimeout, false},

	// Cancellation is a decision somebody already made, not a fault: repeating
	// the call would simply undo it.
	"mcp_cancelled": {Unsupported, hintCancelled, false},
	"cancelled":     {Unsupported, hintCancelled, false},

	// Outcome genuinely unknown — the one class where a blind retry can do
	// real damage, so it is called out separately from a plain failure.
	"result_unknown": {ResultUnknown, hintResultUnkown, false},

	// Downstream reported its own trouble; the detail is the useful part.
	"mcp_tool_error":     {InternalError, hintInternal, false},
	"mcp_result_invalid": {InternalError, hintInternal, false},
	"resource_error":     {InternalError, hintInternal, false},
	"schedule_failed":    {InternalError, hintInternal, false},
	"runtime_failed":     {InternalError, hintInternal, false},
}

// newClassifiedError reports a classified failure with `retryable` stated as a
// field, not only implied by the prose. The prose can be skimmed past; a field
// cannot. It marks the difference that matters most here — between a condition
// that will clear on its own and a fact about how the node is configured, which
// no amount of retrying will change.
func newClassifiedError(toolName string, class failureClass, msg string, detail any) ResultValue {
	rv := NewError(toolName, class.code, msg, class.hint, detail)
	if errObj, ok := rv.Value["error"].(map[string]any); ok {
		errObj["retryable"] = class.retry
	}
	return rv
}

// classifyActorError resolves the code an actor returned. The lookup is on the
// code alone: the same code means the same thing to an agent wherever it was
// raised, and a per-subsystem table would drift the moment a new adapter
// reused an existing code.
func classifyActorError(code string) (failureClass, bool) {
	class, ok := actorErrorClasses[strings.TrimSpace(code)]
	return class, ok
}

// FailureDetail is the error_code and detail an actor put in its terminal
// payload, lifted out so they can be reported at the top level instead of only
// surviving inside the opaque detail blob.
type FailureDetail struct {
	Code   string
	Detail string
}

func failureDetailOf(payload any) FailureDetail {
	obj, ok := payload.(map[string]any)
	if !ok {
		return FailureDetail{}
	}
	return FailureDetail{
		Code:   StringValue(obj["error_code"]),
		Detail: StringValue(obj["detail"]),
	}
}
