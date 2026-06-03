package behavior

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ModuleContext is the helper bundle injected into a Module at Init — it
// carries the respond/emit helpers plus the actor's addressing context
// (L2 §8.1 ModuleContext).
//
// A Module grabs the helpers off ModuleContext and calls them — it does
// NOT pull dependencies in directly. This is the kernel-level seam that
// lets tests substitute every collaborator.
type ModuleContext struct {
	// AdapterActorID is the actor_registry id this adapter owns
	// (Declaration.ActorID).
	AdapterActorID actor.ActorID

	// ChannelID identifies the channel this adapter instance services
	// (each channel gets its own adapter instance bound to its own
	// channel sqlite — L1 §11.5 / L2 §8.6).
	ChannelID channel.ID

	// Respond is the F5 helper that wraps the adapter's outbound
	// terminal response. Hides harness invocation + Ad-2 dedupe race +
	// canonical-id derivation.
	Respond RespondFunc

	// Fail emits a failed terminal response through the same write-first
	// lifecycle path as Respond.
	Fail FailFunc

	// EmitEvent emits an adapter-owned event. It cannot emit responses.
	EmitEvent EmitEventFunc

	// Provisional emits one non-terminal interim response (kind=response,
	// payload.status ∈ provisional subset) for a pending request without
	// resolving its closure (proto-foundation §1.6.3 + proto-layer0 §2.5).
	//
	// Callers may invoke Provisional multiple times per request, but only
	// before the final Respond / Fail — emitting a provisional after a
	// final response is rejected by harness Step 8 as a zombie chain
	// (harness_provisional_after_final).
	//
	// Status MUST be a valid provisional status:
	//   - Layer 2 core (closed set): received / queued / processing /
	//     deferred / unavailable.
	//   - Layer 3 extension: <adapter_short_name>.<name> where the
	//     namespace matches sender.id local-name; harness enforces format,
	//     namespace ownership, and core/final shadowing
	//     (harness_response_status_namespace_mismatch /
	//     harness_response_status_invalid).
	//
	// Reuses the Respond envelope shape (audience = parent request sender,
	// parent_id = requestID, type = request.type, sender = the actor).
	// Unlike Respond, it does NOT touch the pending correlation registry or
	// F3 timer; the request remains in flight until the final Respond / Fail
	// or O3 fallback.
	Provisional ProvisionalFunc
}

// RespondOptions adjusts the Respond call (L2 §8.5 + L1 §11.1 Ad-2).
type RespondOptions struct {
	// Status is the payload.status value ("completed" / "failed").
	// Empty defaults to "completed" — adapters MUST set "failed"
	// explicitly for failure terminals.
	Status string

	// Reason populates payload.reason on failure (closed set; empty when
	// completed).
	Reason message.TerminalFailureReason

	// Visibility overrides the default response visibility (which
	// inherits from the request). Empty = inherit.
	Visibility message.Visibility
}

// RespondResult is what RespondFunc returns when the harness write
// succeeds (or the terminal-duplicate path treats it as a benign no-op).
type RespondResult struct {
	MessageID message.ID
}

// RespondFunc is the F5 contract — the caller invokes it inside Handle (or
// later, once its own async work completes) to emit the terminal response.
//
// Constructs the envelope (sender = the actor, kind = response,
// parent_id = requestID, payload = wrapped via RespondOptions), runs it
// through the harness, and returns the resulting RespondResult.
type RespondFunc func(
	ctx context.Context,
	requestID CorrelationKey,
	payload json.RawMessage,
	opts RespondOptions,
) (RespondResult, error)

// ProvisionalOptions adjusts the Provisional call. Status is supplied as
// a positional argument; this struct carries the optional knobs.
type ProvisionalOptions struct {
	// Visibility overrides the default response visibility (which
	// inherits from the request). Empty = inherit.
	Visibility message.Visibility
}

// ProvisionalFunc is the provisional response helper. Constructs the
// envelope (kind=response, sender=the actor, parent_id=requestID, payload
// includes status + caller-supplied fields) and writes it through the
// harness chain. Final closure state (pending
// registry + F3 timer) is intentionally untouched — provisional response
// does not resolve the request.
type ProvisionalFunc func(
	ctx context.Context,
	requestID CorrelationKey,
	status string,
	payload json.RawMessage,
	opts ProvisionalOptions,
) (RespondResult, error)

// FailOptions adjusts a failed terminal emitted through FailFunc.
type FailOptions struct {
	// Reason is the closed-set terminal failure reason. Empty defaults
	// to receiver_internal_error.
	Reason message.TerminalFailureReason

	// Visibility overrides the default response visibility.
	Visibility message.Visibility
}

// FailFunc is the failed terminal helper. Writes first, then closes
// correlation/timer only after accepted or terminal_duplicate.
type FailFunc func(
	ctx context.Context,
	requestID CorrelationKey,
	payload json.RawMessage,
	opts FailOptions,
) (RespondResult, error)

// EmitEventOptions adjusts adapter event emission.
type EmitEventOptions struct {
	Visibility message.Visibility
	Audience   message.Audience
}

// EmitEventFunc emits one adapter-owned event. Implementations must
// reject non-event writes.
type EmitEventFunc func(
	ctx context.Context,
	eventType string,
	payload json.RawMessage,
	opts EmitEventOptions,
) (message.ID, error)
