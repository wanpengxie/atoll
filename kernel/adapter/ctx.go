package adapter

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ModuleContext is the helper bundle injected into every adapter Module
// by Manager.Init (L2 §8.1 ModuleContext).
//
// Adapter handlers grab the helpers off ModuleContext and call them —
// they do NOT pull dependencies in directly. This is the kernel-level
// seam that lets tests substitute every collaborator.
type ModuleContext struct {
	// AdapterName mirrors Declaration.Name — convenience, so handlers
	// don't have to thread the name through their own state.
	AdapterName string

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

	// CompleteExternalResponse completes a pending request from an
	// externally supplied response envelope after framework validation.
	CompleteExternalResponse ExternalResponseFunc

	// EmitEvent emits an adapter-owned event. It cannot emit responses.
	EmitEvent EmitEventFunc

	// ReportOrphanCallback emits the framework-owned orphan callback
	// observability event for this adapter actor. The adapter supplies only
	// callback diagnostics; framework stamps adapter/channel identity and
	// the event type.
	ReportOrphanCallback OrphanCallbackFunc

	// ForwardExternalRequest sends an already accepted request to an
	// external transport without owning request lifecycle state. The
	// adapter supplies only the domain payload; the framework stamps the
	// transport wrapper identity from the accepted envelope.
	ForwardExternalRequest ExternalRequestFunc

	// UpdateReadiness updates this adapter actor's readiness through the
	// framework-owned readiness path.
	UpdateReadiness ReadinessFunc

	// LookupPendingRequest exposes read-only request lifecycle state to
	// callback decoders. It does not grant mutation authority.
	LookupPendingRequest PendingRequestLookupFunc
}

// RespondOptions adjusts the Respond call (L2 §8.5 + L1 §11.1 Ad-2).
type RespondOptions struct {
	// Status is the payload.status value ("completed" / "failed").
	// Empty defaults to "completed" — adapters MUST set "failed"
	// explicitly for failure terminals.
	Status string

	// Reason populates payload.reason on failure. When non-empty it MUST
	// be a message.TerminalFailureReason wire value.
	Reason string

	// Visibility overrides the default response visibility (which
	// inherits from the request). Empty = inherit.
	Visibility message.Visibility

	// Audience overrides the response audience. Empty = use the
	// request's sender as the single audience entry (the canonical
	// "respond to caller" path).
	Audience message.Audience

	// Dedupe signals to the framework that a duplicate inbound
	// callback should be tagged with payload.dedupe=true (used by
	// observability).
	Dedupe bool
}

// RespondResult is what RespondFunc returns when the harness write
// succeeds (or dedupes against an existing terminal).
type RespondResult struct {
	MessageID message.ID
	Deduped   bool // true when harness step 8 detected terminal_duplicate
}

// RespondFunc is the F5 contract — adapter calls it inside Handle /
// OnExternalCallback to emit the terminal response.
//
// The framework constructs the envelope (sender = adapter actor,
// kind = response, parent_id = requestID, payload = wrapped via
// RespondOptions), runs it through the harness, and returns the
// resulting RespondResult.
type RespondFunc func(
	ctx context.Context,
	requestID CorrelationKey,
	payload json.RawMessage,
	opts RespondOptions,
) (RespondResult, error)

// FailOptions adjusts a failed terminal emitted through FailFunc.
type FailOptions struct {
	// Reason is the closed-set terminal failure reason. Empty defaults
	// to receiver_internal_error.
	Reason message.TerminalFailureReason

	// Visibility overrides the default response visibility.
	Visibility message.Visibility

	// Audience overrides the response audience.
	Audience message.Audience
}

// FailFunc is the adapter-facing failed terminal helper. The framework
// writes first, then closes correlation/timer only after accepted or
// terminal_duplicate.
type FailFunc func(
	ctx context.Context,
	requestID CorrelationKey,
	payload json.RawMessage,
	opts FailOptions,
) (RespondResult, error)

// ExternalResponseFunc completes a request with a response envelope
// produced outside the channel daemon. The framework validates the
// envelope against the pending parent request before writing.
type ExternalResponseFunc func(
	ctx context.Context,
	env *message.Envelope,
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

// OrphanCallbackReport describes an inbound callback that could not be
// parsed or correlated by an adapter. It intentionally carries no adapter
// identity fields; framework derives those from the installed Declaration.
type OrphanCallbackReport struct {
	CorrelationID string
	Detail        string
	Payload       json.RawMessage
}

// OrphanCallbackFunc emits the canonical orphan-callback adapter event.
// It cannot emit arbitrary events or responses.
type OrphanCallbackFunc func(
	ctx context.Context,
	report OrphanCallbackReport,
) error

// ExternalRequestResult is returned after forwarding an external
// request to transport.
type ExternalRequestResult struct {
	FrameID string
}

// ExternalRequestPayload is the adapter-owned domain payload carried
// inside the framework-owned external transport wrapper.
type ExternalRequestPayload json.RawMessage

func (p ExternalRequestPayload) MarshalJSON() ([]byte, error) {
	return json.RawMessage(p).MarshalJSON()
}

func (p *ExternalRequestPayload) UnmarshalJSON(data []byte) error {
	if p == nil {
		return nil
	}
	*p = append((*p)[:0], data...)
	return nil
}

// ExternalRequestFunc forwards a request envelope and domain payload to
// the runtime-owned external transport. It does not reserve or close
// pending lifecycle state, and it does not accept caller-stamped transport
// identity fields.
type ExternalRequestFunc func(
	ctx context.Context,
	env *message.Envelope,
	payload ExternalRequestPayload,
) (ExternalRequestResult, error)

// ReadinessFunc updates readiness for this adapter actor only.
type ReadinessFunc func(
	ctx context.Context,
	update actorreg.ReadinessUpdate,
) (actorreg.ReadinessTransition, error)

// PendingRequestLookupFunc exposes a read-only snapshot of a pending
// request lifecycle entry.
type PendingRequestLookupFunc func(
	ctx context.Context,
	requestID CorrelationKey,
) (CorrelationEntry, bool, error)
