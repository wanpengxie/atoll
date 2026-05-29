package adapter

import (
	"context"
	"encoding/json"
	"time"

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

	// AdapterActorKind is the actor_registry kind this adapter owns. Current
	// adapter Declaration.ActorID rows are tool actors; exposing the kind lets
	// callback decoders reject callbacks that spoof the same id under a different
	// actor.kind.
	AdapterActorKind actor.Kind

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

	// ── caller side (framework sync/async mechanism, §2.2) ──
	//
	// Submit synchronously writes a request envelope through the harness and
	// returns once accepted (§0③): the caller gets a durable RequestID + an
	// ack descriptor. Awaiting the response is a subsequent Await / Watch.
	Submit SubmitFunc

	// Await blocks until the request's final response arrives, ctx is done,
	// or timeout elapses (timeout 0 = the type default per R5).
	Await AwaitFunc

	// Watch returns a stream of every response (provisional + final) for the
	// request.
	Watch WatchFunc

	// AwaitAll waits for every request's final, all-settled (§0②): it returns
	// one Outcome per id and never returns early on a single failure.
	AwaitAll AwaitAllFunc

	// Call is sugar over Submit + Await — the synchronous one-shot path.
	Call CallFunc

	// Abandon drops the local waiter for a request (fan-out early-failure
	// sibling drop). It does NOT touch the substrate; the daemon-side pending
	// + F3 stay intact.
	Abandon AbandonFunc

	// ── receiver side (framework sync/async mechanism, §2.2) ──
	//
	// Resolve produces the final response for a pending request from a
	// receiver-supplied ResolveRequest (status / payload / reason). On
	// success it closes the pending correlation + cancels the F3 timer
	// through the router's single lifecycle center. It is the receiver-side
	// terminal path for deferred requests (Handle → Deferred, resolved later
	// via an external callback or async completion).
	Resolve ResolveFunc

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
	// The framework reuses the Respond envelope shape (audience = parent
	// request sender, parent_id = requestID, type = request.type, sender =
	// adapter actor). Unlike Respond, it does NOT touch the pending
	// correlation registry or F3 timer; the request remains in flight until
	// the final Respond / Fail or O3 fallback.
	Provisional ProvisionalFunc
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

// ProvisionalOptions adjusts the Provisional call. Status is supplied as
// a positional argument; this struct carries the optional knobs.
type ProvisionalOptions struct {
	// Visibility overrides the default response visibility (which
	// inherits from the request). Empty = inherit.
	Visibility message.Visibility

	// Audience overrides the response audience. Empty = use the request's
	// sender as the single audience entry (the canonical "respond to
	// caller" path; Layer 0 §2.5 keeps provisional audience cardinality
	// at 1).
	Audience message.Audience
}

// ProvisionalFunc is the adapter-facing provisional response helper. The
// framework constructs the envelope (kind=response, sender=adapter actor,
// parent_id=requestID, payload includes status + user-supplied fields)
// and writes it through the harness chain. Final closure state (pending
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

// SubmitFunc is the caller-side ctx.Submit contract (§0③). It writes a
// request envelope through the harness and returns once accepted.
type SubmitFunc func(ctx context.Context, req CallRequest) (SubmitResult, error)

// AwaitFunc is the caller-side ctx.Await contract.
type AwaitFunc func(ctx context.Context, id RequestID, timeout time.Duration) (Terminal, error)

// WatchFunc is the caller-side ctx.Watch contract.
type WatchFunc func(ctx context.Context, id RequestID) (Watcher, error)

// AwaitAllFunc is the caller-side ctx.AwaitAll contract (all-settled, §0②).
type AwaitAllFunc func(ctx context.Context, ids []RequestID, timeout time.Duration) ([]Outcome, error)

// CallFunc is the caller-side ctx.Call sugar (= Submit + Await).
type CallFunc func(ctx context.Context, req CallRequest) (Terminal, error)

// AbandonFunc is the caller-side ctx.Abandon contract — drop the local
// waiter, leave the substrate untouched.
type AbandonFunc func(id RequestID)

// ResolveFunc is the receiver-side ctx.Resolve contract — produce the final
// response for a pending request from a ResolveRequest (status/payload/reason).
type ResolveFunc func(ctx context.Context, id RequestID, r ResolveRequest) error
