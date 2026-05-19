package adapter

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/harness"
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

	// Correlation is the F2 tracker scoped to this adapter.
	Correlation CorrelationTracker

	// ErrorPolicy is the F3 timeout / retry / fallback emitter.
	ErrorPolicy ErrorPolicy

	// Respond is the F5 helper that wraps the adapter's outbound
	// terminal response. Hides harness invocation + Ad-2 dedupe race +
	// canonical-id derivation.
	Respond RespondFunc

	// HarnessChain is the M1.5 entry-point caller for raw harness
	// writes (used by adapters that need to emit non-response messages
	// — e.g. orphan_callback events).
	HarnessChain harness.Chain

	// DeviceTransit is non-nil iff Declaration.Binding ==
	// BindingViaServerTransit. Other binding types receive nil; the
	// framework refuses to call Handle when DeviceTransit is required
	// but absent (T3 composition root wires it).
	DeviceTransit devicetransit.DeviceTransit
}

// RespondOptions adjusts the Respond call (L2 §8.5 + L1 §11.1 Ad-2).
type RespondOptions struct {
	// Status is the payload.status value ("completed" / "failed").
	// Empty defaults to "completed" — adapters MUST set "failed"
	// explicitly for failure terminals.
	Status string

	// Reason populates payload.reason on failure. Optional for
	// "completed".
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
