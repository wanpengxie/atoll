package adapter

import (
	"context"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/message"
)

// Declaration is the static metadata an adapter Module exposes at
// install time. Mirrors the L2 §8.1 framework Declaration with M1.5
// updates (Binding → BindingKind tri-class per L1 §11.7).
//
// Every field is read once during Manager.Install — Modules MUST keep
// the value side-effect-free and identical across calls.
type Declaration struct {
	// Name is the adapter identifier (e.g. "xhs", "feishu"). Used for
	// logging, orphan-callback event payload, and routing
	// OnExternalCallback by adapter name. Non-empty + unique within a
	// Manager.
	Name string

	// ActorID is the actor_registry row this adapter owns. Every request
	// envelope whose audience[0] equals ActorID dispatches to this
	// Module.Handle. MUST refer to a pre-registered tool actor
	// (actor_kind='tool').
	ActorID actor.ActorID

	// Types lists the envelope.type strings the adapter accepts. Each
	// entry MUST already exist in type_registry with handler_actor_id
	// == ActorID and handler_binding == Binding (Manager.Install
	// verifies).
	Types []string

	// Binding is the M1.5 tri-class transport for this adapter (L1
	// §11.7). Determines which framework helpers run (in-process
	// dispatch / outbound HTTP / via_server_transit + DeviceTransit).
	Binding BindingKind

	// MaxPendingMs is the per-type request timeout (milliseconds). Used
	// by Ad-2 framework timeout timer (L1 §11.1 + L2 §8.3). Each Types
	// entry MUST have a positive value or type_registry install rejects
	// with adapter_timeout_missing.
	MaxPendingMs int64

	// Needs is the optional helper opt-in list (L2 §8.1 declares.needs).
	// Currently informational — Manager wires every helper unconditionally.
	Needs []string
}

// Module is the contract every adapter implements (L2 §8.1 F1). The
// framework calls Declares first, then Init, then Handle /
// OnExternalCallback on demand, finally Shutdown.
//
// All methods MUST be safe for concurrent invocation: the framework
// may dispatch overlapping Handle calls (one per inbound request) and
// an OnExternalCallback at the same time.
type Module interface {
	// Declares returns the static metadata. Called exactly once per
	// Install. Result MUST be deterministic (same call → same value).
	Declares() Declaration

	// Init receives the framework-built ModuleContext. The adapter MUST
	// persist the *ModuleContext if it intends to call helpers
	// (Respond / ErrorPolicy / Correlation) from Handle /
	// OnExternalCallback.
	Init(ctx context.Context, mctx *ModuleContext) error

	// Shutdown cancels in-flight work + releases external connections.
	// M1.5 baseline: best-effort, invoked from Manager.Shutdown only.
	Shutdown(ctx context.Context) error

	// Handle translates one inbound kind=request envelope into outbound
	// protocol traffic. Returning an error leaves the request pending —
	// the F3 timer eventually emits adapter_default_timeout via the
	// framework. Adapters typically return nil once the external call
	// is launched + the correlation is tracked.
	Handle(ctx context.Context, env *message.Envelope) error

	// OnExternalCallback translates one inbound external callback (e.g.
	// webhook body, WS message, device_transit.recv frame) into a
	// Respond call. Framework de-dupes the callback before invoking
	// (terminal already exists → not invoked).
	OnExternalCallback(ctx context.Context, payload []byte) error
}
