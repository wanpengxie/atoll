package adapter

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// RuntimeEvent is a lifecycle signal pushed from the channel runtime
// into a Module so the adapter can own its own device-state machine
// without reading transport-layer plumbing.
//
// The framework dispatches one RuntimeEvent per signal source:
//   - devicebus ws register / unregister / token-expiry (binding =
//     runtime_inbound_via_relay) → device-lifecycle event.
//
// Other binding kinds (embedded / runtime_outbound) currently receive
// nothing; the channel-lifecycle hooks (boot / fence-loss / shutdown,
// proto-layer1 §3.6 O6) are wired separately by the framework
// composition root, not through this enum.
//
// Modules opt in by implementing RuntimeEventAware; modules that don't
// implement it never see RuntimeEvents and the framework drops them
// silently.
type RuntimeEvent struct {
	// Kind is the event source / closed set.
	Kind RuntimeEventKind

	// ChannelID is the channel this signal belongs to. Always non-empty.
	// A Module instance is bound to exactly one channel so this MUST equal
	// ModuleContext.ChannelID; the framework rejects mismatches before
	// invoking the hook.
	ChannelID channel.ID

	// AdapterActorID echoes the adapter actor identity the runtime event
	// applies to. For runtime_inbound_via_relay this is always the
	// Module's own actor id; included so future multi-actor-per-module
	// implementations stay possible.
	AdapterActorID actor.ActorID

	// DeviceLifecycle is non-nil iff Kind == RuntimeEventDeviceLifecycle.
	// Carries the devicebus-side connect / disconnect / token-expired
	// signal. See kernel/devicetransit.LifecycleFrame for the wire shape.
	DeviceLifecycle *devicetransit.LifecycleFrame
}

// RuntimeEventKind enumerates the runtime-event sources a Module can
// receive. Closed set; new kinds are protocol-level additions.
type RuntimeEventKind string

const (
	// RuntimeEventDeviceLifecycle — devicebus connection lifecycle
	// (register / unregister / token expired) for binding =
	// runtime_inbound_via_relay adapters.
	RuntimeEventDeviceLifecycle RuntimeEventKind = "device_lifecycle"
)

// RuntimeEventAware is the optional Module sub-interface. Modules that
// want device / channel lifecycle signals implement this method; the
// framework type-asserts and skips delivery when absent. Method is
// invoked off the main Handle goroutine; implementations MUST be
// concurrency-safe with Handle / OnExternalCallback / Shutdown.
type RuntimeEventAware interface {
	OnRuntimeEvent(ctx context.Context, evt RuntimeEvent) error
}

// TypeDeclaration is the per-type install metadata an adapter declares
// for the Message-Write Harness install path (L2 §1.4.2). Payload
// schema is intentionally absent: protocol Level A
// (proto-layer0 §1.4.1 / proto-layer1 §1.3) leaves payload opaque, so
// the harness does NOT validate payload schemas and the type_registry
// does NOT store payload schemas.
//
// TypeDeclaration is OPTIONAL: adapters that omit it for a Types entry
// fall back to permissive defaults
// (AllowedKinds={event,request,response}, payload_status terminal
// convention). Adapters that provide it MUST cover every entry of
// Declaration.Types — manager.Install fails closed with
// InstallTypeRegistryInvalid on a partial map.
type TypeDeclaration struct {
	// AllowedKinds is the closed set of envelope.kind the harness will
	// accept for this type. Subset of {event, request, response}. When
	// empty, install uses {event, request, response}.
	AllowedKinds []message.Kind

	// TerminalConvention controls harness step 8 is_terminal computation
	// (proto-layer1 §2.8). Either "payload_status" (default) or
	// "single-response". Empty string is normalised to "payload_status"
	// at install time.
	TerminalConvention string
}

// Declaration is the static metadata an adapter Module exposes at
// install time. Mirrors the L2 §8.1 framework Declaration with the
// actor.Binding tri-class transport per L1 §11.7.
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

	// TypeDeclarations optionally maps type → TypeDeclaration, supplying
	// the allowed_kinds + terminal_convention rows the harness loads at
	// write time. Non-nil opts the adapter into strict mode: every entry
	// of Types MUST have a matching row, otherwise install fails closed
	// with InstallTypeRegistryInvalid. Nil → permissive defaults for
	// every type (install logs a warning so the gap is observable).
	TypeDeclarations map[string]TypeDeclaration

	// Binding is the launch tri-class transport for this adapter (L1 §11.7).
	// Determines which framework helpers run (in-process dispatch /
	// outbound HTTP / runtime_inbound_via_relay + DeviceTransit).
	Binding actor.Binding

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
	// launch baseline: best-effort, invoked from Manager.Shutdown only.
	Shutdown(ctx context.Context) error

	// Handle translates one inbound kind=request envelope into outbound
	// protocol traffic. Returning an error leaves the request pending —
	// the F3 timer eventually emits unanswered_timeout via the framework.
	// Adapters typically return nil once the external call is launched +
	// the correlation is tracked.
	Handle(ctx context.Context, env *message.Envelope) error

	// OnExternalCallback translates one inbound external callback (e.g.
	// webhook body, WS message, `device_transit.send` frame —
	// impl-layer2 §5.3.1 inbound) into a Respond call. Framework de-dupes
	// the callback before invoking (terminal already exists → not
	// invoked).
	OnExternalCallback(ctx context.Context, payload []byte) error
}
