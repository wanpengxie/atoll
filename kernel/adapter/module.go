package adapter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// RuntimeEvent is a lifecycle signal pushed from the channel runtime
// into a Module so the adapter can own its own state machine without
// reading transport-layer plumbing.
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

	// Payload is an opaque framework-owned runtime event body. Kernel does
	// not know the source-specific schema; framework packages define the
	// kind strings and payload contracts they emit.
	Payload json.RawMessage
}

// RuntimeEventKind identifies the framework-owned runtime-event source.
// Kernel treats values as opaque; framework packages own their kind
// strings and payload schemas.
type RuntimeEventKind string

// RuntimeEventAware is the optional Module sub-interface. Modules that
// want runtime lifecycle signals implement this method; the framework
// type-asserts and skips delivery when absent. Method is invoked off the
// main Handle goroutine; implementations MUST be concurrency-safe with
// Handle / OnExternalCallback / Shutdown.
type RuntimeEventAware interface {
	OnRuntimeEvent(ctx context.Context, evt RuntimeEvent) error
}

// Heartbeater is the optional Module sub-interface for adapters that
// can report live readiness to the framework. Implementations must
// return promptly; the framework calls it with a short deadline.
type Heartbeater interface {
	Heartbeat(ctx context.Context) (HeartbeatReport, error)
}

// HeartbeatReport is the binding-specific readiness observation a
// Heartbeater returns. Reason values are convention-level diagnostics
// such as ok, initializing, upstream_unreachable, token_expired,
// shutdown, extension_disconnected, and unknown.
type HeartbeatReport struct {
	Available bool
	Reason    string
	Detail    map[string]any
	CheckedAt time.Time
}

// StatusReporter optionally enriches actor.status responses. The
// framework always provides a registry-backed baseline; this hook only
// adds binding-specific detail under a short deadline.
type StatusReporter interface {
	Status(ctx context.Context) (StatusReport, error)
}

// StatusReport carries optional detail for actor.status. Empty fields
// leave the registry-backed baseline unchanged.
type StatusReport struct {
	Available bool
	Reason    string
	Detail    map[string]any
	CheckedAt time.Time
}

// FieldDoc describes one product-layer payload field for actor-CLI
// describe_type output. It is convention metadata only: the protocol
// layer still treats payloads as opaque JSON.
type FieldDoc struct {
	Name        string `json:"name"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
	Example     any    `json:"example,omitempty"`
}

// ErrorDoc documents one adapter-specific response payload error_code
// for actor-CLI describe_type output. Meta-tool error codes remain a
// separate closed set.
type ErrorDoc struct {
	Code        string `json:"code"`
	Description string `json:"description,omitempty"`
	Recovery    string `json:"recovery,omitempty"`
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
// (AllowedKinds={event,request,response}). Adapters that provide it MUST
// cover every entry of Declaration.Types — manager.Install fails closed
// with InstallTypeRegistryInvalid on a partial map.
type TypeDeclaration struct {
	// AllowedKinds is the closed set of envelope.kind the harness will
	// accept for this type. Subset of {event, request, response}. When
	// empty, install uses {event, request, response}.
	AllowedKinds []message.Kind

	// Description is optional product-layer convention metadata used by
	// list_actors / describe_actor / describe_type. Install validation
	// does not require it.
	Description string

	// PayloadExample is an optional product-layer example returned by
	// describe_type. The protocol layer does not validate it.
	PayloadExample json.RawMessage

	// PayloadFields is optional field-by-field product-layer guidance
	// returned by describe_type.
	PayloadFields []FieldDoc

	// ErrorCodes is the optional adapter-specific error catalog returned
	// by describe_type. These codes are distinct from the meta-tool
	// closed set.
	ErrorCodes []ErrorDoc

	// Notes is optional markdown guidance returned by describe_type.
	Notes string
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
	// the allowed_kinds rows the harness loads at write time. Non-nil opts
	// the adapter into strict mode: every entry
	// of Types MUST have a matching row, otherwise install fails closed
	// with InstallTypeRegistryInvalid. Nil → permissive defaults for
	// every type (install logs a warning so the gap is observable).
	TypeDeclarations map[string]TypeDeclaration

	// Binding is the launch tri-class transport for this adapter (L1 §11.7).
	// Determines which framework helpers run (in-process dispatch /
	// outbound HTTP / runtime_inbound_via_relay).
	Binding actor.Binding

	// MaxPendingMs is the per-type request timeout (milliseconds). Used
	// by Ad-2 framework timeout timer (L1 §11.1 + L2 §8.3). Each Types
	// entry MUST have a positive value or type_registry install rejects
	// with adapter_timeout_missing.
	MaxPendingMs int64

	// Needs is the optional helper opt-in list (L2 §8.1 declares.needs).
	// Currently informational — Manager wires every helper unconditionally.
	Needs []string

	// Description is optional actor-CLI convention metadata: one-line
	// actor positioning for list_actors and describe_actor.
	Description string

	// SkillDoc is optional markdown usage guidance returned by
	// describe_actor after the LLM has selected this actor.
	SkillDoc string
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
	// webhook body, WS message, relay callback frame) into a Respond call.
	// Framework de-dupes
	// the callback before invoking (terminal already exists → not
	// invoked).
	OnExternalCallback(ctx context.Context, payload []byte) error
}

// ExternalCallbackFrame is the framework-owned wrapper for inbound
// runtime_inbound_via_relay callbacks. Payload remains adapter-domain
// data; request/channel/correlation identity comes from the transport
// wrapper stamped by the framework on the outbound leg and preserved by
// the callback bridge on the inbound leg.
type ExternalCallbackFrame struct {
	ChannelID      channel.ID
	AdapterActorID actor.ActorID
	RequestID      message.ID
	ParentID       message.ID
	CorrelationID  message.ID
	ExpiresAt      int64
	Payload        json.RawMessage
}

// ExternalCallbackFrameAware lets adapters consume the framework-owned
// callback wrapper without receiving raw transport capabilities.
type ExternalCallbackFrameAware interface {
	OnExternalCallbackFrame(ctx context.Context, frame ExternalCallbackFrame) error
}
