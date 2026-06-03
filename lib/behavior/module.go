package behavior

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

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
	// logging + diagnostics. Non-empty + unique within a channel.
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

	// Description is optional actor-CLI convention metadata: one-line
	// actor positioning for list_actors and describe_actor.
	Description string

	// SkillDoc is optional markdown usage guidance returned by
	// describe_actor after the LLM has selected this actor.
	SkillDoc string
}

// Module is the gen_server callback contract every adapter implements
// (lib/behavior = coagent's OTP behaviour). The host calls Declares first,
// then Init, then Handle on demand, finally Shutdown.
//
// SERIAL CONTRACT (v2, dismantle-spec §3 — the abstraction returning home):
// the adapterhost guarantees ALL Module callbacks (Handle / Heartbeat / Status
// / Shutdown) are invoked SERIALLY by this adapter actor's single cell
// goroutine. Inbound external I/O (device/webhook results) is the adapter's own
// private business: its reader folds results back by self-delivering an envelope
// onto its cell (ActorContext.Deliver), handled in the same serial Receive — NOT
// a framework callback. A Module MUST NOT depend on concurrent
// invocation, and MUST NOT read or write its own logical state from a
// goroutine it spawned itself. External resources (HTTP client, watcher
// goroutine) may carry their own synchronisation, but their results MUST be
// folded back onto the actor via the mailbox (self-post / Ask) before they
// touch the actor's logical state. Because the cell goroutine is the sole
// owner, a Module holds logical state in plain fields — no mutex/atomic.
type Module interface {
	// Declares returns the static metadata. Called exactly once per
	// Install. Result MUST be deterministic (same call → same value).
	Declares() Declaration

	// Init receives the ModuleContext (respond/emit helpers + injected
	// deps). The adapter MUST persist the *ModuleContext if it intends to
	// call helpers from Handle.
	Init(ctx context.Context, mctx *ModuleContext) error

	// Shutdown cancels in-flight work + releases external connections
	// (best-effort).
	Shutdown(ctx context.Context) error

	// Handle translates one inbound kind=request envelope into outbound
	// protocol traffic. Returning nil with no terminal leaves the request
	// pending — the adapter answers later (e.g. once its own external call
	// completes and it self-delivers the result onto its cell), or the
	// caller-scoped closure times it out. A hard error collapses to a
	// receiver_internal_error terminal.
	Handle(ctx context.Context, env *message.Envelope) error
}
