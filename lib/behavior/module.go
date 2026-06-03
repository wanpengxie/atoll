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

// APIDescriptor describes one callable API an actor exposes. It is returned
// DYNAMICALLY by the actor's describe self-answer (see Describer) — never
// predefined at install. The actor is the sole authority on its own capability
// surface; a caller discovers it by asking the actor, live.
type APIDescriptor struct {
	// Name is the request envelope.type the API answers (e.g. "xhs.publish").
	Name string `json:"name"`
	// Schema is the parameter schema for the request payload — a caller uses it
	// to construct a valid call. Its concrete format is the actor's domain
	// concern (opaque to the framework here).
	Schema json.RawMessage `json:"schema,omitempty"`
	// Desc is a one-line description of what the API does.
	Desc string `json:"desc,omitempty"`
	// Skill is optional longer usage guidance (markdown) for an LLM caller.
	Skill string `json:"skill,omitempty"`
}

// Describer is the OPTIONAL Module sub-interface for actors that expose a
// capability surface. adapterhost routes the reserved actor.describe query to
// Describe and relays the result — answered LIVE on the cell goroutine, so the
// actor reports its CURRENT APIs (e.g. only what it can do while logged in),
// never a stale predefined registry. Adapters that don't implement it answer
// describe with their identity only.
//
// There is deliberately NO declared type list or per-type catalog on the
// Declaration: "what can I do" is this dynamic self-answer; "what I dispatch"
// is the Module's own Handle (the substrate is type-agnostic — it does not gate
// on business types, so neither does lib).
type Describer interface {
	Describe(ctx context.Context) ([]APIDescriptor, error)
}

// Declaration is the static IDENTITY an adapter Module exposes at install time
// — purely what the framework needs to address and spawn the actor. The actor's
// capability surface is NOT declared here (it is the dynamic describe
// self-answer, see Describer); its request dispatch is its Handle, not a
// declared type list.
type Declaration struct {
	// Name is the adapter identifier (e.g. "xhs", "feishu"). Logging + the
	// describe identity fallback. Non-empty + unique within a channel.
	Name string

	// ActorID is the membership row this adapter owns. Every request envelope
	// whose audience[0] equals ActorID dispatches to this Module.Handle. MUST
	// refer to a pre-registered actor.
	ActorID actor.ActorID

	// Binding is the launch tri-class transport for this adapter (L1 §11.7).
	Binding actor.Binding
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
