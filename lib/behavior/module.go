package behavior

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// NOTE: there is no Heartbeater / StatusReporter / report types, and no
// actor.status query at all. An actor's serviceable-state is the OUTCOME of
// send→terminal (the substrate materialises receiver_unavailable when the actor
// is gone), not a polled or queryable gate. When a concrete adapter needs to
// surface non-trivial domain state (e.g. "not logged in"), an optional Statuser
// self-answer (parallel to lib/introspect.Describer) is added additively — not
// pre-built here.

// The actor's capability surface (actor.describe) is NOT declared here: it is
// the actor's dynamic self-answer via lib/introspect.Describer. Its request
// dispatch is its Handle, not a declared type list (the substrate is
// type-agnostic — it does not gate on business types, so neither does lib).

// Declaration is the static IDENTITY a Module exposes at install time
// — purely what the host needs to address and spawn the actor. The actor's
// capability surface is NOT declared here (it is the dynamic describe
// self-answer, see Describer); its request dispatch is its Handle, not a
// declared type list.
type Declaration struct {
	// Name is the actor's short, human-readable identifier. Non-empty,
	// unique within a channel. Used in logging and as the describe identity
	// fallback.
	Name string

	// ActorID is the membership row this adapter owns. Every request envelope
	// whose audience[0] equals ActorID dispatches to this Module.Handle. MUST
	// refer to a pre-registered actor.
	ActorID actor.ActorID

	// Binding is the launch tri-class transport for this adapter (L1 §11.7).
	Binding actor.Binding
}

// Module is the gen_server-style callback contract for an actor (coagent's
// Erlang behaviour analogue). The host goroutine calls Declares first, then
// Init, then Handle on demand, finally Shutdown.
//
// SERIAL CONTRACT (v2, dismantle-spec §3 — the abstraction returning home):
// the host guarantees ALL Module callbacks (Declares / Init / Handle /
// Shutdown) are invoked SERIALLY by this actor's single cell goroutine. Because
// that goroutine is the sole accessor, a Module holds its logical state in plain
// fields — no mutex/atomic — and therefore MUST NOT read or write that state
// from any goroutine it spawns itself. External resources (HTTP client, watcher
// goroutine) may carry their own synchronisation, but a Module must not touch
// its cell state off the cell goroutine. There is no substrate self-send: a
// message reaches this actor only through the harness→fanout collaboration path
// (Receive), never by the Module injecting into its own mailbox.
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
	// pending — the adapter answers later via the ModuleContext seams
	// (Respond/Fail), or the caller-scoped closure times it out. A hard error
	// collapses to a receiver_internal_error terminal.
	Handle(ctx context.Context, env *message.Envelope) error
}
