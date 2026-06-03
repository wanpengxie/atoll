// Package adapterhost is the driver增量 over lib/behavior: it hosts a
// behavior.Module as a real serial actor cell (runtime/actorrt), folding every
// entry point onto the cell's single goroutine so the Module's logical state
// needs no locks. One adapterActor per adapter — the collapse of the former
// adapters/framework.Manager god-object into per-adapter cells (dismantle-spec
// §1); there is NO long-lived god-object and this package must not reintroduce
// one.
//
// adapterActor (actorrt.Actor; all logical state in PLAIN fields, no
// mutex/atomic — the cell goroutine is the sole owner):
//   - module/declaration  — the hosted callback module + its static metadata
//   - inflight            — cached request envelopes, doubling as the lock-free
//     pending tracker (a compute cell builds responses without a local truth
//     lookup; the reaper bounds this map)
//   - chain               — the harness write path (runtime/harness.Writer)
//
// Receive dispatches one envelope SERIALLY:
//   - kind=request, type=actor.describe → self-answer (identity + live API
//     surface via the optional introspect.Describer)
//   - kind=request, other type          → reserve pending → module.Handle; a
//     non-deferred Handle error collapses to a receiver_internal_error terminal
//   - NO actor.status self-answer: "serviceable right now" is the OUTCOME of
//     send→terminal, not a queryable gate (P15/P16). A non-trivial domain
//     Statuser is added additively when an adapter needs it.
//
// Inbound external I/O (device/webhook results) is the adapter's OWN business,
// not a framework callback: the adapter's reader folds results back by
// self-delivering an envelope onto its cell (actorrt.ActorContext.Deliver),
// handled in the same serial Receive. The only framework-driven out-of-band
// entry is the self-scheduled tick: the cell delivers a tick to itself, and
// Receive runs the in-flight reaper (bounds inflight memory) on the cell
// goroutine. No god-object GC goroutine.
//
// Install is a pure install-time factory (installer.go): validate the
// declaration, verify the handler actor's membership/binding via the registry,
// construct the adapterActor cell for the composition root to Spawn. type
// vocabulary is NOT published here — type_registry left the substrate and the
// type catalog is a domain concern deferred to daemon.
//
// The respond seam (Respond/Fail/EmitEvent + ModuleContext construction) lives
// HERE, not in lib/behavior: it writes terminals through runtime/harness,
// whereas lib/behavior stays pure-kernel so adapters depending on it don't
// transitively pull runtime (仲裁-2). The caller-side future hub
// (lib/behavior/futurereg) is NOT an adapter-cell concern — it belongs to
// non-actor callers (SDK clients, worker subprocesses) that block-await off the
// cell goroutine; an adapter cell calling out emits a request + handles the
// response in a later Receive (async continuation), never block-awaits (that
// would deadlock its own goroutine).
//
// Dependencies: kernel + runtime/actorrt + runtime/harness + runtime/storespec
// + lib/behavior.
package adapterhost
