// Package adapterhost is the driver增量 over lib/behavior: it hosts a
// behavior.Module as a real serial actor cell (runtime/actorrt), folding every
// entry point onto the cell's single goroutine so the Module's logical state
// needs no locks. This is where the adapters/framework.Manager god-object
// (~1763 LOC) collapses into one adapterActor per adapter (dismantle-spec §1).
//
// # Collapse blueprint (the施工 map for actor.go — port from adapters/framework)
//
// adapterActor (= actorrt.Actor impl; private fields, NO mutex/atomic on
// logical state — the cell goroutine is the sole owner):
//
//	module       behavior.Module          // the hosted callback module
//	declaration  behavior.Declaration     // static metadata (Declares())
//	correlation  behavior.CorrelationTracker // receiver-side pending (was boundModule.correlation)
//	readiness    actor.Readiness          // was actor_registry.ready_state + readinessMu — now plain field
//	live         <module-owned>           // proxy_facade.live etc. already plain (cell-serial)
//	chain        harness.Chain            // write path
//	policy/timer <cell-private>           // F3 timer map (was timerPolicy.mu) → cell-private; closure → caller-scoped (lib/behavior)
//
// Receive(env) dispatches SERIALLY (port of Manager.Dispatch manager.go:887):
//   - kind=request, type=actor.status   → respondActorStatus (self-answer, read own readiness/live)
//   - kind=request, type=actor.describe → respondActorDescribe (Declaration projection)
//   - kind=request, type∈declared       → reservePendingRequest + runHandle → module.Handle
//       * NO sticky-readiness gate (manager.go:932 — dispatch is dumb delivery;
//         not-ready actor self-answers receiver_unavailable; reachability is the
//         OUTCOME of send→terminal, never a stored gate). P15/P16.
//       * Handle error: ErrHandleDeferred → keep pending; else → policy.OnExternalError
//         emits receiver_internal_error terminal (manager.go:946-978).
//
// Out-of-band entry points fold onto the SAME cell (no off-cell module touch):
//   - device external callback (OnExternalCallbackFrame, manager.go:1255) →
//     runtime/actorrt Ask (SYNC ack: bad/dup/expired frame rejected with
//     permanent/retryable verdict handed back to transit — dismantle §2.5-A a)
//   - device lifecycle (OnRuntimeEvent, manager.go:1461) → actorrt Post (async)
//   - heartbeat tick → cell self-schedules a ticker → self.Deliver(tick) →
//     runHeartbeatOnce writes own readiness (manager.go:523 binding intervals)
//   - Stop() → module.Shutdown on the cell goroutine (manager.go:1617)
//
// installer contract (port of Manager.Install/installOne manager.go:219/231 +
// dismantle §2.5-A c): input module → output {actorID, declaration, actor impl,
// route metadata}; composition root (daemon/host) does Spawn + device route.
// The kernel/adapter.Manager runtime面 is ALREADY removed (deleted in v2 kernel
// purge); this package MUST NOT reintroduce a long-lived god-object.
//
// caller-side futures (Call/Await/Watch) stay a channel-level futureHub
// (lib/behavior/futurereg), NOT folded into the sender mailbox (deadlock —
// dismantle §2.5-B b).
//
// correlation reaper: runOneGCPass logic (manager.go:1564 RunGC) → cell
// self-scheduled tick (actor-local bounded reaper); NO Manager.RunGC goroutine.
//
// Dependencies: kernel + runtime/actorrt + runtime/harness + lib/behavior.
package adapterhost
