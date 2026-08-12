// Package metatool is the channel's actor-invocation surface (bash positioning).
// A channel is a set of actors; a client edge (an LLM brain, a UI, a gateway)
// uses them through three primitives — invoke (call_actor), collect
// (sync/async: await_result/list_pending/cancel), and discover (list_actors/
// describe_actor/describe_type). This is "how you call an actor in this
// channel", the universal entry — NOT any single caller's private wiring. job
// control (& / wait / jobs / kill %) is bash's, implemented once and shared by
// every program; correlation + sync/async is the substrate's, shared by every
// client edge. The LLM loop is its first (and currently only) client edge.
//
// One package, one purpose:
//
//   - the 7 client-edge meta tool specs (call_actor, list_actors,
//     describe_actor, describe_type, await_result, cancel, list_pending)
//     and their Execute functions (the binding onto the Exec face)
//   - the tool-result VOCABULARY: ResultValue, the ErrorCode closed set, ack
//     shapes (AckDescriptor/AckResult), the tool-call spec
//     (RequestSpec/WaitMode), and payload normalisation
//   - the Exec face (exec.go): the ONE adapter from a metatool RequestSpec onto
//     the substrate's out-station JobTable (call_actor/await_result/cancel/
//     list_pending — the cross-turn correlation account) plus a synchronous
//     sys.Call face (list_actors/describe — transient introspection queries).
//
// 期10 S5 collapse: the historical metatool.Shell (a private correlator holding
// its OWN author#2 timer (the since-拆删 behavior.Caller) alongside the engine's ledger — the
// "two historical fragments") is GONE. The seven tools drive lib/actorbase's
// JobTable directly — the SAME machine, moved house, not a second one. The
// subscribe-before-send correlator, the bounded-window await, the
// timeout-vs-buffered-final reconcile, and closure author#2 all live in the
// engine's callLedger now; this package only translates params into JobTable
// operations and renders the results.
//
// The actor.* self-answer contract — Describe / DescribeType / TypeMeta /
// Catalog response shapes — lives in lib/introspect, the ONE home of those
// shapes: this package only binds them to the LLM tool surface and never
// restates their fields.
//
// This package is pure Go + protocol/message + introspect + behavior +
// actorbase (the JobTable interface it drives) — it never imports an engine SDK
// or any LLM-SDK types. The engine binding layer lives in drivers/agents/base, which builds
// one Exec from the incarnation's Sys and wraps each tool into the engine's own
// tool surface.
package metatool
