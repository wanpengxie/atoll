// Package metatool is the channel's actor-invocation SHELL (bash positioning).
// A channel is a set of actors; a client edge (an LLM brain, a UI, a gateway)
// uses them through three primitives — invoke (call_actor), collect
// (sync/async: await_result/list_pending/abandon), and discover (list_actors/
// describe_actor/describe_type). This is "how you call an actor in this
// channel", the universal entry — NOT any single caller's private wiring. job
// control (& / wait / jobs / kill %) is bash's, implemented once and shared by
// every program; correlation + sync/async is this shell's, shared by every
// client edge. The LLM loop is its first (and currently only) client edge.
//
// One package, one purpose:
//
//   - the 7 client-edge meta tool specs (call_actor, list_actors,
//     describe_actor, describe_type, await_result, abandon, list_pending)
//     and their Execute functions (the binding onto Shell)
//   - the tool-result VOCABULARY: ResultValue, the ErrorCode closed set, ack
//     shapes (AckDescriptor/AckResult), the tool-call spec
//     (RequestSpec/WaitMode), and payload normalisation
//   - the Shell itself: the in-flight correlator (subscribe-before-send +
//     bounded blocking Await + the timeout-vs-buffered-final reconcile) PLUS
//     the complete outbound request lifecycle — build (behavior.BuildRequest),
//     Arm closure author#2 (behavior.Caller), emit through the harness write
//     door, and Match author#2 on Deliver. The Shell HOLDS and DRIVES the
//     behavior.Caller primitive; it does not re-implement it.
//
// Boundary axiom (lib-reshape spec §2.7): anything that can block-await is by
// definition NOT an actor — the Shell's Await therefore never merges into
// lib/behavior (the actor-side call face stays behavior.BuildRequest +
// behavior.Caller, closure author#2). The Shell's Call blocks the CLIENT-EDGE
// goroutine (the LLM loop), never a mailbox; Receive stays non-blocking. §2.7
// only barred the correlator from behavior — its correct home is this shell, a
// lib, not the agent. sync is an EXPERIENCE for the model's trained
// distribution, async is the STRUCTURE.
//
// The actor.* self-answer contract — Describe / DescribeType / TypeMeta /
// Catalog response shapes — lives in lib/introspect, the ONE home of those
// shapes: this package only binds them to the LLM tool surface and never
// restates their fields.
//
// This package is pure Go + protocol/message + introspect + behavior/harness
// (the write door + author#2 it drives) — it never imports go-kimi or any
// LLM-SDK types. The go-kimi binding layer lives in actors/agent, which holds
// one Shell, feeds responses into Shell.Deliver from its non-blocking Receive,
// and materialises ResultValue into go-kimi types.ToolResult values.
package metatool
