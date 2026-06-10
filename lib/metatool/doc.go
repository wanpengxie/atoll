// Package metatool is the LLM ADAPTER onto the one actor face. A channel has
// exactly one way to participate — as an actor, through lib/behavior — and
// this package translates the LLM's tool-call idiom onto it (down: tool call
// → envelope; up: terminal/response → model-consumable result), exactly as
// the UI/gateway adapts a human brain onto the same face. All tools are
// actors and call_actor is the single entrance, so everything behind that
// entrance lives here — one package, one purpose:
//
//   - the 7 LLM-facing meta tool specs (call_actor, list_actors,
//     describe_actor, describe_type, await_result, abandon, list_pending)
//     and their Execute functions (the binding)
//   - the tool-result VOCABULARY: ResultValue, the ErrorCode closed set, ack
//     shapes (AckDescriptor/AckResult), the tool-call spec
//     (RequestSpec/WaitMode), and payload normalisation
//   - the futures MECHANISM (Client/RequestCorrelator: subscribe-before-send
//     plus a bounded blocking Await) that implements the async tool semantics
//     (call_actor fast-path, await_result, abandon, list_pending)
//
// Boundary axiom (lib-reshape spec §2.7): anything that can block-await is by
// definition NOT an actor — the futures half therefore never merges into
// lib/behavior (the actor-side call face is behavior.BuildRequest +
// behavior.Caller, closure author#2). It serves the LLM loop, which is a
// CLIENT EDGE; at the first-class async refactor the futures half migrates
// into the agent as private internals behind a non-blocking Receive — sync is
// an EXPERIENCE for the model's trained distribution, async is the STRUCTURE.
//
// The actor.* self-answer contract — Describe / DescribeType / TypeMeta /
// Catalog response shapes — lives in lib/introspect, the ONE home of those
// shapes: this package only binds them to the LLM tool surface and never
// restates their fields.
//
// This package is pure Go + protocol/message + introspect — it never imports
// go-kimi or any LLM-SDK types. The go-kimi binding layer lives in
// actors/agent, which imports metatool and materialises its types into
// go-kimi types.ToolResult values.
package metatool
