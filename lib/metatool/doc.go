// Package metatool defines the 7 LLM-facing meta tool specs (call_actor,
// list_actors, describe_actor, describe_type, await_result, abandon,
// list_pending) and their Execute functions.
//
// This package also owns the full LLM tool-result VOCABULARY: ResultValue,
// the ErrorCode closed set, ack shapes (AckDescriptor/AckResult), the
// tool-call spec (RequestSpec/WaitMode), and payload normalisation. The
// blocking call MECHANISM (futures + bounded Await) lives in lib/callkit. The actor.* self-answer contract —
// Describe / DescribeType / TypeMeta / Catalog response shapes — lives in
// lib/introspect, the ONE home of those shapes: this package only binds them
// to the LLM tool surface and never restates their fields.
//
// This package is pure Go + protocol/message + callkit + introspect —
// it never imports go-kimi or any LLM-binding types. The go-kimi
// binding layer lives in actors/agent, which imports metatool and
// materialises its types into go-kimi types.ToolResult values.
package metatool
