// Package metatool defines the 7 LLM-facing meta tool specs (call_actor,
// list_actors, describe_actor, describe_type, await_result, abandon,
// list_pending) and their Execute functions.
//
// The call workflow primitives (correlator, caller, error codes, payload
// normalisation) live in lib/callkit. Rich catalog metadata types
// (ChannelContext, ActorInfo, TypeInfo) live in lib/introspect.
//
// This package is pure Go + protocol/message + callkit + introspect —
// it never imports go-kimi or any LLM-binding types. The go-kimi
// binding layer lives in actors/agent, which imports metatool and
// materialises its types into go-kimi types.ToolResult values.
package metatool
