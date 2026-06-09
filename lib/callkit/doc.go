// Package callkit is the call workflow kit: send a request, wait for a reply,
// manage pending futures, and report results. It sits above the behavior
// primitives (which are pure, transport-free) and below the metatool layer
// (which defines the 7 LLM-facing tool specs).
//
// One import covers: RequestCorrelator, IPCCaller, RequestSpec, WaitMode,
// ErrorCode closed set, and payload normalisation helpers.
package callkit
