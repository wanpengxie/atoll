// Package agent is the LLM bridge — it wraps go-kimi's Agent so the
// coagent worker can drive a real LLM (DeepSeek's anthropic-compat
// endpoint per the cvmax deploy plan) instead of the deterministic
// MockBridge used during e2e bring-up.
//
// Wire types → envelope mapping:
//
//	wire.TextDelta    → pure buffer (NO envelope emitted). Deltas are
//	                    accumulated locally; the final text is folded
//	                    into the single TurnEnd terminal envelope.
//	wire.TurnEnd      → exactly ONE agent.text envelope, visibility=public,
//	                    payload.text carries the full accumulated content,
//	                    payload.next_action stamped from TurnEnd.StopReason,
//	                    payload.stop_reason echoes the provider raw reason.
//	wire.ToolCallReq  → not emitted as envelope; future ticket can promote
//	                    into a dedicated tool.invocation type.
//	everything else   → dropped.
//
// Failure path: a single failed-terminal envelope is emitted from the
// LLM error classifier (see emitTerminalLLMError) carrying the full
// error description in payload.text + payload.reason bucket.
package kimiagent
