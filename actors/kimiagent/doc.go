// kimiagent is a THIN ADAPTER: go-kimi drives the cognition, a held
// metatool.Shell drives the channel calls, and this package only bridges
// the two — go-kimi wire frames out to envelopes, channel envelopes in to
// shell.Deliver. It owns no call mechanism of its own (correlation,
// sync/async, author#2 all live in the Shell) and no LLM brain of its own
// (that is go-kimi's Agent). The package-level positioning lives on
// bridge.go; this file documents only the wire↔envelope mapping the
// adapter performs.
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
