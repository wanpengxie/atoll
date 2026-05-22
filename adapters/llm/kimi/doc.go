// Package kimi is the M1.6-T7 phase-4 LLM bridge — it wraps go-kimi's
// Agent so the coagent worker can drive a real LLM (DeepSeek's
// anthropic-compat endpoint per the cvmax deploy plan) instead of the
// deterministic MockBridge used during M1.6 e2e bring-up.
//
// Why this lives under adapters/**:
//   - .go-arch-lint.yml forbids adapters/** from importing runtime/**,
//     which means we cannot implement runtime/worker.Bridge directly
//     here. Instead we expose a local Bridge interface that operates
//     on a lightweight IPCFacade abstraction (channel id / actor id /
//     trigger channel / WriteEnvelope func). cmd/worker (composition
//     root, allowed to import both adapters and runtime) writes a
//     ~30-line adapter that turns runtime/worker.IPCClient into our
//     IPCFacade and wraps the kimi.Bridge into a worker.Bridge.
//   - adapters/** is allowed anyVendorDeps:true so the go-kimi import
//     stays here without polluting the worker runtime package surface.
//
// Wire types → v4 envelope mapping (the "24 wire types" callout in the
// ticket description maps to ~23 const lines in go-kimi/pkg/kimi/wire/
// types.go). M1.6 scope is intentionally narrow + strictly single-response
// per proto-layer0 single-response semantics (each request gets at most
// ONE response envelope; streaming chunks are a transport-layer artifact
// and never leak into the protocol envelope layer):
//
//	wire.TextDelta    → pure buffer (NO envelope emitted). Deltas are
//	                    accumulated locally; the final text is folded
//	                    into the single TurnEnd terminal envelope.
//	wire.TurnEnd      → exactly ONE agent.text envelope, visibility=public,
//	                    payload.text carries the full accumulated content,
//	                    payload.next_action stamped from TurnEnd.StopReason,
//	                    payload.stop_reason echoes the provider raw reason.
//	wire.ToolCallReq  → not emitted as envelope in M1.6; future ticket
//	                    can promote into a dedicated tool.invocation type.
//	everything else   → dropped. Future expansion (M1.7+) can promote
//	                    more wire types into envelope traffic.
//
// Failure path: a single failed-terminal envelope is emitted from the
// LLM error classifier (see emitTerminalLLMError) carrying the full
// error description in payload.text + payload.reason bucket.
//
// Errors from go-kimi (wraps *kimierrors.LLMError when the provider
// is non-2xx) are classified into 5 reason buckets — rate_limit, auth,
// server, network, unknown — and emitted as a terminal agent.text
// envelope with payload.next_action="failed" and payload.reason
// carrying the bucket name. The worker process exits 0 after this so
// the daemon's worker supervisor sees a clean shutdown rather than a
// crash loop.
package kimi
