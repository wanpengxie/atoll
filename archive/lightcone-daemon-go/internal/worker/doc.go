// Package worker hosts the v4 worker runtime: a go-kimi Agent instance
// plus the v4 ABI adapter that turns each agent loop event into a
// channel message (M1.3 ticket T10, L2 §3.9 normative).
//
// Layout:
//
//   - turn_ctx.go    — TurnCtx struct + flag binding + env overlay +
//     ~/.coagent/turn-ctx.json writer (L2 §3.4.2 + §3.9.3 spawn
//     protocol). The on-disk schema matches pkg/coagent.turnCtxFile so
//     the coagent CLI fallback path reads exactly what the worker
//     writes.
//
//   - wire_bridge.go — WireBridge implements go-kimi's wire.Emitter,
//     translating each of the 24 wire message types into a v4 envelope
//     per the L2 §3.9.5 mapping table (turn_begin / text_delta /
//     turn_end / step_* / status / notification / subagent / compaction /
//     mcp_loading / approval / question — tool_call_* swallowed because
//     T11 V4ize wraps those separately). Text deltas accumulate per
//     turn_id and turn_end emits one public agent.text with the full
//     reply. Envelope ids derive from a canonical hash so replay
//     dedupes through harness step 0.5.
//
//   - agent.go       — NewAgent(AgentConfig) wraps kimi.NewAgent with
//     the v4-specific injection (WireEmitter, AdditionalTools,
//     DisableStandardSandboxTools) + a minimal kimi.config.Config
//     (no MCP, no Moonshot fetch/search). AdditionalTools is the T11
//     v4-wrapped tool slot — empty by default in M1.3 baseline.
//
//   - heartbeat.go   — RunHeartbeat ticks at LeaseTTL/2 calling
//     supervisor.Heartbeat. ErrFencingStale / ErrLockMissing cancel the
//     runtime ctx so a stolen worker self-destructs without emitting
//     further side effects (the L2 §1.4.9 spawn protocol contract).
//
//   - runtime.go     — Run(ctx, RuntimeConfig) is the lifecycle
//     orchestrator cmd/worker/main.go calls: validate TurnCtx → write
//     turn-ctx.json → open channel sqlite → confirm actor row → build
//     harness Deps → build WireBridge wired to harness.InWorkerBus →
//     build kimi.Agent → spawn heartbeat goroutine → drive a turn (or
//     skip via SkipAgentRun) → cancel siblings → release worker_locks.
//
//   - spawner.go     — ExecSpawner implements supervisor.Spawner via
//     exec.Cmd. Spawn turns SpawnContext into argv flags + env vars
//     and starts the worker binary in its own process group; Kill
//     SIGKILLs the group so forked coagent CLI helpers are cleaned
//     up too.
//
// The cmd/worker/main.go entrypoint glues these together: flag parse →
// TurnCtx → Run → structured exit code.
//
// Out-of-scope items (covered by other tickets):
//
//   - T11 V4ize tool actor wrapping (passed via AdditionalTools).
//   - T13+ real LLM provider selection (M1.3 worker uses the in-process
//     echo provider per ticket spec — real LLM calls forbidden during
//     PM-unattended phase).
//   - Backlog → prompt context injection (L2 §4 minimal injection
//     policy lands separately once channel templates ship).
//
// Spec references: L2 §3.9 (worker runtime contract), §1.4.9 / §1.4.10
// (worker_locks + supervisor), §3.4.2 (turn-ctx propagation), §3.9.5
// (wire event bridge mapping table); M1.3 ticket §T10.
package worker
