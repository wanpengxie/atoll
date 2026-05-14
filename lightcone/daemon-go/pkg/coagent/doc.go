// Package coagent implements the v4 agent-facing CLI library
// (L2 §3.1 — `emit / ask / answer`). It exposes:
//
//   - Run(cfg) — the unified entrypoint dispatching the three
//     subcommands. The `cmd/coagent` binary is a thin shell over Run;
//     worker processes (in-process callers) can also drive Run by
//     supplying their own Binding.
//   - Binding — the harness write surface; two implementations live in
//     this package (`NewDaemonRPCBinding` for HTTP and
//     `NewInWorkerBusBinding` for the in-process path described in
//     L2 §3.4.1).
//   - Subcommand helpers — flag parsing, envelope normalize (id / ts /
//     sender.id / correlation_id 3-tier fallback per L1 §2.2.1),
//     turn-context loading (env vars + ~/.coagent/turn-ctx.json
//     fallback per L2 §3.4.2), and `ask` audience three-branch
//     validation (L2 §3.2.2).
//
// Authoritative spec text:
//
//   - L2 §3.1     three subcommands ↔ ADT three kinds
//   - L2 §3.2.*   semantics of each subcommand
//   - L2 §3.3     full flag table
//   - L2 §3.3.1   --correlation-id three-state semantics
//   - L2 §3.4     two harness implementations
//   - L2 §3.4.1   binding routing rule
//   - L2 §3.4.2   trigger context auto-injection
//   - L2 §3.4.3   `coagent answer` implementation note
//   - L2 §3.6     binding-specific error mapping
//
// Out of scope here (per T12): real daemon endpoints (tests use
// httptest), channel-template install flow, scheduler bootstrap.
package coagent
