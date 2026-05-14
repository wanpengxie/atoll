// Package kernel is the v5 actor-message-channel protocol contract core.
//
// kernel is a top-level "first-class citizen" of the daemon-go module
// (m1.5-tickets §T2). Everything inside kernel/ MUST be:
//
//   - interface contracts (e.g. ActorRegistry, MessageLog, ViewSync)
//   - pure value types (Envelope, Sender, ChannelId, …)
//   - pure logic (canonical_hash, ledger_key derivation, …)
//
// kernel MUST NOT depend on any IO library:
//
//   - no database/sql / sqlite driver
//   - no net/http / gorilla / gin
//   - no go-kimi or any LLM-runtime
//   - no runtime/adapters/server packages (kernel sits ABOVE them)
//
// The 6 ownership invariants (.dalek/pm/m1.5-tickets.md §T2 table) are
// enforced by go-arch-lint via lightcone/daemon-go/.go-arch-lint.yaml.
//
// Sub-package layout (per §T2 deliverable):
//
//	kernel/actor       — Sender/ActorId types + ActorRegistry interface
//	kernel/message     — Envelope/Kind/Reason types + canonical hash
//	kernel/channel     — ChannelId / ChannelRef
//	kernel/harness     — 9-step Message-Write Harness chain abstraction
//	kernel/ledger      — ActionLedger reserve/commit interface + ledger_key
//	kernel/log         — MessageLog append-only interface + seq cursor
//	kernel/viewsync    — daemon-push + server-ack + Resync contracts
//	kernel/adapter     — Binding/Module/Manager + correlation/policy/ctx
//	kernel/placement   — placement state machine + control protocol types
//	kernel/daemonbus   — daemonbus mux frame schema (T1.5)
//
// Concrete backends (sqlite store, HTTP server, worker process, etc.)
// live OUTSIDE kernel — under runtime/ (T3) and adapters/ (T4/T6).
package kernel
