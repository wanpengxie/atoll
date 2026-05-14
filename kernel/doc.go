// Package kernel is the protocol contract layer for coagent (M1.5+).
//
// It contains pure types, interfaces and pure functions that mirror the
// v4 protocol spec (.dalek/pm/v4-layer{0,1,2,4}-spec.md). It owns no
// state, performs no IO, and depends on **no** external runtime libs
// (no sqlite, no HTTP, no LLM SDK, no go-kimi).
//
// Subpackages:
//
//   - kernel/message    — envelope schema, kind enum, reason closed sets,
//     canonical JSON hash (RFC 8785 + SHA-256).
//   - kernel/actor      — actor identity, sender struct, ActorRegistry
//     interface (in-memory contract, not bound to
//     sqlite).
//   - kernel/channel    — ChannelID type, ChannelRef (org_id?, channel_id)
//     for federation forward-compat.
//   - kernel/harness    — 9-step Harness chain interface, Step contract,
//     re-export of HarnessRejectReason.
//   - kernel/ledger     — ActionLedger reserve/commit interface, ledger key
//     derivation.
//   - kernel/log        — append-only MessageLog interface, Seq/Cursor.
//   - kernel/viewsync   — Pusher / Receiver / Resyncer contracts, frame
//     types (PushFrame / AckFrame / ResyncRequest /
//     ResyncResponse), cursor types — covers L1 §8.
//   - kernel/placement  — channel placement state machine + ACK-frame
//     field set + state transition matrix — covers
//     L2 §1.4.11.
//   - kernel/daemonbus  — daemonbus mux frame schema (control / viewsync
//     / device_transit) + epoch-bearing header —
//     covers L2 §9.
//   - kernel/adapter    — BindingKind tri-class enum, Module / Manager /
//     CorrelationTracker / ErrorPolicy / AdapterCtx /
//     DeviceTransit interfaces — covers L1 §11.7 +
//     L2 §8 framework contract.
//
// Layering rule (enforced by go-arch-lint in T2):
//
//   - kernel/** MUST NOT import: database/sql, net/http, gorilla/**,
//     gin-gonic/**, mattn/go-sqlite3, modernc.org/sqlite, go-kimi.
//   - All concrete backends (sqlite store, daemon RPC HTTP server,
//     daemonbus WS impl, etc.) live in runtime/ / server/ / adapters/
//     and depend on kernel/, never the other way round.
//
// Spec cross-reference: .dalek/pm/m1.5-tickets.md §T1 (origin ticket
// for the kernel layout) + §T2 (go-arch-lint enforce baseline).
package kernel
