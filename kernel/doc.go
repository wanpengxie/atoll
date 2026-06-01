// Package kernel is the protocol contract layer for coagent (launch+).
//
// It contains pure types, interfaces and pure functions that mirror the
// current protocol specs (.dalek/pm/proto-layer*.md and impl-layer*.md).
// It owns no
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
//   - kernel/harness    — 9-step Harness chain interface and Step contract.
//   - kernel/ledger     — ActionLedger reserve/commit interface, ledger key
//     derivation.
//   - kernel/log        — append-only MessageLog interface, Seq/Cursor.
//   - kernel/fencing    — minimal write-fence primitives shared by channel
//     mutation paths.
//   - kernel/adapter    — Module / Manager / CorrelationTracker /
//     ErrorPolicy / AdapterCtx interfaces — covers L2 §8 framework
//     contract. Binding lives in kernel/actor.
//
// Layering rule (enforced by go-arch-lint in T2):
//
//   - kernel/** MUST NOT import: database/sql, net/http, gorilla/**,
//     gin-gonic/**, mattn/go-sqlite3, modernc.org/sqlite, go-kimi.
//   - All concrete backends and release-specific frameworks (sqlite store,
//     daemon RPC HTTP server, multiuser daemonbus, etc.) live outside
//     kernel/ and depend on kernel/, never the other way round.
//
// Spec cross-reference: launch-ticket notes §T1 (origin ticket
// for the kernel layout) + §T2 (go-arch-lint enforce baseline).
package kernel
