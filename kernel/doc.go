// Package kernel is the protocol contract layer for coagent (launch+).
//
// It is PURE proto: pure types, closed-set vocabularies and pure functions
// that mirror the protocol specs (.dalek/pm/proto-*.md). It owns no state,
// performs no IO, takes no context.Context, declares no engine interfaces,
// and knows nothing about storage (no Scan/Value) or transport bindings
// (no HTTP status). All concrete backends, engines and bindings live
// outside kernel and depend on kernel — never the other way round.
//
// Subpackages (3):
//
//   - kernel/actor    — actor identity (ActorID), the actor Kind closed set,
//     Binding closed set, and the reserved-type closed sets.
//   - kernel/channel  — channel ID type + Ref (federation forward-compat).
//   - kernel/message  — envelope schema (17 content+metadata fields), kind /
//     visibility closed sets, core-type table, and the terminal-failure reason
//     closed set (INVARIANT-10).
//
// What is NOT in kernel (and why):
//
//   - Stateful engine seams (the harness Chain/Step, the store contracts
//     Registry/Cursors/RequestLookup/MessageLog) — they take context.Context
//     and are implemented by runtime/store; they live with their consumers in
//     runtime. (Go idiom: interface at the consumer.)
//   - Projections (actor membership Record/Registry) — derived read caches, a
//     runtime/server facility, never a kernel model (truth-vs-projection).
//   - store-derived envelope columns (seq, is_terminal) —
//     produced by the store, not part of the 17 protocol fields.
//   - harness reject + install reason vocabularies — the write/install
//     ENGINES' errno, co-evolving with their engines → runtime.
//   - reason→HTTP-status mapping — strerror, a binding concern → server/gateway.
//
// Layering rule (enforced by go-arch-lint in T2):
//
//   - kernel/** MUST NOT import: context, database/sql, net/http, gorilla/**,
//     gin-gonic/**, mattn/go-sqlite3, modernc.org/sqlite, go-kimi, or any
//     other kernel-external module.
//   - Acceptance red line: `git grep context.Context kernel/` → 0.
package kernel
