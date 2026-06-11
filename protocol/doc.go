// Package protocol is the protocol contract layer for coagent (launch+).
//
// It is PURE proto: pure types, closed-set vocabularies and pure functions
// that mirror the protocol specs (.dalek/pm/proto-*.md). It owns no state,
// performs no IO, takes no context.Context, declares no engine interfaces,
// and knows nothing about storage (no Scan/Value) or transport bindings
// (no HTTP status). All concrete backends, engines and bindings live
// outside protocol and depend on protocol — never the other way round.
//
// Subpackages (3):
//
//   - protocol/actor    — actor identity (ActorID), the actor Kind closed set,
//     Binding closed set, and the reserved-type closed sets.
//   - protocol/channel  — channel ID type (opaque stable string).
//   - protocol/message  — envelope schema (14 wire content fields, L0 §2.1;
//     sender flattened into sender.kind/sender.id), kind /
//     visibility closed sets, core-type table, and the terminal-failure reason
//     closed set (INVARIANT-10).
//
// What is NOT in protocol (and why):
//
//   - Stateful engine seams (the harness Chain/Step, the store contracts
//     Registry/RequestLookup/MessageLog) — they take context.Context
//     and are implemented by runtime/store; they live with their consumers in
//     runtime. (Go idiom: interface at the consumer.)
//   - Projections (actor membership Record/Registry) — derived read caches,
//     never a protocol (truth) model (truth-vs-projection).
//   - store-derived columns (seq, is_terminal) — derived/produced by the
//     store (a message-log ROW concern), never wire envelope content fields.
//   - harness reject + install reason vocabularies — the write/install
//     ENGINES' errno, co-evolving with their engines → runtime.
//   - reason→HTTP-status mapping — strerror, a binding concern; lives outside protocol.
//
// Layering rule (design intent — see NB on enforcement):
//
//   - protocol/** MUST NOT import anything outside the Go standard library, and
//     not even the stdlib seams that imply state/IO/transport: context,
//     database/sql, net/http, or any SQL/driver/transport package. Concretely
//     no module outside protocol may be imported.
//   - Manual acceptance check: `git grep '"context"' -- 'protocol/'` → 0 (greps
//     the import, not prose, so it cannot self-match this doc's "no context").
//
// TODO(lint-rebuild): NOT mechanically enforced right now. The old go-arch-lint
// config (which claimed to) is dead — make lint / CI never run it. The rule
// holds by review + the manual grep above. Re-mechanise protocol/ stdlib-purity
// in the deferred v2-topology lint rebuild.
package protocol
