// Package protocol is the protocol contract layer for coagent (launch+).
//
// It is PURE proto: pure types, closed-set vocabularies and pure functions
// that mirror the protocol specs (.dalek/pm/proto-*.md). It owns no state,
// performs no IO, takes no context.Context, declares no engine interfaces,
// and knows nothing about storage (no Scan/Value) or transport bindings
// (no HTTP status). All concrete backends, engines and bindings live
// outside protocol and depend on protocol — never the other way round.
//
// Subpackages (5):
//
//   - protocol/actor    — actor identity (ActorID), the actor Kind closed set,
//     Binding closed set, and the reserved-type closed sets.
//   - protocol/channel  — channel ID type (opaque stable string).
//   - protocol/message  — envelope schema (14 wire content fields, L0 §2.1;
//     sender flattened into sender.kind/sender.id), kind /
//     visibility closed sets, core-type table, and the terminal-failure reason
//     closed set (INVARIANT-10).
//   - protocol/resource — the opaque ResourceID, the passive object of the
//     access plane (second plane, subject/object closure); single-level
//     (no incarnation), bytes opaque.
//   - protocol/access   — the access invocation (Invocation), the Operation
//     closed set {create,read,write,set,delete}, the FailureReason closed set,
//     and the Grant operand for op=set. The second plane's subject to object
//     relation, off-log.
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
//     no module outside protocol may be imported (other protocol/ packages are
//     the only permitted ActOS edge — all layers depend on protocol, never the
//     reverse).
//
// Enforcement: archtest/protocol_purity_test.go (TestProtocolPurityAndDirection)
// mechanically enforces BOTH purity and dependency direction under
// `go test ./...` / `make lint`, so a PR adding a forbidden import — an external
// module, a reversed ActOS import (protocol importing runtime/lib/platform), or
// a state/IO/transport stdlib seam — turns the build red. A Go import path is a
// mandatory string literal, so the AST check has no computed-import escape
// hatch: this is a structural boundary, not a review convention.
package protocol
