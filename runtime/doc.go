// Package runtime is the reusable execution substrate for coagent — the v2
// BEAM-style engine layer. It imports only kernel.
//
// runtime/ owns:
//
//   - storespec        — kernel-only leaf contracts (store ports).
//   - internal/store   — sqlite implementations of those contracts; the raw
//     *sql.DB is confined here (Go internal/), exposed only as storespec
//     interfaces via OpenChannel → ChannelStores.
//   - harness          — the 9-step Message-Write chain; the single writer of
//     channel truth.
//   - actorrt          — actor hosts by transport distance: cell (in-proc,
//     mailbox=chan) + port (out-of-proc, mailbox=connection), plus the
//     supervisor that materialises receiver_unavailable on death.
//   - ipc              — the port wire protocol (length-prefixed frames over a
//     connection: handshake / handshake_ack / deliver / emit / down).
//
// Boundary rules (design intent — see NB on enforcement):
//
//   - runtime/* MAY import protocol/* and runtime/* (self).
//   - protocol/* MUST NOT import runtime/*.
//
// TODO(lint-rebuild): this layering is NOT mechanically enforced right now. The
// old .go-arch-lint.yml (which claimed to) is dead config — make lint / CI never
// run it (lint = go vet + archtest); archtest guards only specific shapes
// (platform dependency direction, contract-shape containment, package-doc
// convention), NOT this inter-layer matrix. Re-mechanise "protocol/* must not
// import runtime/*" in the deferred v2-topology lint rebuild. Until then the rule
// holds by review, not by tooling.
package runtime
