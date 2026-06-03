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
// Boundary rules (enforced by .go-arch-lint.yml at repo root):
//
//   - runtime/* MAY import kernel/* and runtime/* (self).
//   - kernel/* MUST NOT import runtime/*.
package runtime
