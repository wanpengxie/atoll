// Package runtime is the reusable daemon-side execution substrate for
// coagent.
//
// Authoritative spec: launch-ticket notes §T3.
//
// runtime/ owns (v2 BEAM-engine layer; imports only kernel):
//
//   - storespec        — kernel-only leaf contracts (store ports).
//   - internal/store   — sqlite implementations of those contracts.
//   - harness          — the 9-step Message-Write chain (single writer).
//   - actorrt          — actor cells / mailboxes / supervisor (process engine).
//   - workerhost       — worker subprocess pool + lease + IPC server.
//   - worker           — worker subprocess runtime — strict IPC, no sqlite.
//   - trigger          — post-harness fanout gateway + client-push view.
//   - scheduler        — Timer mechanism.
//   - ipc              — daemon↔worker length-prefixed JSON wire protocol.
//
// Subpackages:
//
//   - runtime/storespec     — storespec.* interfaces (kernel-only leaf).
//   - runtime/internal/store — sqlite impls of storespec; the raw *sql.DB is
//     confined here (Go internal/), exposed only as storespec interfaces via
//     OpenChannel → ChannelStores.
//   - runtime/workerhost    — worker subprocess pool + lease (5min, volatile)
//   - IPC server (length-prefixed JSON over pipes) + worker-LEASE fence.
//   - runtime/worker        — worker subprocess main loop. STRICT IPC ONLY.
//     No sqlite. Emits envelopes UPWARD to the server harness (truth on
//     server). Enforced by .go-arch-lint.yml.
//   - runtime/scheduler     — Timer mechanism (the long-pending scan
//     implementation moves to server channelhost; truth lives there).
//
// Boundary rules (enforced by .go-arch-lint.yml at repo root):
//
//   - runtime/* MAY import kernel/* and runtime/* (self).
//   - runtime/worker MUST NOT import database/sql, modernc.org/sqlite,
//     runtime/internal/store.
//   - kernel/* MUST NOT import runtime/*.
package runtime
