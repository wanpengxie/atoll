// Package runtime is the reusable daemon-side execution substrate for
// coagent.
//
// Authoritative spec: launch-ticket notes §T3.
//
// runtime/ owns:
//
//   - sqlite-backed implementations of kernel interfaces (store)
//   - worker subprocess IPC and hosting primitives (workerhost)
//   - worker subprocess runtime — strict IPC, no sqlite (worker)
//   - long-pending scheduler, trigger gateway, workerhost leases
//
// Subpackages:
//
//   - runtime/store        — sqlite channel-local store (kernel/log,
//     kernel/actor, kernel/ledger implementations).
//   - runtime/workerhost   — worker subprocess pool + lease (5min) + IPC
//     server (length-prefixed JSON over pipes) +
//     fencing token + daemon_epoch enforcement.
//   - runtime/worker       — worker subprocess main loop. STRICT IPC ONLY.
//     No sqlite. Enforced by .go-arch-lint.yml.
//   - runtime/scheduler    — message deliver + long-pending Step 1/2/3 +
//     daemon-restart timer recover.
//
// Multiuser release assembly lives in framework/multiuser/runtime.
//
// Boundary rules (enforced by .go-arch-lint.yml at repo root):
//
//   - runtime/* MAY import kernel/* and runtime/* (self) and runtime_worker.
//   - runtime/worker MUST NOT import database/sql, mattn/go-sqlite3,
//     modernc.org/sqlite, runtime/store.
//   - kernel/* MUST NOT import runtime/*.
package runtime
