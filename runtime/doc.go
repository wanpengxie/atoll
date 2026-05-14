// Package runtime is the daemon-side execution engine for coagent (M1.5+).
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3.
//
// runtime/ owns:
//
//   - sqlite-backed implementations of kernel interfaces (store)
//   - channel placement state machine — daemon side (lifecycle)
//   - WS/IPC fan-out to server (transit) and worker subprocess (workerhost)
//   - worker subprocess runtime — strict IPC, no sqlite (worker)
//   - long-pending scheduler, bootstrap saga, lease supervisor
//
// Subpackages:
//
//   - runtime/store        — sqlite channel-local store (kernel/log,
//     kernel/actor, kernel/ledger, kernel/placement
//     implementations) + view_sync_outbox +
//     channel_lock.
//   - runtime/transit      — daemonbus client (push / ack / resync /
//     control / device_transit), persistent outbox,
//     ack-driven cursor, gap resync.
//   - runtime/lifecycle    — T1.4 channel placement + T1.6 phase 1/2/3/4
//     startup barriers + fencing enforcement.
//   - runtime/workerhost   — worker subprocess pool + lease (5min) + IPC
//     server (length-prefixed JSON over pipes) +
//     fencing token + daemon_epoch enforcement.
//   - runtime/worker       — worker subprocess main loop. STRICT IPC ONLY.
//     No sqlite. Enforced by .go-arch-lint.yaml.
//   - runtime/supervisor   — lease lifecycle (vs heartbeat 30s) + spawner
//     abstraction.
//   - runtime/scheduler    — message deliver + long-pending Step 1/2/3 +
//     daemon-restart timer recover.
//   - runtime/bootstrap    — 9-step channel bootstrap saga + crash
//     reconcile.
//
// Boundary rules (enforced by .go-arch-lint.yaml at repo root):
//
//   - runtime/* MAY import kernel/* and runtime/* (self) and runtime_worker.
//   - runtime/worker MUST NOT import database/sql, mattn/go-sqlite3,
//     modernc.org/sqlite, runtime/store.
//   - kernel/* MUST NOT import runtime/*.
package runtime
