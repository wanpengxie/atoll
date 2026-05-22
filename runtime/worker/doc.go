// Package worker is the subprocess runtime for coagent workers.
//
// HARD BOUNDARY (codex review #9, enforced by .go-arch-lint.yml):
//
//   - NO import of database/sql
//   - NO import of github.com/mattn/go-sqlite3 / modernc.org/sqlite
//   - NO import of runtime/store
//
// All state mutation flows through IPC to the daemon. The daemon
// validates fencing_token + daemon_epoch before writing into
// channel-local sqlite.
//
// Authoritative spec: launch-ticket notes §T3.
//
// Files:
//
//   - runtime.go      — worker main loop (subprocess entry helper).
//     Wired by cmd/worker/main.go.
//   - kimi_bridge.go  — go-kimi wire event → v4 envelope mapper.
//   - tool_wrappers.go — embedded tool actor wrappers.
//   - ipc_client.go   — IPC client (write_message / reserve_ledger /
//     commit_ledger / heartbeat / shutdown_ack).
//   - fence_check.go  — verify daemon_epoch on every daemon ACK; on
//     mismatch the worker exits immediately (prevents
//     stale worker from writing after daemon restart).
package worker
