// Package worker is the subprocess runtime for coagent workers.
//
// HARD BOUNDARY (codex review #9, enforced by .go-arch-lint.yml):
//
//   - NO import of database/sql
//   - NO import of modernc.org/sqlite
//   - NO import of runtime/internal/store
//
// The worker holds NO truth: it EMITS its envelopes upward over IPC
// (KindEmit) to the daemon host, which forwards them to the server harness —
// the single channel-log writer (truth lives on server).
//
// Authoritative spec: runtime-construction-spec §1.10.
//
// Files:
//
//   - runtime.go      — worker main loop (subprocess entry helper).
//     Wired by cmd/worker/main.go.
//   - kimi_bridge.go  — go-kimi wire event → envelope mapper.
//   - tool_wrappers.go — embedded tool actor wrappers.
//   - ipc_client.go   — IPC client (emit / down / heartbeat / shutdown_ack).
//   - fence_check.go  — verify the worker-LEASE token on every daemon ACK; on
//     mismatch the worker exits immediately (prevents a stale/zombie worker
//     from acting after a daemon restart or reconnect).
package worker
