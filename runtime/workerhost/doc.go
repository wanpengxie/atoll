// Package workerhost is the daemon-side host for worker subprocesses.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (worker lease model)
// + codex review #10 (lease vs heartbeat + daemon_epoch fencing).
//
// Files:
//
//   - pool.go   — worker process pool + quota (e.g. 32 concurrent leases).
//   - lease.go  — Acquire / Release / Renew (lease TTL = 5min); idle
//     timeout sweep.
//   - spawn.go  — exec.Cmd worker binary + bidirectional stdin/stdout
//     pipe.
//   - ipc.go    — IPC protocol: length-prefixed JSON frames over the
//     spawn pipes. Methods: write_message / reserve_ledger
//     / commit_ledger / heartbeat / shutdown.
//   - fence.go  — fencing_token + daemon_epoch enforcement on every IPC
//     mutation. Mismatch → daemon rejects + sends fence_invalid
//     reply (worker exits per fence_check).
package workerhost
