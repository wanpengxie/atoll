// Package workerhost is the daemon-side host for worker subprocesses.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (worker lease model)
// + codex review #10 (lease vs heartbeat + daemon_epoch fencing)
// + .dalek/pm/m1.6-tickets.md §T1 (per-channel WorkerBridge).
//
// Files:
//
//   - pool.go    — worker process pool + quota (e.g. 32 concurrent leases).
//   - lease.go   — Acquire / Release / Renew (lease TTL = 5min); idle
//     timeout sweep.
//   - spawn.go   — exec.Cmd worker binary + bidirectional stdin/stdout
//     pipe.
//   - host.go    — daemon-side IPC server for one worker: handshake +
//     ack, fence enforcement, write_message / reserve_ledger /
//     commit_ledger handlers. Exposes Ready() + PushTrigger(ctx) so
//     the Bridge can gate the first KindTrigger frame on the
//     worker handshake completing, then wait for trigger ack/nack.
//   - fence.go   — fencing_token + daemon_epoch enforcement on every
//     IPC mutation. Mismatch → daemon rejects + sends fence_invalid
//     reply (worker exits per fence_check).
//   - bridge.go — per-channel lazy-spawn / reuse Bridge. Wired into
//     runtime/daemon.bootChannel via DaemonConfig.WorkerSpawner.
//     The Bridge owns one workerSession at a time; OnTrigger
//     spawns when there is no live worker, otherwise pushes the
//     envelope onto the existing IPC channel as a KindTrigger
//     frame. Crash recovery: Host.Serve exit → tombstone session,
//     release lease, next OnTrigger re-spawns.
package workerhost
