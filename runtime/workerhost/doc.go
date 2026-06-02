// Package workerhost is the daemon-side host for worker subprocesses.
//
// Authoritative spec: runtime-construction-spec §1.9 (v2 worker-lease model:
// truth on server, daemon = attached compute, worker EMITS upward).
//
// Files:
//
//   - pool.go    — worker process pool + quota (e.g. 32 concurrent leases).
//   - lease.go   — Acquire / Release (lease TTL = 5min). VOLATILE in-memory
//     instance fence; no sqlite worker_locks row.
//   - spawn.go   — exec.Cmd worker binary + bidirectional stdin/stdout
//     pipe.
//   - host.go    — daemon-side IPC server for one worker: handshake +
//     ack, worker-LEASE fence check, and the KindEmit / KindDown uplink
//     handlers (worker emits an envelope upward to the server harness — the
//     single writer — or signals actor/worker death). Exposes Ready() +
//     PushTrigger(ctx) so the Bridge can gate the first KindTrigger frame on
//     the worker handshake completing, then wait for processing ack/nack.
//   - fence.go   — worker-LEASE token check on every uplink frame. Mismatch
//     → daemon rejects + sends fence_invalid reply (worker exits). v2 single
//     opaque token, not the v1 fencing_token/daemon_epoch pair.
//   - bridge.go — per-channel lazy-spawn / reuse Bridge. The Bridge owns one
//     workerSession at a time; OnTrigger spawns when there is no live worker,
//     otherwise pushes the envelope onto the existing IPC channel as a
//     KindTrigger frame. Crash recovery: Host.Serve exit → tombstone session,
//     release lease, next OnTrigger re-spawns.
package workerhost
