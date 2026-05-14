// Package supervisor manages worker lease lifecycles for a daemon.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3.
//
// Lease vs heartbeat (codex review #10):
//
//   - Lease: daemon-internal turn-level resource grant. TTL = 5min.
//     Released on turn done or TTL expiry.
//   - Heartbeat: worker → daemon liveness probe. 30s cadence (v4
//     baseline).
//
// Files:
//
//   - lease.go     — Lease lifecycle (acquire / release / sweep).
//   - spawn.go     — Spawner interface (workerhost provides impl).
//   - lifecycle.go — actor start / stop / restart (channel-bound).
package supervisor
