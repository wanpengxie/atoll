// Package lifecycle implements the daemon-side channel lifecycle:
// placement state machine (T1.4) + startup phase barriers (T1.6).
//
// Authoritative spec: launch-ticket notes §T3 + L2 §1.4.11
// (channel_placements) + L2 §3.6.* (daemon startup phases).
//
// Files:
//
//   - fencing.go — fencing_token + daemon_epoch validators; every write
//     into channel-local sqlite passes through this gate.
//   - create.go  — handles control.create_channel: 4-state local decision
//     (no _lock / fencing < / fencing == / fencing >) +
//     ACK with complete field set (owner_epoch + fencing
//     token + daemon_id + daemon_epoch + create_request_id).
//   - boot.go    — Phase 1/2/3/4 sequencer (load local -> connect server
//     reclaim -> recover scheduler/outbox/workerhost ->
//     accept new control frames).
//   - unload.go  — idle / orphan / stale unload paths.
package lifecycle
