// Package viewsync implements the daemon → server view sync push
// interface + the server-to-daemon Resync RPC per L1 §8 + L2 §8.1.4.
//
// M1.3 baseline scope (ticket M1.3-T15):
//
//   - push.go   — Pusher callable interface (HTTPPusher impl + FailureSink
//     fallback emitting system.event payload.kind=view_sync_failed
//     on transport / 4xx-5xx failure). Daemon local message store
//     remains source of truth — the Pusher never touches it.
//   - resync.go — Resync HTTP handler (daemon-side) + ResyncClient
//     (server-side caller) wiring the L1 §8.1.3 Resync API. Returns
//     channel envelopes ordered by messages.seq ASC since the
//     since_seq cursor (0 / omitted = full backfill).
//
// Out of scope for M1.3 (parked per L1 §8.1.2):
//
//   - Outbox persistence (push interface is callable so M1.x can replace
//     HTTPPusher with an enqueue+worker impl without rewriting callers).
//   - Retry / backoff (M1.x outbox layer owns it).
//   - Real Node server integration — tests use httptest doubles.
//
// Authoritative spec text:
//
//   - L1 §8       Daemon → Server View Sync 协议
//   - L1 §8.1.1   protocol baseline 接受 view sync 偶发丢失
//   - L1 §8.1.3   Resync API (server-to-daemon RPC)
//   - L1 §8.1.4   callable interface caveat
//   - L2 §3.6.1   binding-specific error mapping (HTTP)
package viewsync
