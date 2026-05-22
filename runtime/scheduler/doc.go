// Package scheduler handles long-pending message scheduling and daemon
// restart timer recovery.
//
// Authoritative spec: launch-ticket notes §T3 + L1 §5.3 / §6.4 /
// L2 §3.7.
//
// Files:
//
//   - deliver.go — Dispatch envelope to per-actor handlers.
//   - timer.go   — Long-pending Step 1 / Step 2 / Step 3 fallback timer.
//     1s scan period (per L2 §3.7).
//   - recover.go — Daemon restart timer recovery: rescan pending request
//     rows + arm in-memory timers.
//
// Scheduler vs. cron — A7 axiom (no schedule entity):
//
// The L1 axiom set (A7) deliberately refuses a separate `schedule_cron`
// table: every recurring trigger is just a message whose `not_before`
// points at the future tick. A channel agent that wants to "wake up
// every Monday at 9am" emits, on each wake, a fresh `agent.text` event
// targeting itself with `not_before = next_monday_9am`. The §5.3 future-
// message drain in daemon.scanLongPending picks the row up the moment
// `not_before <= now` and runs the same trigger.Gateway.Dispatch fan-out
// as a regular write — no extra scheduling primitive, no parallel
// state, no separate restart path. Persistence guarantees the wake
// survives daemon crashes (the row sits in messages.sqlite with
// delivered_at IS NULL until the scheduler drains it).
//
// Timezone-aware cron parsing, fan-out across multiple agents, and
// catch-up policy after long downtime are intentionally out of scope
// — agents express any of these by computing the next not_before
// themselves and emitting accordingly. The M1.6-T5 prompt docs cover
// the recommended idiom.
package scheduler
