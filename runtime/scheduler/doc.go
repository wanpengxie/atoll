// Package scheduler handles long-pending message scheduling and daemon
// restart timer recovery.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 + L1 §6 / L2 §3.7.
//
// Files:
//
//   - deliver.go — Dispatch envelope to per-actor handlers.
//   - timer.go   — Long-pending Step 1 / Step 2 / Step 3 fallback timer.
//     1s scan period (per L2 §3.7).
//   - recover.go — Daemon restart timer recovery: rescan pending request
//     rows + arm in-memory timers.
package scheduler
