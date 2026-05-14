// Package bootstrap is the daemon-side 9-step channel bootstrap saga +
// crash reconcile.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 + L2 §3.6
// (bootstrap_registry).
//
// Files:
//
//   - saga.go      — 9-step ChannelCreate orchestrator.
//   - reconcile.go — crash recovery: scan bootstrap_registry for
//     in_progress rows, roll back / resume.
package bootstrap
