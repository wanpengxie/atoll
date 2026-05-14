// Package scheduler hosts daemon-level scheduling for the long-pending
// fallback emitter (L2 §3.7 / L1 §6.4). One Scheduler per channel sqlite
// scans every 1 s for pending request rows that have run past their
// expires_at or whose receiver has gone missing, then writes a deterministic
// system terminal response through the harness 9-step chain so the original
// request chain closes.
//
// Future-message reactivation lives in internal/trigger/future_scheduler.go;
// adapter F3 timer for tool actors lives in the adapter framework. The
// long-pending scheduler is the message-layer fallback that L1 §6.4 calls
// out for agent / system / human / deregistered receivers.
//
// Authoritative spec references:
//
//   - L1 §6.4   long-pending scheduler contract
//   - L2 §3.7   three-step scan SQL + fallback envelope template
//   - L1 §10.2  harness 9-step Write chain (the emit path)
package scheduler
