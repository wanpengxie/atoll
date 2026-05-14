// Package trigger implements the v4 trigger gateway per L1 §5
// (.dalek/pm/v4-layer1-spec.md §5) plus the future-message scheduler
// from L1 §5.3 and the subscription registry hook from L1 §5.4.
//
// Three pieces live here:
//
//   - gateway.go (Gateway):
//     The L1 §5.1 decision algorithm. `Dispatch(ctx, env, upstream)`
//     returns the list of actor ids that should be triggered for env.
//     The decision order is visibility filter → audience expansion →
//     sender/type + self-trigger filter. `upstream` carries L1 §5.3
//     "dispatch-path 语义" — direct harness writes pass env.Sender.ID,
//     scheduler / fallback paths pass "" so the original sender is
//     not filtered as self.
//
//   - subscription.go (Registry):
//     The L1 §5.4 in-memory subscription store. Adapter framework F6
//     `init()` calls Register at daemon boot; the gateway invokes
//     Match to surface subscribers for visibility=system events that
//     would otherwise receive no triggers. The protocol baseline
//     matches on the (type, kind?, visibility?) 3-tuple; payload-level
//     matching is deferred to L1.1.
//
//   - future_scheduler.go (FutureScheduler):
//     The L1 §5.3 periodic scanner. Once per cfg.Period it queries
//     `not_before<=now AND delivered_at IS NULL AND
//     delivery_failed_at IS NULL`, marks rows whose expires_at has
//     elapsed as expired, and forwards live rows to the gateway with
//     upstream="" (scheduler dispatch-path semantics). Crash-recovery
//     is automatic because the scheduler holds no in-memory state —
//     a restarted daemon's next Tick re-discovers pending rows from
//     sqlite.
//
// Out of scope (handled by other M1.3 tickets):
//
//   - harness.Dispatcher wiring (T8 keeps the harness's NoopDispatcher
//     in place — the gateway is consumed by future schedulers /
//     supervisors today, with a follow-up to wire the synchronous
//     dispatch path).
//   - Supervisor wake-up of triggered actors — supervisor.Loop owns
//     the worker lifecycle; T8 only returns "who to trigger".
//   - Long-pending request fallback — T9 (scheduler package) handles
//     `expires_at < now AND no terminal response`; T8's expires_at
//     branch only covers future messages.
//   - View sync fan-out — T15.
//   - Adapter framework F6 integration with Registry — the registry's
//     API is exposed here; F6 itself ships in the adapter framework
//     ticket.
//
// Authoritative spec references:
//
//   - .dalek/pm/v4-layer1-spec.md §5 (decision matrix, future message,
//     subscription)
//   - .dalek/pm/m1.3-v4-foundation-spec.md §4.5 (trigger gateway
//     specification + acceptance criteria)
//   - .dalek/pm/m1.3-tickets.md §T8 (ticket scope)
package trigger
