// Package trigger implements the v4 Trigger Gateway (L1 §5).
//
// After the Message-Write Harness (kernel/harness + runtime/harness) has
// validated and appended an envelope to the channel messages table, the
// trigger gateway answers a single question: "which actors should this
// daemon hand this envelope to next?"
//
// Authoritative spec references:
//
//   - L1 §5.1  Decision order (visibility → audience expand → sender/type
//     filter → self-trigger ban).
//   - L1 §5.3  Future-message dispatch path semantics (NotBefore > now
//     bypasses immediate trigger; scheduler resurfaces the row after
//     not_before <= now and the self-trigger ban then does NOT apply
//     because the scheduler is the upstream, not the original sender).
//   - L1 §5.2  Informative example table (covered by the test matrix).
//
// Files:
//
//   - resolve.go  — Resolve(ctx, env, registry, opts) []ActorID per §5.1.
//   - gateway.go  — Gateway combines Resolve with runtime/scheduler
//     Deliverer + the L1 §5.3 future-message skip; this is the seam
//     wired into the daemon's post-harness dispatch path.
package trigger
