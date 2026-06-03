// Package harness is the runtime-side implementation of the Message-Write
// Harness — the validation chain every channel-truth write MUST go through. It
// is the channel's single writer; the server write-loop drives it.
//
// Concrete steps each implement the Step contract; the Chain composes them in
// stable StepID order with a short-circuit on the first reject. Implementations
// live in step_*.go siblings of chain.go.
//
// Authoritative spec:
//
//   - L1 §10.2  Harness 校验链
//   - L1 §10.2.1 Authoritative pseudocode
//   - L1 §10.3.1 Closed harness_reject_reason set
//   - L2 §1.4.5  Engine append ACL (harness is the only principal)
//
// **Not in scope of this package**:
//
//   - Post-append delivery to actor mailboxes. The harness only validates +
//     appends truth; delivering an appended envelope into the audience's
//     mailboxes (and reporting per-audience Outcome / collapsing an
//     undeliverable request to receiver_unavailable) is runtime/actorrt's
//     Deliver — invoked by the server write-loop, not a separate package.
//   - Actor hosting + transport (in-process cell / connect-in port) —
//     runtime/actorrt and the daemon host, not the harness.
package harness
