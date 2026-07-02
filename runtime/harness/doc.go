// Package harness is the runtime-side implementation of the Message-Write
// Harness — the validation chain every channel-truth write MUST go through. It
// is the channel's single write path; every channel-truth write passes through
// exactly this engine.
//
// Concrete steps each implement the Step contract; the internal chain composes
// them in stable StepID order with a short-circuit on the first reject.
// Implementations live in step_*.go siblings of chain.go. The package exports
// only an opaque Pen (welded-identity write capability) + a Minter (mint machine,
// platform-only) — the bare chain never leaves the package.
//
// Contracts:
//
//   - The harness validation chain, with authoritative pseudocode.
//   - A closed set of harness_reject_reason values.
//   - Engine append ACL: the harness is the only principal allowed to append.
//
// **Not in scope of this package**:
//
//   - Post-append delivery to actor mailboxes. The harness only validates +
//     appends truth; delivering an appended envelope into the audience's
//     mailboxes (and reporting per-audience Outcome / collapsing an
//     undeliverable request to receiver_unavailable) is outside this package.
//   - Actor hosting + transport (in-process cell / connect-in port) is outside
//     this package.
package harness
