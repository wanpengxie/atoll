// Package access defines the second-plane (subject→object) access relation: the
// off-log vertical channel through which a subject (actor) invokes lifecycle and
// content operations on a passive object (resource). It is the acting facet of
// the object lifecycle — dual to the message plane's on-log collaboration truth.
//
// Contents:
//
//   - Operation     — the closed-set lifecycle/content verb set
//     {create,read,write,delete} falling out of the resource lifecycle.
//   - FailureReason — the closed-set access-failure verdict vocabulary.
//   - Invocation    — one access: a subject invokes one operation on one object
//     with operands.
//
// There is NO per-object grant vocabulary: the membrane is a uniform trust
// phase (PM-D1) — read/write authorization is channel membership itself
// (owner root ∪ active member), delete additionally distinguishes the creator
// (PM-D3). Authorization needs no relation R to store, so the protocol carries
// no grant operand and no set verb.
//
// The package depends on protocol/actor (Caller) and
// protocol/resource (Resource). It does NOT import protocol/channel (access is
// connection/door-scoped and carries no channel_id) or protocol/message
// (the two planes — on-log message vs off-log access — are orthogonal).
//
// Pure proto: no context, no storage, no transport. The door, the drivers, and
// the success outcome (value/found) are all
// RUNTIME, not proto — this package gives only types + single-field closed-set
// predicates (ParseOperation / IsValidFailureReason). Op×field shape rules are
// enforced at the runtime door's ingress step (like the harness step_* checks),
// NOT as a proto method — exactly as message carries no Envelope.Validate.
package access
