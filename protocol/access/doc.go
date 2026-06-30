// Package access defines the second-plane (subject→object) access relation: the
// off-log vertical channel through which a subject (actor) invokes lifecycle and
// content operations on a passive object (resource). It is the施动面 of the
// object lifecycle (proto-second-plane-spec §2.1) — dual to the message plane's
// on-log协作真相.
//
// Authoritative spec: proto-second-plane-spec.md §3 (derived from the client
// object lifecycle, §1/§2); ontology source proto-ontology-closure.md §1.5/§2.
//
// Contents:
//
//   - Operation     — the closed-set lifecycle/content verb set
//     {create,read,write,set,delete} falling out of the resource lifecycle (§3.2).
//   - Grant         — the substrate-typed operand of op=set (§3.2.1).
//   - FailureReason — the closed-set access-failure verdict vocabulary (§3.3).
//   - Invocation    — one access: a subject invokes one operation on one object
//     with operands (§3.4).
//
// The package depends on protocol/actor (Caller + Grant.Grantee) and
// protocol/resource (Resource). It does NOT import protocol/channel (access is
// connection/door-scoped and carries no channel_id, §2.5) or protocol/message
// (the two planes — on-log message vs off-log access — are orthogonal).
//
// Pure proto: no context, no storage, no transport. The authorization relation
// R, the door, the drivers, and the success outcome (value/found) are all
// RUNTIME, not proto — this package gives only types + single-field closed-set
// predicates (ParseOperation / IsValidFailureReason). Op×field shape rules are
// enforced at the runtime door's ingress step (like the harness step_* checks),
// NOT as a proto method — exactly as message carries no Envelope.Validate.
package access
