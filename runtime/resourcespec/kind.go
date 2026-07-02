package resourcespec

// ResourceKind is runtime-owned, NOT proto (protocol/resource/id.go keeps
// kind/controller/grants OUT of the pure ResourceID: they are lifecycle
// runtime state, not a cross-boundary name). Unlike actor.Kind — a permanent
// closed set of four — ResourceKind is a SEMI-CLOSED set that grows by pain,
// one driver at a time; a new value = a new substrate driver (kernel change +
// review), not a protocol revision, and NOT an open extension point for domain
// code.
//
// The naming axis is the DOOR-BACK IMPLEMENTATION VARIANT (mechanical: byte
// size / at-rest encryption / storage locus), NOT the use case. kind is a
// durable persisted value (the resources.kind column), so the implementation
// name never lies about the bytes behind it; use case lives in an id naming
// convention instead (the Unix /etc pattern). Day-1 pins exactly KindKV;
// state (actor-scoped) / file / secret land additively — a value + a driver —
// when their mechanical difference is real.
type ResourceKind string

// KindKV is day-1's only driver: channel-scoped, small inline bytes, plaintext.
const KindKV ResourceKind = "kv"
