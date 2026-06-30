package resource

// ResourceID is the opaque name of a resource — the passive object of the access
// plane (proto-ontology-closure §2). The object has NO gift (§1.2): it does not run,
// panic, or restart, so it has NO incarnation — its coordinate is SINGLE-LEVEL (this
// ResourceID + current state), dual to an actor's two-level identity×incarnation. That
// single level is the first-principles root of "the object is thin": in Lampson terms
// it is just an index into the access matrix's object axis, carrying no substrate-
// meaningful structure of its own (bytes opaque, all structure in the access relation).
// So the kernel says exactly one thing about it — a stable, opaque addressing name.
// Like channel.ID: no validation, normalization, or allocation.
//
// CONTROLLER / GRANTS / KIND are NOT here: they are the object's LIFECYCLE runtime
// state (the authorization relation R + the resource registry, §3.5), not proto. The
// object is born/owned/granted/destroyed entirely via the access plane (§1.3); only the
// kernel-level name crosses a proto boundary. resource is thus even THINNER than actor
// (no Kind/Binding) — exactly "the object is only an index".
//
// GRANULARITY is the driver's model, not the kernel's (守结构不守词汇): whether one
// ResourceID is one KV entry, one file, or a namespace — and whether a sub-selector
// exists — is the driver's mapping. The kernel keeps it opaque, never interprets "key".
//
// WHY ResourceID is proto = it is the TARGET of the access relation (access.Invocation.Resource,
// §3.4) — a plane-2 cross-boundary name both ends must agree on (认证判准). It is NOT proto
// "because message references it" (that older reason is retired): a message referencing a
// resource carries the id as OPAQUE payload, and protocol/message does NOT import
// protocol/resource. (A domain payload schema MAY still encode with the canonical
// resource.ResourceID — message-the-substrate just never interprets it.) So ResourceID ships
// WITH access, never alone (a type with no consumer violates 零预留).
//
// Cross-channel uniqueness is NOT guaranteed (access is channel-封, forward §12.5).
type ResourceID string

// String returns the wire form.
func (r ResourceID) String() string { return string(r) }
