package message

// CoreTypeRule expresses the core-type table for one entry. Core types are
// engine-built-in (kernel-authored, not domain-registered).
//
// DefaultKind is the type's CANONICAL kind. It is NOT a fill-default — kind is
// sender-required (stepEnvelopeShape rejects an empty kind before normalize),
// so nothing ever "defaults" to it. Its live role is a CONSTRAINT: when
// AllowOverride is false, stepKindAndAudience rejects any envelope of this
// type whose kind != DefaultKind. The name is kept (rather than renamed to
// CanonicalKind) only to bound this cleanup's blast radius.
//
// The AllowOverride=false enforcement branch currently has no subject — both
// live core types (human.text, agent.text) are AllowOverride=true — but it is
// additive-ready, not a separate slice (a few lines in an existing validation
// step). The first real AllowOverride=false core type (e.g. a system/file
// event with a pinned kind) reactivates it for free. Kept, not ripped.
type CoreTypeRule struct {
	DefaultKind   Kind
	AllowOverride bool
}

// CoreTypeTable is the closed set of core message types — the substrate's OWN
// authoritative vocabulary (engine-built-in, not domain-registered):
//
//	human.text · agent.text
//
// Only types with a LIVE producer are members. `core.system_event`,
// `file.created`, `file.updated` were entries with ZERO producer anywhere in the
// system (the live system-event path is `agent.text` + visibility=system, see
// below) — a core type the substrate manages by rule but no one emits is a name
// in a frozen set that earns nothing, so they were removed. Re-adding is additive
// (one map line + one closed-set test line; both consumers — step_normalize,
// step_kind_audience — are generic LookupCoreType callers with no per-type
// special-casing): a real file/system-event producer brings its core-type
// entry back with it when the need arises.
//
// `system.heartbeat` is wire-only — it is a transport keepalive frame, NOT a
// channel envelope (and NOT the actor control channel — neither work, control,
// nor obs at the actor level; pure transport liveness). It does not enter the
// message log and MUST NOT appear in coreTypeTable.
//
// `agent.progress` was historically a separate core type for intermediate
// per-step progress bubbles. That collapses into `agent.text` +
// visibility=system, so no separate core-type registration.
//
// coreTypeTable is UNEXPORTED: an exported map is mutable (importers can add /
// delete / rewrite core-type entries), which turns a protocol closed set into
// runtime-writable config. The public contract is the LookupCoreType predicate.
var coreTypeTable = map[string]CoreTypeRule{
	"human.text": {DefaultKind: KindEvent, AllowOverride: true},
	"agent.text": {DefaultKind: KindEvent, AllowOverride: true},
}

// LookupCoreType resolves a core message type to its rule. ok=false means the
// type is not a core type — it is then ordinary domain-defined vocabulary the
// substrate carries opaquely and does not resolve (the substrate is
// type-agnostic outside this frozen core set). This is the read-only contract
// over that frozen core-type closed set.
func LookupCoreType(typeName string) (CoreTypeRule, bool) {
	r, ok := coreTypeTable[typeName]
	return r, ok
}
