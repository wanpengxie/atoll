package message

// CoreTypeRule expresses the L1 §1.1 core-type table for one entry. Core
// types are engine-built-in (kernel-authored, not domain-registered) and
// carry a default kind plus whether callers may override that kind.
type CoreTypeRule struct {
	DefaultKind   Kind
	AllowOverride bool
}

// CoreTypeTable is the closed set of core message types per
// impl-vocabulary §2.1 (v1 collaboration vocabulary):
//
//	human.text · agent.text · core.system_event(*)
//	system.heartbeat(**) · file.created · file.updated
//
// (*) `core.system_event` is the canonical v1 spelling for system events.
// The pre-v1 dotted spelling is intentionally not accepted here:
// this is a clean rename, not a back-compat alias.
//
// (**) `system.heartbeat` is wire-only — impl-vocabulary §2.7 +
// proto-foundation §1.12 + proto-layer0 §3.1 specify it as a transport
// keepalive frame, NOT a channel envelope (and NOT the actor control channel —
// it is neither work, control, nor obs at the actor level; it is pure transport
// liveness). It does not enter the message log and intentionally MUST NOT appear
// in coreTypeTable — so it can never be a substrate type at all (no envelope-side
// filter is needed or wanted: the substrate is type-agnostic and owns no
// per-type-name special cases).
//
// Likewise `agent.progress` was historically a separate core type for
// intermediate per-step progress bubbles. impl-vocabulary §2.3
// collapses that into `agent.text` + `visibility=system` — progress is
// emitted as an agent.text envelope with visibility=system, so no
// separate core-type registration is needed.
// coreTypeTable is UNEXPORTED: an exported map is mutable (importers can add /
// delete / rewrite core-type entries), which turns a protocol closed set into
// runtime-writable config. The public contract is the LookupCoreType predicate.
var coreTypeTable = map[string]CoreTypeRule{
	"human.text":        {DefaultKind: KindEvent, AllowOverride: true},
	"agent.text":        {DefaultKind: KindEvent, AllowOverride: true},
	"core.system_event": {DefaultKind: KindEvent, AllowOverride: false},
	"file.created":      {DefaultKind: KindEvent, AllowOverride: false},
	"file.updated":      {DefaultKind: KindEvent, AllowOverride: false},
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
