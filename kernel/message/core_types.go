package message

// CoreTypeRule expresses the L1 §1.1 core-type table for one entry. Core
// types are engine-built-in: they live outside type_registry and carry a
// default kind plus whether callers may override that kind.
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
// this is a clean rename, not a type_registry alias.
//
// (**) `system.heartbeat` is wire-only — impl-vocabulary §2.7 +
// proto-foundation §1.12 + proto-layer0 §3.1 specify it as a control
// frame, NOT a channel envelope. It does not enter the message log,
// is not registered in type_registry, and intentionally MUST NOT
// appear in CoreTypeTable. The runtime/trigger noise-filter (see
// trigger.TypeSystemHeartbeat) is defence-in-depth against bypass.
//
// Likewise `agent.progress` was historically a separate core type for
// intermediate per-step progress bubbles. impl-vocabulary §2.3
// collapses that into `agent.text` + `visibility=system`. Callers
// (kimi bridge, mock bridge) emit progress as agent.text envelopes —
// no separate type registration is needed.
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
// type is not a core type (it must then resolve via type_registry). This is the
// read-only contract over the frozen core-type closed set.
func LookupCoreType(typeName string) (CoreTypeRule, bool) {
	r, ok := coreTypeTable[typeName]
	return r, ok
}
