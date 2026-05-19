package message

// CoreTypeRule expresses the L1 §1.1 core-type table for one entry. Core
// types are engine-built-in: they live outside type_registry and carry a
// default kind plus whether callers may override that kind.
type CoreTypeRule struct {
	DefaultKind   Kind
	AllowOverride bool
}

// CoreTypeTable is the L1 §1.1 closed set of core message types. Keep this
// table in kernel/message so server/gateway and runtime/harness consume one
// source of truth without importing each other.
var CoreTypeTable = map[string]CoreTypeRule{
	"human.text": {DefaultKind: KindEvent, AllowOverride: true},
	"agent.text": {DefaultKind: KindEvent, AllowOverride: true},
	// agent.progress is the intermediate "process bubble" event emitted per
	// LLM-step inside one trigger turn. It is a locked event type.
	"agent.progress":   {DefaultKind: KindEvent, AllowOverride: false},
	"system.event":     {DefaultKind: KindEvent, AllowOverride: false},
	"system.heartbeat": {DefaultKind: KindEvent, AllowOverride: false},
	"file.created":     {DefaultKind: KindEvent, AllowOverride: false},
	"file.updated":     {DefaultKind: KindEvent, AllowOverride: false},
}
