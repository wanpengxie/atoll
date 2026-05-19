// FIX-T8 — server-side early kind normalize + audience validation. The
// authoritative source is L1 §1.1 default kind table; runtime/harness/
// deps.go holds the harness-side mirror. server/gateway cannot import
// runtime/harness (topology runs the other way), so we re-declare the
// minimal subset gateway needs here. KEEP IN SYNC WITH L1 §1.1 AND
// runtime/harness.CoreTypeTable.
//
// We do server-side normalize purely to short-circuit invalid request
// frames before they enter the daemon — the harness re-runs the full
// step 5 check (L1 §10.2 step 5) on the daemon side, so this table is
// defense-in-depth, not the single source of truth.

package gateway

import "github.com/wanpengxie/ActOS/kernel/message"

// coreKindRule is the gateway-local subset of runtime/harness.CoreTypeRule:
// just the default kind and whether the caller may override it (override
// = ❌ means caller-supplied kind must equal the default).
type coreKindRule struct {
	defaultKind   message.Kind
	allowOverride bool
}

// coreKindTable mirrors L1 §1.1. Source of truth is the spec; this map
// is only consulted by handleWriteMessage to fill the default kind when
// the caller omits it.
var coreKindTable = map[string]coreKindRule{
	"human.text": {defaultKind: message.KindEvent, allowOverride: true},
	"agent.text": {defaultKind: message.KindEvent, allowOverride: true},
	// agent.progress — intermediate process bubble (one envelope per
	// tool-call step inside a turn). See runtime/harness/deps.go for the
	// authoritative entry; this table mirrors it so the gateway edge
	// fills the default kind when callers post via the public API.
	"agent.progress":   {defaultKind: message.KindEvent, allowOverride: false},
	"system.event":     {defaultKind: message.KindEvent, allowOverride: false},
	"system.heartbeat": {defaultKind: message.KindEvent, allowOverride: false},
	"file.created":     {defaultKind: message.KindEvent, allowOverride: false},
	"file.updated":     {defaultKind: message.KindEvent, allowOverride: false},
}

// resolveKind applies L1 §1.1 default-kind semantics at the gateway
// edge. caller-supplied kind is preferred when non-empty; otherwise the
// core-type default fills in. business types (not in coreKindTable)
// without caller kind return empty — daemon harness step 5 then rejects
// (`kind_not_allowed`). The bool reports whether the resolved kind is
// valid for the type (false → caller overrode a kind-locked core type).
func resolveKind(typeName string, caller message.Kind) (message.Kind, bool) {
	rule, isCore := coreKindTable[typeName]
	switch {
	case caller != "":
		// Caller provided a kind. If type is core AND override=false,
		// the caller's kind must equal the default.
		if isCore && !rule.allowOverride && caller != rule.defaultKind {
			return caller, false
		}
		return caller, true
	case isCore:
		// Caller omitted; core type has a default.
		return rule.defaultKind, true
	default:
		// Business type, kind omitted — leave empty; daemon harness
		// rejects on step 5.
		return "", true
	}
}
