package gateway

import "github.com/wanpengxie/ActOS/kernel/message"

// resolveKind applies L1 §1.1 default-kind semantics at the gateway
// edge. caller-supplied kind is preferred when non-empty; otherwise the
// kernel/message core-type default fills in. business types without
// caller kind return empty — daemon harness step 5 then rejects
// (`kind_not_allowed`). The bool reports whether the resolved kind is valid
// for the type (false → caller overrode a kind-locked core type).
func resolveKind(typeName string, caller message.Kind) (message.Kind, bool) {
	rule, isCore := message.CoreTypeTable[typeName]
	switch {
	case caller != "":
		// Caller provided a kind. If type is core AND override=false,
		// the caller's kind must equal the default.
		if isCore && !rule.AllowOverride && caller != rule.DefaultKind {
			return caller, false
		}
		return caller, true
	case isCore:
		// Caller omitted; core type has a default.
		return rule.DefaultKind, true
	default:
		// Business type, kind omitted — leave empty; daemon harness
		// rejects on step 5.
		return "", true
	}
}
