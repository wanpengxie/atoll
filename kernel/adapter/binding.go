// Package adapter declares the M1.5 adapter framework contracts (L1
// §11 + L2 §8): BindingKind tri-class enum, Module / Manager interfaces,
// CorrelationTracker / ErrorPolicy / AdapterCtx / DeviceTransit
// interfaces. Concrete implementations live in adapters/framework /
// adapters/<driver> (T4) + runtime/transit (T3).
//
// kernel/adapter is interface-only — no goroutines, no IO, no
// dependencies beyond kernel/* and the standard library.
package adapter

import "github.com/coagent-ai/coagent/kernel/actor"

// BindingKind is the M1.5 closed enum for adapter handler_binding (L1
// §11.7). Three values: in_process / outbound_http / via_server_transit.
//
// Wire-form values match the actor_registry.actor_binding +
// type_registry.handler_binding columns (L2 §1.4.2 / §1.4.6) and
// kernel/actor.Binding (kept duplicated to avoid kernel/actor ↔
// kernel/adapter cycles).
type BindingKind string

// BindingKind closed set per L1 §11.7.
const (
	BindingInProcess        BindingKind = "in_process"
	BindingOutboundHTTP     BindingKind = "outbound_http"
	BindingViaServerTransit BindingKind = "via_server_transit"
)

// AllBindingKinds lists every M1.5 binding value in spec order.
var AllBindingKinds = []BindingKind{
	BindingInProcess,
	BindingOutboundHTTP,
	BindingViaServerTransit,
}

// String returns the wire form.
func (b BindingKind) String() string { return string(b) }

// LegacyBindingMap maps the pre-M1.5 binding names to their M1.5
// equivalents per L1 §1.2.9 + L1 §11.7 ("Legacy 名称映射"). install
// validators MUST consult this map when accepting old type_registry
// rows; new schema MUST emit M1.5 names only.
//
// Both legacy values map to BindingInProcess — they were physical
// transports under the old daemon-rpc / in-worker-bus model and now
// collapse into "adapter handler runs in daemon process".
var LegacyBindingMap = map[string]BindingKind{
	"daemon_rpc":    BindingInProcess,
	"in_worker_bus": BindingInProcess,
}

// NormalizeBinding resolves a wire-form binding string against the M1.5
// closed set + legacy map. Returns the canonical BindingKind on success.
// Returns ok=false when the input is neither legacy nor canonical.
func NormalizeBinding(raw string) (BindingKind, bool) {
	if mapped, ok := LegacyBindingMap[raw]; ok {
		return mapped, true
	}
	for _, b := range AllBindingKinds {
		if string(b) == raw {
			return b, true
		}
	}
	return "", false
}

// MatchesActorBinding reports whether a type_registry.handler_binding
// can satisfy an actor_registry.actor_binding per L1 §11.7 + L2 §1.4.2
// install rule ("handler_binding 与 actor_binding 1:1 对应").
//
// Currently identical-value match — but kept as a function so M1.x
// extensions (e.g. broaden to capability sets) can refine the rule
// without touching every call site.
func (b BindingKind) MatchesActorBinding(actorBinding actor.Binding) bool {
	return string(b) == string(actorBinding)
}
