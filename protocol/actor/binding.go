package actor

// Binding is the actor transport descriptor — how an actor is reached. Its
// string values are the canonical wire forms.
//
// Closed set semantics:
//
//   - embedded                  — the actor is co-located in the channel host
//     process, no wire out of process (in-process calls).
//   - runtime_outbound          — the actor is co-located in the channel host
//     process and actively dials external services
//     (transport choice — HTTP / gRPC / etc. — is
//     implementation freedom).
//   - runtime_inbound_via_relay — the actor is co-located in the channel host
//     process; a remote peer connects in via a relay (transport choice is
//     implementation freedom).
type Binding string

const (
	BindingEmbedded               Binding = "embedded"
	BindingRuntimeOutbound        Binding = "runtime_outbound"
	BindingRuntimeInboundViaRelay Binding = "runtime_inbound_via_relay"
)

// allBindings backs ParseBinding. UNEXPORTED: the public closed-set contract is
// the ParseBinding predicate, not a mutable enumeration slice.
var allBindings = []Binding{
	BindingEmbedded,
	BindingRuntimeOutbound,
	BindingRuntimeInboundViaRelay,
}

// String returns the wire form.
func (b Binding) String() string { return string(b) }

// ParseBinding resolves a canonical wire-form binding string.
func ParseBinding(raw string) (Binding, bool) {
	for _, b := range allBindings {
		if string(b) == raw {
			return b, true
		}
	}
	return "", false
}
