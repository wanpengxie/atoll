package actor

// Binding is the actor/tool transport descriptor shared by actor_registry
// rows and adapter type_registry declarations. Its string values are the
// canonical wire/SQL forms from proto-foundation §2.5.1.
//
// Closed set semantics:
//
//   - embedded                  — adapter runs in the channel runtime process,
//     no wire out of process (in-process tool calls).
//   - runtime_outbound          — adapter runs in the channel runtime process,
//     actively dials external services
//     (transport choice — HTTP / gRPC / etc. — is
//     implementation freedom).
//   - runtime_inbound_via_relay — adapter runs in the channel runtime process,
//     remote peer connects via server relay (transport choice is
//     implementation freedom).
type Binding string

const (
	BindingEmbedded               Binding = "embedded"
	BindingRuntimeOutbound        Binding = "runtime_outbound"
	BindingRuntimeInboundViaRelay Binding = "runtime_inbound_via_relay"
)

// AllBindings lists every supported binding value in spec order.
var AllBindings = []Binding{
	BindingEmbedded,
	BindingRuntimeOutbound,
	BindingRuntimeInboundViaRelay,
}

// String returns the wire form.
func (b Binding) String() string { return string(b) }

// ParseBinding resolves a canonical wire-form binding string.
func ParseBinding(raw string) (Binding, bool) {
	for _, b := range AllBindings {
		if string(b) == raw {
			return b, true
		}
	}
	return "", false
}
