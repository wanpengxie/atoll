package actor

// Binding is the actor/tool transport descriptor shared by actor_registry
// rows and adapter type_registry declarations. Its string values are the
// canonical wire/SQL forms from L1 §11.7.
type Binding string

const (
	BindingInProcess        Binding = "in_process"
	BindingOutboundHTTP     Binding = "outbound_http"
	BindingViaServerTransit Binding = "via_server_transit"
)

// AllBindings lists every supported binding value in spec order.
var AllBindings = []Binding{
	BindingInProcess,
	BindingOutboundHTTP,
	BindingViaServerTransit,
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
