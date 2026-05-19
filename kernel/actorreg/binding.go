package actorreg

// Binding is the actor-level transport descriptor. It mirrors the
// `actor_registry.actor_binding` column (L2 §1.4.6) and is identical to
// the M1.5 closed enum defined in L1 §11.7. The string values match the
// SQL CHECK constraint; keep in sync with kernel/adapter/binding.go.
type Binding string

// Binding closed set. Kept duplicated as raw strings here rather than
// importing kernel/adapter so actorreg does not depend on adapter.
const (
	BindingInProcess        Binding = "in_process"
	BindingOutboundHTTP     Binding = "outbound_http"
	BindingViaServerTransit Binding = "via_server_transit"
)
