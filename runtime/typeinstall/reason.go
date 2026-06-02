package typeinstall

// InstallReason is the closed set of reasons the type-install behaviour
// rejects with — the install ENGINE's errno vocabulary (Erlang/Unix prior-art,
// kernel-construction-spec §0.1). Relocated from the deleted kernel/message;
// target-state it moves with typeinstall to lib/adapterhost.
type InstallReason string

const (
	InstallAdapterTimeoutMissing         InstallReason = "adapter_timeout_missing"
	InstallHandlerActorNotRegistered     InstallReason = "handler_actor_not_registered"
	InstallHandlerActorBindingMismatch   InstallReason = "handler_actor_binding_mismatch"
	InstallTypeRegistryInvalid           InstallReason = "type_registry_invalid"
	InstallTypeRegistryReservedNamespace InstallReason = "type_registry_reserved_namespace"
	InstallWorkerLockHeld                InstallReason = "worker_lock_held"
	InstallBootstrapInProgress           InstallReason = "bootstrap_in_progress"
)

// String returns the wire form.
func (r InstallReason) String() string { return string(r) }
