// Package compute is the daemon process runtime. Run maintains one realm
// carrier and a map of channel compartments. Each compartment independently
// owns workspace, HostSupervisor, DaemonOutbound, storage host and recovery
// pump; it survives lane and carrier replacement and is destroyed only by an
// explicit compartment_close command.
package compute
