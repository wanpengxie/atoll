// Package compute is the attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical
// world for ONE daemon process attached to a channel home over the wire. Run
// is the assembly root — it dials in, runs the daemon's own reconcile ring
// against a desired source + builder table, and hosts every AlwaysOn desired
// member as a cell for as long as the process lives:
//
//	Run(ctx, cfg) error
//
// Config's PlanSource is the daemon's authenticated reconcile snapshot — the
// same host-neutral diff-loop paradigm platform/home's reconcile ring runs,
// applied to a daemon's own hosted set rather than a channel's membership.
// StorageHost/LocalFileOpener are the optional injection points a daemon that
// hosts file-kind resources wires (期11 §4/§5) — nil on a daemon that never
// does, at no cost.
//
// # File map
//
// compute.go (Run/Config — the daemon assembly root: dial, redial loop,
// forwarder lifecycle), ring.go (computeRing — dial/reattach/spawn/dispatch/
// cancel + redial backoff), forwarders.go (cellDownWatcher + obs/cancel/
// storage-host forwarders), decl.go (ActorFactorySource/LocalFileOpener/StorageHost +
// the storage mirror types — the decl-family words compute alone speaks; see
// decl.go's own B′ header comment for why ActorDecl itself stays on the
// platform root instead). ActorFactory (the def shape ActorFactorySource.Lookup
// resolves to) is platform.ActorFactory (platform-topology 批 T5b: compute
// consumes the cross-host membrane's word, never defines its own).
package compute
