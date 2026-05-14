package addressing

// Route is the M2+ federation routing abstraction placeholder per
// .dalek/pm/m1.5-tickets.md §T10 — "addr → 实际传输路径". M1.5 has no
// implementations: all transports are in-process or single-server
// daemonbus, and routing is implicit ("the only server").
//
// Future M1.4 / M2+ work (channel-as-actor cross-channel, federation
// peer-server hop, SaaS multi-region) will provide concrete Route
// implementations without changing call sites that already speak in
// ActorRef / ChannelRef.
//
// The interface is intentionally tiny: M1.5 only needs the type to
// exist so that callers can be written against `addressing.Route`
// (instead of hard-coding "single-server in-process call"). New
// methods will be added in M1.4 / M2+ tickets as concrete routing
// strategies emerge.
type Route interface {
	// Target returns the logical address this route resolves to.
	Target() ActorRef
}

// LocalRoute is the trivial single-server / in-process route used by
// M1.5 callers that need a Route value but have no real transport
// hop. It just echoes the target ref.
//
// Federation / channel-as-actor work will introduce additional
// concrete types (e.g. ChannelMirrorRoute, FederatedPeerRoute);
// LocalRoute stays as the demo default.
type LocalRoute struct {
	Ref ActorRef
}

// Target implements Route.
func (r LocalRoute) Target() ActorRef { return r.Ref }
