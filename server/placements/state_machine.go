package placements

import "github.com/wanpengxie/ActOS/kernel/placement"

// parseState converts a wire-form string into placement.State. Any
// unrecognised value returns an empty placement.State which
// SQLStore.ListByState then surfaces as a SQL filter that matches no
// rows — safer than panicking on user input.
func parseState(s string) placement.State {
	switch placement.State(s) {
	case placement.StateCreating, placement.StateActive, placement.StateOrphan, placement.StateStale:
		return placement.State(s)
	}
	return ""
}

// TenantDefault is the placeholder tenant id used by launch demo
// deployments per launch-ticket notes §T10 ("demo 期 = "" 或
// "default""). Callers that don't care about multi-tenancy may write
// the literal "" — both flow through nullableString to a NULL SQL
// value. TenantDefault exists for callers that want an explicit,
// auditable value.
const TenantDefault placement.TenantID = "default"

// ReserveOptions bundles the federation / tenancy / channel-as-actor
// reservation fields that the launch state machine threads through
// Reserve → INSERT channel_placements per launch-ticket notes §T10. All
// three fields default to "" → NULL in sqlite; launch demo callers
// leave the struct at its zero value.
//
// Future M1.4 / federation / SaaS tickets populate one or more
// fields; the launch reserve / activate / reconcile paths ignore them.
type ReserveOptions struct {
	// TenantID is the multi-tenant scope. "" / "default" in demo;
	// M2+ SaaS deployments scope placement selection / quota by
	// this value without changing the state machine.
	TenantID placement.TenantID

	// HostActorID is the channel-local actor id that this channel
	// exposes externally for M1.4 channel-as-actor (m1.4-channel-as-
	// actor-spec §10). Empty means the channel does not act as an
	// addressable actor (launch default).
	HostActorID string

	// FederatedOrigin is the remote origin (peer-server / region tag)
	// this channel mirrors for M2+ federation. Empty means the channel
	// is native, non-mirror (launch default).
	FederatedOrigin string

	// ChannelType is the L4 channel-template key (e.g. "xhs-creator")
	// that the catalog stored on the channels row. The placement layer
	// itself does not interpret it — it threads the value verbatim into
	// CreateChannelRequest.ChannelType so the daemon-side bootstrap saga
	// can resolve the matching template (actor seeds, workdir subdirs,
	// domain prompt). Empty means "no template" — legacy or generic
	// group channels (M1.6-T5).
	ChannelType string
}

// tenantOrDefault returns the tenant id to persist. Demo callers
// passing "" still land as NULL in sqlite (via nullableString); this
// helper exists so future paths that need a non-NULL audit value can
// substitute TenantDefault explicitly without touching call sites.
func tenantOrDefault(t placement.TenantID) placement.TenantID {
	if t == "" {
		return ""
	}
	return t
}
