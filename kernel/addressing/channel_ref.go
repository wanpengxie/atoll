package addressing

import "github.com/wanpengxie/ActOS/kernel/channel"

// ChannelRef is the federation-shaped channel reference per
// .dalek/pm/m1.5-tickets.md §T10 — `(org_id?, channel_id)`. It is the
// stable name kernel-level callers depend on; the underlying struct
// lives in kernel/channel (existing M1.5 type) so addressing/channel_ref
// is a one-line alias and there is a single source of truth.
//
// OrgID is empty in single-org / demo deployments (M1.5). M2+
// federation populates it to express cross-org channel mirrors.
type ChannelRef = channel.Ref

// NewChannelRef constructs a channel ref. OrgID is optional; pass ""
// for single-org / demo callers.
func NewChannelRef(orgID string, id channel.ID) ChannelRef {
	return ChannelRef{OrgID: orgID, ID: id}
}

// LocalChannelRef builds a same-org ref (OrgID = "") for the given
// channel id. Convenience wrapper for M1.5 callers that never set
// OrgID.
func LocalChannelRef(id channel.ID) ChannelRef {
	return ChannelRef{ID: id}
}
