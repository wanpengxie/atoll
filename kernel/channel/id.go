// Package channel defines the channel-id type and its forward-compatible
// federation reference (ChannelRef). Channel-id is server-allocated and
// globally unique within a server registry per L1 §3.1.
package channel

// ID is the channel identifier — server-allocated, globally unique
// within the server registry per L1 §3.1. Equivalent to envelope
// `channel_id` (L0 §2.1 — same namespace).
type ID string

// String returns the wire form.
func (c ID) String() string { return string(c) }
