package platform

// The channel system actor's public request verbs — the words an outsider must
// say to address it. They live here, in the membrane, because the system actor
// itself is platform-internal while its callers are not: an agent driver asks
// for channel history, the realm bootstrap sets the default agent. Both sides
// need the same word, and the layer graph puts platform below every caller, so
// this is the one place all of them can read it from without a back edge.
const (
	// TypeLogbookRecent is the bounded, read-only channel-history query used to
	// catch an agent up on what it missed.
	TypeLogbookRecent = "logbook.recent"

	// TypeSetDefaultAgent names the channel's routing verb: which member
	// receives a request that names no audience.
	TypeSetDefaultAgent = "channel.set_default_agent"
)
