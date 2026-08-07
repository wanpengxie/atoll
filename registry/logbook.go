package registry

// TypeLogbookRecent is the system actor's bounded, read-only channel-history
// query. It is shared by the producer and agent/base so the wire verb has one
// owner.
const TypeLogbookRecent = "logbook.recent"

// TypeSetDefaultAgent is the channel system actor's standard routing verb.
const TypeSetDefaultAgent = "channel.set_default_agent"
