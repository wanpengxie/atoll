package channel

// ChannelRef is the federation-addressable reference to a channel.
// Reserved for future cross-daemon / cross-server addressing; demo era
// uses the local-only ChannelId form (Daemon empty).
//
// TODO(T1): finalize federation addressing per L2 placement spec.
type ChannelRef struct {
	// ChannelID is the channel scalar (required).
	ChannelID ChannelId

	// Daemon is the owning daemon-id; empty means "local / unknown".
	Daemon string
}
