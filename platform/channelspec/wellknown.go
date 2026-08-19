package channelspec

import "github.com/wanpengxie/atoll/protocol/channel"

// Installation identities belong to the platform recipe, not the wire
// protocol. Bootstrap uses these names before registry truth is readable.
const (
	C0ChannelID        channel.ID = "c0"
	LobbyChannelID     channel.ID = "c0.lobby"
	RootPrincipalID               = "root"
	StewardPrincipalID            = "steward"
	GuestPrincipalID              = "guest"
	LocalDeviceID                 = "local-device"
)
