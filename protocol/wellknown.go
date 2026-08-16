package protocol

import "github.com/wanpengxie/atoll/protocol/channel"

// Installation-wide identities are protocol constants because bootstrap must
// be able to locate the first channel and its non-login system principals
// before the registry is readable.
const (
	C0ChannelID        channel.ID = "c0"
	LobbyChannelID     channel.ID = "c0.lobby"
	RootPrincipalID               = "root"
	StewardPrincipalID            = "steward"
	GuestPrincipalID              = "guest"
	LocalDeviceID                 = "local-device"
)
