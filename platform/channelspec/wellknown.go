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

	// LocalEmailDomain is the domain a node's own accounts live in. A principal
	// is named by an email; on a personal node every account it carves shares
	// this one domain, which is what lets an entrance read a bare name as a
	// local one. Spelled once: an entrance that guessed a different domain than
	// boot wrote would deny accounts that do exist.
	LocalEmailDomain = "atoll.local"
)
