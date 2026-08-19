package harness

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const PayloadContextKey = "_context"

// Caller identifies the concrete member for whom a framework-authored request
// was written.
type Caller struct {
	Channel channel.ID    `json:"channel"`
	Actor   actor.ActorID `json:"actor"`
}

type Context struct {
	Caller Caller `json:"caller"`
}
