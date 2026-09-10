package message

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Caller and Context are message metadata, carried with the request, never
// indexed by an actor's lifetime. The payload framing is enforced by harness.
type Caller struct {
	Channel channel.ID    `json:"channel"`
	Actor   actor.ActorID `json:"actor"`
}

type Context struct {
	Caller  *Caller `json:"caller,omitempty"`
	Session string  `json:"session,omitempty"`
}

func (c Context) Clone() Context {
	if c.Caller != nil {
		caller := *c.Caller
		c.Caller = &caller
	}
	return c
}
