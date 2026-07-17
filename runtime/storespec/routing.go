package storespec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type ChannelRouting interface {
	DefaultAgent(context.Context) (actor.ActorID, bool, error)
	SetDefaultAgent(context.Context, actor.ActorID) error
}

type RestartJournal interface {
	MarkRestartApplied(context.Context, int64, actor.ActorID, int64) (bool, error)
}
