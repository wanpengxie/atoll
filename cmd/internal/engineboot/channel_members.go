package engineboot

import (
	"context"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Resolve against the current Home at use time, never retain an incarnation's roster.
type bodyMembers struct {
	host *channelhost.ChannelHost
	body channel.ID
}

func (m bodyMembers) MemberOfDeclaration(decl string) (actor.ActorID, error) {
	b, ok := m.host.Acquire(m.body)
	if !ok {
		return "", channelmember.ErrUnreachable
	}
	members, ok := b.View().(channelmember.Members)
	if !ok {
		return "", channelmember.ErrUnreachable
	}
	return members.MemberOfDeclaration(decl)
}
func (m bodyMembers) ActorFacts(ctx context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	b, ok := m.host.Acquire(m.body)
	if !ok {
		return channelspec.ActorFacts{}, false, channelmember.ErrUnreachable
	}
	return b.View().ActorFacts(ctx, id)
}
