package addressing

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// ActorRef is the federation-shaped actor reference per
// .dalek/pm/m1.5-tickets.md §T10 — the global address of an actor =
// `(ChannelRef, ActorID)`. Actor IDs are channel-local (L1 §3.2 / L1
// §12.5), so any cross-channel / cross-org reference MUST carry the
// hosting ChannelRef to be unambiguous.
//
// Usage:
//
//   - Demo / M1.5: callers populate Channel.OrgID="" and Actor with
//     the channel-local id. ActorRef behaves as `(channel_id, actor_id)`.
//   - M1.4 channel-as-actor: the receiving channel exposes a single
//     ActorRef pointing at the channel's own `host_actor_id` (recorded
//     on the placements row). Cross-channel ask/response addresses use
//     this ref without touching L0 envelope.
//   - M2+ federation: ChannelRef.OrgID is populated, ActorRef becomes
//     a true global address.
//
// Pure value type — safe to copy, compare with ==, use as map key.
type ActorRef struct {
	Channel ChannelRef    // hosting channel (org + channel id)
	Actor   actor.ActorID // channel-local actor id
}

// NewActorRef builds an ActorRef from raw parts. OrgID is optional.
func NewActorRef(orgID string, channelID channel.ID, actorID actor.ActorID) ActorRef {
	return ActorRef{
		Channel: NewChannelRef(orgID, channelID),
		Actor:   actorID,
	}
}

// LocalActorRef builds a same-org ref (OrgID = "") for the given
// channel + actor pair. Convenience wrapper for M1.5 callers.
func LocalActorRef(channelID channel.ID, actorID actor.ActorID) ActorRef {
	return ActorRef{
		Channel: LocalChannelRef(channelID),
		Actor:   actorID,
	}
}

// Local reports whether the ref has no OrgID set (single-org / demo).
// Equivalent to Channel.Local().
func (a ActorRef) Local() bool { return a.Channel.Local() }
