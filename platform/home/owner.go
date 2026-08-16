package home

import (
	"context"
	"strings"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Owner is a property of the CHANNEL, and its one and only home is the genesis
// pointer written at creation. "Is this actor the owner" is therefore always a
// derived judgement — a human whose login principal equals genesis.OwnerPrincipal
// — never a stored bit on the record.
func (h *Home) isOwner(facts storespec.ActorFacts) bool {
	if h == nil || h.ownerPrincipal == "" {
		return false
	}
	return facts.Kind == actor.KindHuman && facts.Principal == h.ownerPrincipal
}

// guardOwnerTerminal is the command-front owner protection for the management
// End/Remove words. Policy lives at the door; the ledger transaction is
// mechanical and knows nothing about owners.
func (h *Home) guardOwnerTerminal(ctx context.Context, target actor.ActorID) error {
	if h == nil || target == "" || h.ownerPrincipal == "" {
		return nil
	}
	if target == actor.SystemActorID {
		return &channelspec.OperationError{Code: channelspec.ErrCodeProtectedActor, Detail: "system actor is protected"}
	}
	facts, active, err := h.actors.ActorFacts(ctx, target)
	if err != nil || !active {
		return err
	}
	if h.isOwner(facts) {
		return &channelspec.OperationError{
			Code: channelspec.ErrCodeProtectedActor, Detail: "channel owner is protected",
		}
	}
	if facts.SourceDeclID == lagoon.SvcActorDeclID || facts.SourceDeclID == lagoon.CoreActorDeclID || facts.SourceDeclID == lagoon.RegistrarSeatDeclID {
		return &channelspec.OperationError{Code: channelspec.ErrCodeProtectedActor, Detail: "system actor is protected"}
	}
	if strings.HasPrefix(facts.SourceDeclID, lagoon.PeerActorDeclPrefix) {
		targetChannel := channel.ID(strings.TrimPrefix(facts.SourceDeclID, lagoon.PeerActorDeclPrefix))
		desired, found, err := h.registryBindings.ChannelDesired(ctx, targetChannel)
		if err != nil {
			return err
		}
		if found && desired.Present && (h.channelID == "c0" || desired.ParentID == h.channelID) {
			return &channelspec.OperationError{Code: channelspec.ErrCodeProtectedActor, Detail: "foundation peeractor is protected"}
		}
	}
	return nil
}
