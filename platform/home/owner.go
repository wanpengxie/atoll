package home

import (
	"context"
	"encoding/json"

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
	facts, active, err := h.actors.ActorFacts(ctx, target)
	if err != nil || !active {
		return err
	}
	if h.isOwner(facts) {
		return &channel.OperationError{
			Code: channel.ErrCodeProtectedActor, Detail: "channel owner is protected",
		}
	}
	return nil
}

func jsonPayload(value map[string]any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
