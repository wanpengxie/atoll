package engineboot

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestChannelCreateCopiesExplicitHumanAndPrivateAgentSeatsByRealKind(t *testing.T) {
	eng, _, core, registrar := newProtocolDeliveryRig(t)
	rootID := currentMemberID(t, core, channelspec.RootPrincipalID)
	stewardDeclID := lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward")
	stewardID := onlyDecl(t, core, stewardDeclID)
	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "explicit-seats", "initial_actor_ids": []any{rootID, stewardID},
	}), &created)
	child := waitBundle(t, eng, created.ChannelID)
	owner, found, err := child.View().OwnerPrincipal(context.Background())
	if err != nil || !found || owner != channelspec.RootPrincipalID {
		t.Fatalf("owner=%q found=%v err=%v", owner, found, err)
	}
	identities, err := child.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var humanRoot, agentSteward, humanSteward bool
	for _, identity := range identities {
		facts, active, factsErr := child.View().ActorFacts(context.Background(), identity.ID)
		if factsErr != nil || !active {
			continue
		}
		switch {
		case facts.Kind == actor.KindHuman && facts.Principal == channelspec.RootPrincipalID:
			humanRoot = true
		case facts.Kind == actor.KindAgent && facts.Principal == channelspec.StewardPrincipalID:
			agentSteward = true
		case facts.Kind == actor.KindHuman && facts.Principal == channelspec.StewardPrincipalID:
			humanSteward = true
		}
	}
	if !humanRoot || !agentSteward || humanSteward {
		t.Fatalf("explicit seats: human root=%v agent steward=%v human steward=%v roster=%+v", humanRoot, agentSteward, humanSteward, identities)
	}
}
