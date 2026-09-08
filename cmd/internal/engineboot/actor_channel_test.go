package engineboot

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestActorChannelIsAHiddenHumanlessServiceContainer(t *testing.T) {
	eng, _, core, registrar := newProtocolDeliveryRig(t)
	rootID := currentMemberID(t, core, channelspec.RootPrincipalID)
	stewardDeclID := lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward")
	stewardID := onlyDecl(t, core, stewardDeclID)
	recipe := map[string]any{
		"type":    "actor",
		"profile": map[string]any{"serving": 1, "svc_agent": stewardDeclID},
	}

	rejected := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "actor-with-human", "recipe": recipe, "initial_actor_ids": []any{rootID, stewardID},
	}))
	if rejected.Status != message.StatusFailed || rejected.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("actor channel accepted a human seat: %+v", rejected)
	}

	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "actor-service", "recipe": recipe, "initial_actor_ids": []any{stewardID},
	}), &created)
	child := waitBundle(t, eng, created.ChannelID)
	row, found, err := eng.registry.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || row.Type != lagoon.ChannelTypeActor {
		t.Fatalf("actor channel row=%+v found=%v err=%v", row, found, err)
	}
	roster, err := child.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range roster {
		if member.Kind == actor.KindHuman {
			t.Fatalf("actor channel contains human member %+v", member)
		}
	}

	var ordinary []regspec.ChannelRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelList), map[string]any{}), &ordinary)
	if channelRowPresent(ordinary, created.ChannelID) {
		t.Fatalf("actor channel leaked into ordinary list: %+v", ordinary)
	}
	var management []regspec.ChannelRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelList), map[string]any{"include_actor_channels": true}), &management)
	if !channelRowPresent(management, created.ChannelID) {
		t.Fatalf("actor channel missing from explicit management list: %+v", management)
	}

	peer := onlyDecl(t, core, string(created.ChannelID))
	var card introspect.Describe
	cardRaw := callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, peer, introspect.QueryDescribe, map[string]any{})
	if err := json.Unmarshal(cardRaw, &card); err != nil {
		t.Fatalf("decode actor-channel card: %v raw=%s", err, cardRaw)
	}
	if _, ok := card.Words[message.TypeSystemMemberList]; ok {
		t.Fatalf("actor channel exposed system membrane: %+v", card.Words)
	}
	if _, ok := card.Words["agent.ask"]; !ok {
		t.Fatalf("actor channel omitted service Agent: %+v", card.Words)
	}
	terminal := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, peer, message.TypeSystemMemberList, map[string]any{}))
	if terminal.Status != message.StatusFailed || terminal.ErrorCode != string(channel.GateEndpointNotFound) {
		t.Fatalf("actor channel accepted disabled channel-system word: %+v", terminal)
	}
	port, _, ok := eng.host.AcquirePort(created.ChannelID)
	if !ok {
		t.Fatal("actor channel service port unavailable")
	}
	spaceResult, err := port.Call(context.Background(), channelspec.C0ChannelID, channel.Request{
		From: channel.From{Channel: channelspec.C0ChannelID}, Type: message.TypeSystemChannelList,
	}, nil)
	if err != nil || spaceResult.Fail == nil || spaceResult.Fail.Code != string(channel.GateEndpointNotFound) {
		t.Fatalf("actor channel accepted direct space-system word: result=%+v err=%v", spaceResult, err)
	}
}

func channelRowPresent(rows []regspec.ChannelRow, id channel.ID) bool {
	for _, row := range rows {
		if row.ID == id {
			return true
		}
	}
	return false
}
