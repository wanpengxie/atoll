package engineboot

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestActorChannelRealizesSeatAndHandleInsteadOfServicePair(t *testing.T) {
	eng, _, core, registrar := newProtocolDeliveryRig(t)
	stewardDeclID := lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward")
	stewardID := onlyDecl(t, core, stewardDeclID)

	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "actor-body", "recipe": map[string]any{
			"type": "actor", "declarations": []any{},
			"profile": map[string]any{"serving": 0, "svc_agent": stewardDeclID},
		}, "initial_actor_ids": []any{stewardID},
	}), &created)
	body := waitBundle(t, eng, created.ChannelID)

	deadline := time.Now().Add(5 * time.Second)
	var seat actor.ActorID
	for time.Now().Before(deadline) {
		roster, _ := core.View().Roster(context.Background())
		for _, member := range roster {
			if member.DeclID == string(created.ChannelID) && member.Kind == actor.KindChannel {
				seat = member.ID
			}
		}
		if seat != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if seat == "" {
		t.Fatal("actor body was not seated in its host as kind=channel")
	}

	roster, err := body.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	handle, svc, humans := false, false, 0
	for _, member := range roster {
		handle = handle || member.DeclID == channelmember.HandleDeclID
		svc = svc || member.DeclID == lagoon.SvcActorDeclID
		if member.Kind == actor.KindHuman {
			humans++
		}
	}
	if !handle || svc || humans != 0 {
		t.Fatalf("body roster handle=%v svcactor=%v humans=%d rows=%+v", handle, svc, humans, roster)
	}

	row, found, err := eng.registry.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || row.Type != lagoon.ChannelTypeActor || row.Serving != 0 {
		t.Fatalf("actor body row=%+v found=%v err=%v", row, found, err)
	}
	decl, found, err := eng.registry.GetDecl(context.Background(), string(created.ChannelID))
	if err != nil || !found || decl.DefaultClass != channelmember.SeatClass || decl.Name != string(created.ChannelID) {
		t.Fatalf("seat declaration=%+v found=%v err=%v", decl, found, err)
	}
	if want := actor.ActorID("channel:" + string(created.ChannelID) + ":"); len(seat) <= len(want) || seat[:len(want)] != want {
		t.Fatalf("seat id=%q want stable prefix %q", seat, want)
	}

	var ordinary []regspec.ChannelRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelList), map[string]any{}), &ordinary)
	for _, candidate := range ordinary {
		if candidate.ID == created.ChannelID {
			t.Fatal("actor body leaked into ordinary channel list")
		}
	}
	var managed []regspec.ChannelRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelList), map[string]any{"include_actor_channels": true}), &managed)
	found = false
	for _, candidate := range managed {
		found = found || candidate.ID == channel.ID(created.ChannelID)
	}
	if !found {
		t.Fatal("actor body absent from explicit management list")
	}
}
