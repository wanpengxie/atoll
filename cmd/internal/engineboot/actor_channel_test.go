package engineboot

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestActorChannelRealizesSeatAndHandleInsteadOfServicePair(t *testing.T) {
	eng, _, core, registrar := newProtocolDeliveryRig(t)
	stewardDeclID := lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward")
	stewardID := onlyDecl(t, core, stewardDeclID)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "body-handle", "name": "host", "class": channelmember.HandleClass, "visibility": "public", "config": map[string]any{"host": string(channelspec.C0ChannelID), "words": map[string]any{}},
	}), nil)

	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "actor-body", "recipe": map[string]any{
			"type": "actor", "declarations": []any{map[string]any{"decl_id": "body-handle"}},
			"profile": map[string]any{"serving": 0, "svc_agent": stewardDeclID},
		}, "initial_actor_ids": []any{stewardID},
	}), &created)
	body := waitBundle(t, eng, created.ChannelID)

	deadline := time.Now().Add(5 * time.Second)
	var seat actor.ActorID
	for time.Now().Before(deadline) {
		roster, _ := core.View().Roster(context.Background())
		for _, member := range roster {
			if member.DeclID == "seat:"+string(created.ChannelID) && member.Kind == actor.KindChannel {
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
	coreRoster, _ := core.View().Roster(context.Background())
	peers, seats := 0, 0
	for _, member := range coreRoster {
		if member.DeclID == string(created.ChannelID) {
			peers++
		}
		if member.DeclID == "seat:"+string(created.ChannelID) {
			seats++
		}
	}
	if peers != 1 || seats != 1 {
		t.Fatalf("c0 actor relation peers=%d seats=%d", peers, seats)
	}
	var second lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "actor-body-second", "initial_actor_ids": []any{}, "recipe": map[string]any{"type": "actor", "declarations": []any{map[string]any{"decl_id": "body-handle"}}, "profile": map[string]any{"serving": 0}},
	}), &second)
	if second.ChannelID == created.ChannelID || second.Relation != "seated" {
		t.Fatalf("same recipe did not create independent body: %+v", second)
	}

	roster, err := body.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// type=actor decides only the parent relation: the body keeps its service
	// door (svcactor) like any channel, and seats its handle beside it.
	handle, svc := false, false
	for _, member := range roster {
		handle = handle || member.DeclID == "body-handle"
		svc = svc || member.DeclID == lagoon.SvcActorDeclID
	}
	if !handle || !svc {
		t.Fatalf("body roster handle=%v svcactor=%v rows=%+v", handle, svc, roster)
	}

	row, found, err := eng.registry.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || row.Type != lagoon.ChannelTypeActor || row.Serving != 0 {
		t.Fatalf("actor body row=%+v found=%v err=%v", row, found, err)
	}
	decl, found, err := eng.registry.GetDecl(context.Background(), "seat:"+string(created.ChannelID))
	if err != nil || !found || decl.DefaultClass != channelmember.SeatClass || decl.Name != string(created.ChannelID) {
		t.Fatalf("seat declaration=%+v found=%v err=%v", decl, found, err)
	}
	if want := actor.ActorID("channel:" + string(created.ChannelID) + ":"); len(seat) <= len(want) || seat[:len(want)] != want {
		t.Fatalf("seat id=%q want stable prefix %q", seat, want)
	}

	// The registry lists every channel with its type; hiding is the frontend's call.
	var listed []regspec.ChannelRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelList), map[string]any{}), &listed)
	found = false
	for _, candidate := range listed {
		if candidate.ID == channel.ID(created.ChannelID) {
			found = candidate.Type == lagoon.ChannelTypeActor
		}
	}
	if !found {
		t.Fatal("actor body absent from channel list or listed without type=actor")
	}
	t.Run("member identity is the body id not declaration name or config", func(t *testing.T) {
		resolver := &assemblyResolver{registry: eng.registry, host: eng.host}
		facts, err := resolver.ResolveDeclaration(context.Background(), channelspec.C0ChannelID, "seat:"+string(created.ChannelID))
		if err != nil || facts.ChannelID != created.ChannelID {
			t.Fatalf("facts=%+v err=%v", facts, err)
		}
		terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{"id": "seat-alias", "name": "arbitrary", "class": channelmember.SeatClass, "visibility": "public", "config": map[string]any{"body": created.ChannelID}}), nil)
		failure := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, "system", "system.member.create", map[string]any{"decl_id": "seat-alias"}))
		if failure.Status != message.StatusFailed {
			t.Fatal("alias created another relation")
		}
		terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{"id": "seat:absent-body", "name": "absent-body", "class": channelmember.SeatClass, "visibility": "public", "config": map[string]any{"body": created.ChannelID}}), nil)
		if _, err := resolver.ResolveDeclaration(context.Background(), channelspec.C0ChannelID, "seat:absent-body"); err == nil {
			t.Fatal("config manufactured a relationship to an absent channel")
		}
		// Configuration cannot retarget an already-minted member identity.
		badConfig := []byte(`{"body":"different-body"}`)
		if _, ok := resolver.BuildClass(channelspec.C0ChannelID, seat, channelmember.SeatClass, badConfig); ok {
			t.Fatal("business config retargeted member")
		}
	})
	t.Run("channel creation does not require a particular handle class", func(t *testing.T) {
		var empty lagoon.ChannelCreateReply
		terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "body-without-handle", "recipe": map[string]any{"type": "actor"}, "initial_actor_ids": []any{}}), &empty)
		if empty.Relation != "seated" {
			t.Fatalf("logical membership depends on a handle implementation: %+v", empty)
		}
		id := onlyDecl(t, core, "seat:"+string(empty.ChannelID))
		failure := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, id, "some.request", map[string]any{}))
		if failure.ErrorCode != "channel_unavailable" {
			t.Fatalf("unattached endpoint=%+v", failure)
		}
	})
	// A separate host may reference the same concrete body. Retiring it must
	// revoke its declarations without silently deleting that host's membership.
	var unrelated lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "unrelated-host", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &unrelated)
	other := waitBundle(t, eng, unrelated.ChannelID)
	t.Run("parent binding leaves other handles untouched", func(t *testing.T) {
		terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{"id": "fixed-core-handle", "name": "core-handle", "class": channelmember.HandleClass, "visibility": "public", "config": map[string]any{"host": "c0", "words": map[string]any{}}}), nil)
		var child lagoon.ChannelCreateReply
		terminalValue(t, callMember(t, unrelated.ChannelID, other, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelCreate), map[string]any{"name": "two-handles", "initial_actor_ids": []any{}, "recipe": map[string]any{"type": "actor", "declarations": []any{map[string]any{"decl_id": "body-handle", "bindings": map[string]any{"host": "parent_channel_id"}}, map[string]any{"decl_id": "fixed-core-handle"}}}}), &child)
		if child.Relation != "seated" {
			t.Fatalf("child=%+v", child)
		}
		row, found, err := eng.registry.GetChannelDesired(context.Background(), child.ChannelID)
		if err != nil || !found {
			t.Fatalf("row missing: %v", err)
		}
		var genesis lagoon.GenesisSpec
		if err := json.Unmarshal(row.Spec, &genesis); err != nil {
			t.Fatal(err)
		}
		seen := 0
		for _, d := range genesis.Declarations {
			want := ""
			if d.DeclID == "body-handle" {
				want = string(unrelated.ChannelID)
			}
			if d.DeclID == "fixed-core-handle" {
				want = "c0"
			}
			if want == "" {
				continue
			}
			cfg, err := channelmember.ParseHandleConfig(d.Rendered.Config)
			if err != nil || string(cfg.Host) != want {
				t.Fatalf("%s host=%s want=%s err=%v", d.DeclID, cfg.Host, want, err)
			}
			seen++
		}
		if seen != 2 {
			t.Fatalf("handles=%d", seen)
		}
	})
	terminalValue(t, callMember(t, unrelated.ChannelID, other, channelspec.RootPrincipalID, "system", "system.member.create", map[string]any{"decl_id": "seat:" + string(created.ChannelID)}), nil)
	otherSeat := onlyDecl(t, other, "seat:"+string(created.ChannelID))
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelDelete), map[string]any{"channel_id": created.ChannelID}), nil)
	if got := onlyDecl(t, other, "seat:"+string(created.ChannelID)); got != otherSeat {
		t.Fatalf("retirement changed unrelated seat: %s", got)
	}
	for _, id := range []string{string(created.ChannelID), "seat:" + string(created.ChannelID)} {
		decl, found, err := eng.registry.GetDecl(context.Background(), id)
		if err != nil || !found || decl.Status != "revoked" {
			t.Fatalf("retired declaration %s: %+v %v", id, decl, err)
		}
	}
	failed := decodeTerminal(t, callMember(t, unrelated.ChannelID, other, channelspec.RootPrincipalID, otherSeat, "some.request", map[string]any{}))
	if failed.Status != message.StatusFailed || failed.ErrorCode != "channel_unavailable" {
		t.Fatalf("retired body terminal=%+v", failed)
	}
}
