package lagoon_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
	_ "modernc.org/sqlite"
)

type cutFacts struct{}

func (cutFacts) ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return channelspec.ActorFacts{Principal: channelspec.RootPrincipalID, Kind: actor.KindHuman, Active: true}, true, nil
}

type cutSys struct {
	actorbase.Sys
	msgs  []actorbase.Msg
	reply lagoon.Reply
	code  string
}

func (s *cutSys) Recv() (actorbase.Msg, error) {
	if len(s.msgs) == 0 {
		return actorbase.Msg{}, errors.New("cut fixture complete")
	}
	msg := s.msgs[0]
	s.msgs = s.msgs[1:]
	return msg, nil
}
func (s *cutSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.reply = value.(lagoon.Reply)
	return "reply", nil
}
func (s *cutSys) Fail(_ actorbase.Msg, code, _ string, _ ...map[string]any) (message.ID, error) {
	s.code = code
	return "fail", nil
}
func (s *cutSys) Call(message.Cause, actor.ActorID, string, any) (actorbase.Pending, error) {
	return nil, errors.New("simulated process cut before edge write")
}
func (s *cutSys) Post(behavior.RequestSpec) (message.ID, error) {
	return "", errors.New("simulated process cut before edge write")
}

func cutMessage(word lagoon.Word, payload any) actorbase.Msg {
	raw, _ := json.Marshal(map[string]any{"body": payload})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: message.ID("request-" + string(word)), ChannelID: channelspec.C0ChannelID,
		Sender: message.Sender{Kind: actor.KindHuman, ID: "human:root"}, Kind: message.KindRequest, Type: string(word), Payload: raw,
	})
}

func TestCreateCommitSurvivesCutBeforeEdgeAndRetireCleansResidual(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "channels")
	installed, err := boot.Ensure(context.Background(), boot.Config{ChannelDir: dir, RootPassword: "cut-password", ResolveClassConfig: registry.ResolveDefaultConfig})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	registrar := lagoon.NewRegistrar(registry, cutFacts{}, nil)
	def := lagoon.Def(registrar)
	proc, err := def.New()
	if err != nil {
		t.Fatal(err)
	}
	createSys := &cutSys{msgs: []actorbase.Msg{cutMessage(lagoon.WordChannelCreate, map[string]any{"name": "cut-child", "initial_seats": []any{}})}}
	_ = proc(createSys)
	if createSys.code != "" {
		t.Fatalf("create failed code=%s", createSys.code)
	}
	var created lagoon.ChannelCreateReply
	if err := createSys.reply.DecodeValue(&created); err != nil || created.ChannelID == "" {
		t.Fatalf("created=%+v reply=%+v err=%v", created, createSys.reply, err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	row, found, err := reopened.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || row.Status != "present" {
		t.Fatalf("post-restart row=%+v found=%v err=%v", row, found, err)
	}
	c0db, err := sql.Open("sqlite", installed.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c0db.Close()
	var peers int
	if err := c0db.QueryRow(`SELECT COUNT(*) FROM actor_registry WHERE source_decl_id=? AND deregistered_at IS NULL`, "peer:"+string(created.ChannelID)).Scan(&peers); err != nil || peers != 0 {
		t.Fatalf("residual peer count=%d err=%v", peers, err)
	}

	retireProc, err := lagoon.Def(lagoon.NewRegistrar(reopened, cutFacts{}, nil)).New()
	if err != nil {
		t.Fatal(err)
	}
	retireSys := &cutSys{msgs: []actorbase.Msg{cutMessage(lagoon.WordChannelDelete, map[string]any{"channel_id": created.ChannelID})}}
	_ = retireProc(retireSys)
	if retireSys.code != "" {
		t.Fatalf("retire failed code=%s", retireSys.code)
	}
	row, found, err = reopened.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || row.Status != "retired" {
		t.Fatalf("retired row=%+v found=%v err=%v", row, found, err)
	}
}
