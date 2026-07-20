package app_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestActorDeclListProjectsCurrentAndLatestChannelVersions(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "versioned", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	declID := respJSON(t, w)["id"].(string)
	payload, _ := json.Marshal(map[string]any{"decl_id": declID, "placement": "server"})
	if _, err := env.app.OperateFaceForTest().Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID), Sender: s.actorID, Payload: payload,
	}); err != nil {
		t.Fatalf("introduce: %v", err)
	}
	secondBody, cookies := createChannel(t, env, s.cookies, "versioned-second")
	s.cookies = cookies
	secondChannel := secondBody["id"].(string)
	secondSender, err := env.app.ResolvePrincipalForTest(secondChannel, "human", s.userID)
	if err != nil {
		t.Fatalf("resolve second-channel sender: %v", err)
	}
	if _, err := env.app.OperateFaceForTest().Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(secondChannel), Sender: secondSender, Payload: payload,
	}); err != nil {
		t.Fatalf("introduce second instance: %v", err)
	}
	_, latest, err := env.app.StageDeclarationEditForTest(channel.ID(s.chID), declID, json.RawMessage(`{"model":"v2"}`))
	if err != nil || latest != 2 {
		t.Fatalf("stage edit latest=%d err=%v", latest, err)
	}

	w = env.do(t, "GET", "/api/actor-decls", nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	body := respJSON(t, w)
	decls := body["decls"].([]any)
	for _, raw := range decls {
		decl := raw.(map[string]any)
		if decl["id"] != declID {
			continue
		}
		instances := decl["instances"].([]any)
		if len(instances) != 2 {
			t.Fatalf("instances=%v", instances)
		}
		byChannel := make(map[string]map[string]any, len(instances))
		for _, rawInstance := range instances {
			instance := rawInstance.(map[string]any)
			byChannel[instance["channel_id"].(string)] = instance
		}
		first := byChannel[s.chID]
		second := byChannel[secondChannel]
		if first == nil || first["current_version"] != float64(1) || first["latest_version"] != float64(2) {
			t.Fatalf("first version projection=%v", first)
		}
		if second == nil || second["current_version"] != float64(1) || second["latest_version"] != float64(1) {
			t.Fatalf("second version projection=%v", second)
		}
		return
	}
	t.Fatalf("declaration %s absent from response: %v", declID, body)
}
