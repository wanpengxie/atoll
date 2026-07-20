package app_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestActorDeclListProjectsCurrentAndLatestChannelVersions(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "versioned", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	declID := respJSON(t, w)["id"].(string)
	createAndBindDaemon(t, env, s.chID, "version-host-a", s.cookies)
	introduced := env.do(t, "POST", "/api/channels/"+s.chID+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introduced, http.StatusCreated)
	secondBody, cookies := createChannel(t, env, s.cookies, "versioned-second")
	s.cookies = cookies
	secondChannel := secondBody["id"].(string)
	createAndBindDaemon(t, env, secondChannel, "version-host-b", s.cookies)
	introduced = env.do(t, "POST", "/api/channels/"+secondChannel+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introduced, http.StatusCreated)
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
		if first == nil || first["current_version"] != float64(2) || first["latest_version"] != float64(2) {
			t.Fatalf("first version projection=%v", first)
		}
		if second == nil || second["current_version"] != float64(1) || second["latest_version"] != float64(1) {
			t.Fatalf("second version projection=%v", second)
		}
		return
	}
	t.Fatalf("declaration %s absent from response: %v", declID, body)
}
