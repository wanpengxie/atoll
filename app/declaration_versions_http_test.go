package app_test

import (
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestActorDeclListProjectsInstancesWithoutChannelLocalVersions(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "versioned", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	declID := respJSON(t, w)["id"].(string)
	firstDaemon := createAndBindDaemon(t, env, s.chID, "version-host-a", s.cookies)["id"].(string)
	introduced := env.do(t, "POST", "/api/channels/"+s.chID+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introduced, http.StatusCreated)
	firstActor := actor.ActorID(respJSON(t, introduced)["actor_id"].(string))
	secondBody, cookies := createChannel(t, env, s.cookies, "versioned-second")
	s.cookies = cookies
	secondChannel := secondBody["id"].(string)
	createAndBindDaemon(t, env, secondChannel, "version-host-b", s.cookies)
	introduced = env.do(t, "POST", "/api/channels/"+secondChannel+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introduced, http.StatusCreated)
	updated := env.do(t, http.MethodPut, "/api/channels/"+s.chID+"/decls/"+declID+"/config", map[string]any{"config": map[string]any{"model": "v2"}}, s.cookies)
	assertStatus(t, updated, http.StatusOK)
	_ = firstDaemon
	waitDeclaredConfig(t, env, channel.ID(s.chID), declID, firstActor, "v2")

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
		first, second := byChannel[s.chID], byChannel[secondChannel]
		if first == nil || second == nil {
			t.Fatalf("instance projection first=%v second=%v", first, second)
		}
		for _, instance := range []map[string]any{first, second} {
			if _, leaked := instance["current_version"]; leaked {
				t.Fatalf("channel-local current_version leaked into realm DTO: %v", instance)
			}
			if _, leaked := instance["latest_version"]; leaked {
				t.Fatalf("channel-local latest_version leaked into realm DTO: %v", instance)
			}
		}
		return
	}
	t.Fatalf("declaration %s absent from response: %v", declID, body)
}

func TestActorDeclListIncludesPublicAndOwnPrivateOnly(t *testing.T) {
	env := setupTestApp(t)
	owner, ownerCookies := register(t, env, "decl-owner@example.com", "secret123", "Decl Owner")
	viewer, viewerCookies := register(t, env, "decl-viewer@example.com", "secret123", "Decl Viewer")

	create := func(cookies []*http.Cookie, name, visibility string) string {
		t.Helper()
		w := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
			"name": name, "class": "go-kimi", "visibility": visibility,
		}, cookies)
		assertStatus(t, w, http.StatusCreated)
		return respJSON(t, w)["id"].(string)
	}
	publicID := create(ownerCookies, "owner-public", "public")
	privateID := create(ownerCookies, "owner-private", "private")
	viewerPrivateID := create(viewerCookies, "viewer-private", "private")

	w := env.do(t, http.MethodGet, "/api/actor-decls", nil, viewerCookies)
	assertStatus(t, w, http.StatusOK)
	rows := respJSON(t, w)["decls"].([]any)
	got := make(map[string]map[string]any, len(rows))
	for _, raw := range rows {
		row := raw.(map[string]any)
		got[row["id"].(string)] = row
	}
	if got[publicID] == nil || got[viewerPrivateID] == nil {
		t.Fatalf("public and own private declarations must be listed: %v", got)
	}
	if got[privateID] != nil {
		t.Fatalf("another principal's private declaration leaked: %v", got[privateID])
	}
	if got[publicID]["owner"] != owner["id"] || got[viewerPrivateID]["owner"] != viewer["id"] {
		t.Fatalf("declaration ownership projection is wrong: %v", got)
	}
}
