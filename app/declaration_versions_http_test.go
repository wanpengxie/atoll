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
	_, latest, err := env.app.SetDeclarationOverlayForTest(channel.ID(s.chID), declID, json.RawMessage(`{"model":"v2"}`))
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
