package app_test

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("overlay-test-agent", registry.ClassDecl{Kind: actor.KindAgent, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
	}})
	registry.Register("overlay-test-tool", registry.ClassDecl{Kind: actor.KindTool, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool}, nil
	}})
}

func TestDeclarationOverlayAuthorizationAndDeadInstanceReachability(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "overlay-owner@example.com", "secret123", "Owner")
	channelBody, ownerCookies := createChannel(t, env, ownerCookies, "overlay-auth")
	chID := channelBody["id"].(string)
	_, memberCookies := register(t, env, "overlay-member@example.com", "secret123", "Member")
	joined := env.do(t, http.MethodPost, "/api/channels/"+chID+"/join", nil, memberCookies)
	assertStatus(t, joined, http.StatusCreated)
	_, strangerCookies := register(t, env, "overlay-stranger@example.com", "secret123", "Stranger")

	create := func(name, visibility string) string {
		t.Helper()
		w := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
			"name": name, "class": "overlay-test-agent", "visibility": visibility, "config": map[string]any{"model": "global"},
		}, ownerCookies)
		assertStatus(t, w, http.StatusCreated)
		return respJSON(t, w)["id"].(string)
	}
	put := func(declID string, cookies []*http.Cookie, model string) *httptest.ResponseRecorder {
		t.Helper()
		return env.do(t, http.MethodPut, "/api/channels/"+chID+"/decls/"+declID+"/config", map[string]any{"config": map[string]any{"model": model}}, cookies)
	}
	countOverlay := func(declID string) int {
		t.Helper()
		var count int
		if err := env.db.QueryRow(`SELECT COUNT(*) FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, chID, declID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	publicID := create("public", "public")
	assertStatus(t, put(publicID, strangerCookies, "forbidden"), http.StatusForbidden)
	if got := countOverlay(publicID); got != 0 {
		t.Fatalf("non-member wrote public overlay: rows=%d", got)
	}
	assertStatus(t, put(publicID, memberCookies, "member"), http.StatusOK)
	if got := countOverlay(publicID); got != 1 {
		t.Fatalf("member public overlay rows=%d", got)
	}
	cleared := env.do(t, http.MethodDelete, "/api/channels/"+chID+"/decls/"+publicID+"/config", nil, memberCookies)
	assertStatus(t, cleared, http.StatusOK)
	if got := countOverlay(publicID); got != 0 {
		t.Fatalf("dead-instance overlay clear rows=%d", got)
	}

	privateID := create("private", "private")
	assertStatus(t, put(privateID, memberCookies, "forbidden"), http.StatusForbidden)
	if got := countOverlay(privateID); got != 0 {
		t.Fatalf("non-owner wrote private overlay: rows=%d", got)
	}
	assertStatus(t, put(privateID, ownerCookies, "owner"), http.StatusOK)
	deleted := env.do(t, http.MethodDelete, "/api/actor-decls/"+privateID, nil, ownerCookies)
	assertStatus(t, deleted, http.StatusOK)
	assertStatus(t, put(privateID, ownerCookies, "after-delete"), http.StatusConflict)
	var stored sql.NullString
	if err := env.db.QueryRow(`SELECT config_json FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, chID, privateID).Scan(&stored); err != nil || stored.String != `{"model":"owner"}` {
		t.Fatalf("soft-delete rejection mutated overlay: value=%q err=%v", stored.String, err)
	}
}

func TestDeclarationOverlayMasksGlobalThenDeleteFallsBack(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	created := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "overlay-live", "class": "s8cfg-proc", "config": map[string]any{"model": "global-v1"},
	}, s.cookies)
	assertStatus(t, created, http.StatusCreated)
	declID := respJSON(t, created)["id"].(string)
	daemonID := createAndBindDaemon(t, env, s.chID, "overlay-host", s.cookies)["id"].(string)
	introduced := env.do(t, http.MethodPost, "/api/channels/"+s.chID+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introduced, http.StatusCreated)
	actorID := actor.ActorID(respJSON(t, introduced)["actor_id"].(string))
	waitActorConfig(t, env, channel.ID(s.chID), daemonID, actorID, "global-v1")

	put := env.do(t, http.MethodPut, "/api/channels/"+s.chID+"/decls/"+declID+"/config", map[string]any{"config": map[string]any{"model": "overlay-v2"}}, s.cookies)
	assertStatus(t, put, http.StatusOK)
	waitActorConfig(t, env, channel.ID(s.chID), daemonID, actorID, "overlay-v2")
	global := env.do(t, http.MethodPatch, "/api/actor-decls/"+declID, map[string]any{"config": map[string]any{"model": "global-v3"}}, s.cookies)
	assertStatus(t, global, http.StatusOK)
	assertActorConfigStays(t, env, channel.ID(s.chID), daemonID, actorID, "overlay-v2", 250*time.Millisecond)
	clear := env.do(t, http.MethodDelete, "/api/channels/"+s.chID+"/decls/"+declID+"/config", nil, s.cookies)
	assertStatus(t, clear, http.StatusOK)
	waitActorConfig(t, env, channel.ID(s.chID), daemonID, actorID, "global-v3")

	legacy := env.do(t, http.MethodPut, "/api/channels/"+s.chID+"/actors/"+string(actorID)+"/config", map[string]any{"config": map[string]any{}}, s.cookies)
	assertStatus(t, legacy, http.StatusNotFound)
}

func TestDeclarationClassCannotCrossKind(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "class-owner@example.com", "secret123", "Owner")
	created := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{"name": "kind-pinned", "class": "overlay-test-agent"}, cookies)
	assertStatus(t, created, http.StatusCreated)
	declID := respJSON(t, created)["id"].(string)
	changed := env.do(t, http.MethodPatch, "/api/actor-decls/"+declID, map[string]any{"class": "overlay-test-tool"}, cookies)
	assertStatus(t, changed, http.StatusBadRequest)
	var class string
	if err := env.db.QueryRow(`SELECT default_class FROM actor_decls WHERE id=?`, declID).Scan(&class); err != nil || class != "overlay-test-agent" {
		t.Fatalf("cross-kind edit persisted class=%q err=%v", class, err)
	}
}

func assertActorConfigStays(t *testing.T, env *testEnv, chID channel.ID, daemonID string, instanceID actor.ActorID, want string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		rows, err := env.app.ActorsForTest(chID)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range rows {
			if row.ID != instanceID || row.Placement.Host != daemonID {
				continue
			}
			found = true
			if got := configModel(row.Config); got != want {
				t.Fatalf("overlay lost precedence: got %q want %q", got, want)
			}
		}
		if !found {
			t.Fatalf("instance %s absent from active actor projection", instanceID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
