package app_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/realmtool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func realmToolRequest(t *testing.T, env *testEnv, setup setupResult, client *wsClient, tool actor.ActorID, typ string, payload any) map[string]json.RawMessage {
	t.Helper()
	ack := client.sendMessage(map[string]any{
		"channel_id": setup.chID, "msg_type": typ, "kind": "request",
		"audience": []string{string(tool)}, "payload": payload,
	})
	if ack["type"] != "ack" {
		t.Fatalf("realm request ack=%v", ack)
	}
	raw := waitForResponse(t, env, setup, ack["message_id"].(string), 5*time.Second)
	var response map[string]json.RawMessage
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("decode realm response %s: %v", raw, err)
	}
	return response
}

func TestRealmToolBuiltInListCreateAndInspect(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	toolID, err := env.app.ResolveSourceForTest(setup.chID, "realm-tool")
	if err != nil {
		t.Fatal(err)
	}
	if !env.app.WaitLiveForTest(setup.chID, toolID, 2*time.Second) {
		t.Fatal("realm tool did not become live")
	}
	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	client := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer client.close()

	created := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeCreateDeclaration, map[string]any{
		"name": "created in channel", "class": "go-kimi", "visibility": "public", "config": map[string]any{},
	})
	if string(created["status"]) != `"completed"` {
		t.Fatalf("create response=%v", created)
	}
	var wrapper struct {
		Declaration struct {
			ID    string `json:"id"`
			Owner string `json:"owner"`
		} `json:"declaration"`
	}
	rawCreate, _ := json.Marshal(created)
	if err := json.Unmarshal(rawCreate, &wrapper); err != nil || wrapper.Declaration.ID == "" || wrapper.Declaration.Owner != setup.userID {
		t.Fatalf("created declaration=%+v err=%v raw=%s", wrapper, err, rawCreate)
	}

	listed := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeListDeclarations, map[string]any{})
	var declarations []map[string]any
	if err := json.Unmarshal(listed["declarations"], &declarations); err != nil {
		t.Fatalf("list response=%v err=%v", listed, err)
	}
	found := false
	for _, declaration := range declarations {
		if declaration["id"] == wrapper.Declaration.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("created declaration absent from list: %v", declarations)
	}

	inspected := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeInspectDeclaration, map[string]any{"decl_id": wrapper.Declaration.ID})
	var detail map[string]any
	if err := json.Unmarshal(inspected["declaration"], &detail); err != nil || detail["id"] != wrapper.Declaration.ID {
		t.Fatalf("inspect=%v detail=%v err=%v", inspected, detail, err)
	}
}

func TestRealmToolActorOwnedIntroduceAndRemove(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	createAndBindDaemon(t, env, setup.chID, "actor-owned-host", setup.cookies)
	toolID, err := env.app.ResolveSourceForTest(setup.chID, "realm-tool")
	if err != nil {
		t.Fatal(err)
	}
	if !env.app.WaitLiveForTest(setup.chID, toolID, 2*time.Second) {
		t.Fatal("realm tool did not become live")
	}
	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	client := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer client.close()
	decl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "actor-owned", "class": "go-kimi", "visibility": "public",
	}, setup.cookies)
	assertStatus(t, decl, http.StatusCreated)
	declID := respJSON(t, decl)["id"].(string)

	introduced := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeIntroduce, map[string]any{"decl_id": declID})
	var target actor.ActorID
	if err := json.Unmarshal(introduced["actor_id"], &target); err != nil || target == "" {
		t.Fatalf("actor-owned introduce=%v target=%q err=%v", introduced, target, err)
	}
	var principal, ownerActor string
	if err := env.db.QueryRow(`SELECT COALESCE(requested_by_principal,''),COALESCE(requested_by_actor_id,'')
		FROM channel_admission_operations WHERE channel_id=? AND op='introduce' ORDER BY created_at DESC LIMIT 1`, setup.chID).
		Scan(&principal, &ownerActor); err != nil {
		t.Fatal(err)
	}
	if principal != "" || ownerActor != string(setup.actorID) {
		t.Fatalf("operation owner=(principal=%q actor=%q), want sender actor %q", principal, ownerActor, setup.actorID)
	}

	removed := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeRemove, map[string]any{"target": target})
	var removedIDs []actor.ActorID
	if err := json.Unmarshal(removed["removed"], &removedIDs); err != nil || len(removedIDs) != 1 || removedIDs[0] != target {
		t.Fatalf("actor-owned remove=%v ids=%v err=%v", removed, removedIDs, err)
	}
	if _, err := env.app.ResolveSourceForTest(setup.chID, declID); err == nil {
		t.Fatal("realm-tool remove left target active")
	}
}

func TestRealmToolFetchCopiesCrossChannelResourceWithProvenance(t *testing.T) {
	env := setupTestApp(t)
	target := fullSetup(t, env)

	_, sourceCookies := register(t, env, "source@example.com", "secret123", "Source Owner")
	sourceBody, sourceCookies := createChannel(t, env, sourceCookies, "source-artifacts")
	sourceID := sourceBody["id"].(string)

	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	sourceClient := dialWS(t, srv, sourceCookies, sourceID, 0)
	defer sourceClient.close()
	sourceClient.send(map[string]any{
		"type": "resource", "ref": "create-source-resource", "channel_id": sourceID,
		"op": "create", "resource_id": "artifact:source", "args": map[string]any{"answer": 42},
	})
	if ack := sourceClient.nextAck(3 * time.Second); ack["type"] != "ack" || ack["status"] != "ok" {
		t.Fatalf("source resource create=%v", ack)
	}

	toolID, err := env.app.ResolveSourceForTest(target.chID, "realm-tool")
	if err != nil {
		t.Fatal(err)
	}
	if !env.app.WaitLiveForTest(target.chID, toolID, 2*time.Second) {
		t.Fatal("target realm tool did not become live")
	}
	targetClient := dialWS(t, srv, target.cookies, target.chID, 0)
	defer targetClient.close()
	fetched := realmToolRequest(t, env, target, targetClient, toolID, realmtool.TypeFetchResource, channel.ResourceRef{
		ChannelID: channel.ID(sourceID), ResourceID: "artifact:source",
	})
	var newID string
	if err := json.Unmarshal(fetched["resource_id"], &newID); err != nil || newID == "" {
		t.Fatalf("fetch response=%v newID=%q err=%v", fetched, newID, err)
	}

	stat := env.do(t, http.MethodGet, "/api/channels/"+target.chID+"/resources/"+newID, nil, target.cookies)
	assertStatus(t, stat, http.StatusOK)
	meta := respJSON(t, stat)
	if meta["source_channel_id"] != sourceID || meta["source_resource_id"] != "artifact:source" {
		t.Fatalf("copied provenance=%v", meta)
	}
	bytesResp := env.do(t, http.MethodGet, "/api/channels/"+target.chID+"/resources/"+newID+"/bytes", nil, target.cookies)
	assertStatus(t, bytesResp, http.StatusOK)
	if got := bytesResp.Body.String(); got != `{"answer":42}` {
		t.Fatalf("copied bytes=%q", got)
	}

	// The copy is self-contained: retiring the source makes a new fetch fail,
	// while the already-copied target resource remains complete and readable.
	destroy := env.do(t, http.MethodDelete, "/api/channels/"+sourceID, nil, sourceCookies)
	if destroy.Code != http.StatusAccepted && destroy.Code != http.StatusNoContent {
		t.Fatalf("destroy source status=%d body=%s", destroy.Code, destroy.Body.String())
	}
	failed := realmToolRequest(t, env, target, targetClient, toolID, realmtool.TypeFetchResource, channel.ResourceRef{
		ChannelID: channel.ID(sourceID), ResourceID: "artifact:source",
	})
	if string(failed["error_code"]) != `"channel_unavailable"` {
		t.Fatalf("fetch retired source=%v", failed)
	}
	bytesResp = env.do(t, http.MethodGet, "/api/channels/"+target.chID+"/resources/"+newID+"/bytes", nil, target.cookies)
	assertStatus(t, bytesResp, http.StatusOK)
	if got := bytesResp.Body.String(); got != `{"answer":42}` {
		t.Fatalf("copied bytes after source retirement=%q", got)
	}
}

func TestRealmToolPrivateIntroductionAndSovereigntySwitch(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	createAndBindDaemon(t, env, setup.chID, "realm-tool-introduce-host", setup.cookies)
	toolID, err := env.app.ResolveSourceForTest(setup.chID, "realm-tool")
	if err != nil {
		t.Fatal(err)
	}
	if !env.app.WaitLiveForTest(setup.chID, toolID, 2*time.Second) {
		t.Fatal("realm tool did not become live")
	}
	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	client := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer client.close()

	privateDecl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "private introduction", "class": "go-kimi", "visibility": "private",
	}, setup.cookies)
	assertStatus(t, privateDecl, http.StatusCreated)
	privateID := respJSON(t, privateDecl)["id"].(string)
	denied := realmToolRequest(t, env, setup, client, toolID, realmtool.TypeIntroduce, map[string]any{"decl_id": privateID})
	if string(denied["error_code"]) != `"forbidden"` {
		t.Fatalf("realm-tool private introduce=%v", denied)
	}
	external := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": privateID}, setup.cookies)
	assertStatus(t, external, http.StatusCreated)

	publicDecl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "public after switch", "class": "go-kimi", "visibility": "public",
	}, setup.cookies)
	assertStatus(t, publicDecl, http.StatusCreated)
	publicID := respJSON(t, publicDecl)["id"].(string)
	if err := env.app.RevokeRealmToolForTest(channel.ID(setup.chID)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.app.ResolveSourceForTest(setup.chID, "realm-tool"); err == nil {
		t.Fatal("realm tool remained in the channel after sovereignty switch")
	}
	ack := client.sendMessage(map[string]any{
		"channel_id": setup.chID, "msg_type": realmtool.TypeIntroduce, "kind": "request",
		"audience": []string{string(toolID)}, "payload": map[string]any{"decl_id": publicID},
	})
	if ack["type"] != "error" {
		t.Fatalf("removed realm-tool request=%v", ack)
	}
	external = env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": publicID}, setup.cookies)
	assertStatus(t, external, http.StatusCreated)
	// The sovereignty switch does not revoke a member's intrinsic read pen.
	memberRead := env.do(t, http.MethodGet, "/api/channels/"+setup.chID+"/messages", nil, setup.cookies)
	assertStatus(t, memberRead, http.StatusOK)
}
