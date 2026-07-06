package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// test-tool is a tool-kind class the app test binary otherwise lacks (only
// agent-kind engines are registered), so the cross-kind半失败 retry test can freeze
// a row to a class whose kind differs from the retry request's engine.
func init() {
	registry.Register("test-tool", registry.ClassDecl{
		Kind: actor.KindTool,
		New: func(registry.InstanceSpec, registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{}, fmt.Errorf("test-tool not buildable")
		},
	})
}

// actorKind returns the roster kind the channel records for id (GET /actors,
// backed by the in-gate sysactor actor.list → membership registry), or "".
func actorKind(t *testing.T, env *testEnv, cookies []*http.Cookie, chID, id string) string {
	t.Helper()
	w := env.do(t, "GET", "/api/channels/"+chID+"/actors", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	var body struct {
		Actors []map[string]any `json:"actors"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode actors: %v", err)
	}
	for _, a := range body.Actors {
		if a["id"] == id {
			k, _ := a["kind"].(string)
			return k
		}
	}
	return ""
}

// TestOperate_IntroduceHalfFailedRetry_UsesFrozenClassKind pins the operate-face
// fix: on a半失败 retry (a prior introduce's intent row landed but its Admit did
// not), SW-8 freezes the effective class to the pre-existing row's class. The Admit
// must register under THAT class's kind, not the retry request's (possibly
// different) engine kind — else the member lands with the wrong kind.
func TestOperate_IntroduceHalfFailedRetry_UsesFrozenClassKind(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates an agent (owner may introduce it regardless of visibility).
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "rev", "looper": "claude"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)
	instID := "agent:" + agentID

	// Simulate the half-failed prior introduce: intent row landed with class
	// "test-tool" (kind=tool), placement server, but Admit never landed.
	if err := env.app.SeedIntentRowForTest(s.chID, instID, "test-tool", "server"); err != nil {
		t.Fatalf("seed intent row: %v", err)
	}
	if actorPresent(t, env, s.cookies, s.chID, instID) {
		t.Fatalf("instance a member before the healing retry")
	}

	// Retry the introduce with a DIFFERENT class (claude, kind=agent). The frozen
	// row's class is test-tool → the Admit must use its kind (tool), not claude's.
	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"agent_id": agentID, "engine": "claude"})
	if _, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    actor.ActorID("user:" + s.userID),
		Payload:   payload,
	}); err != nil {
		t.Fatalf("Introduce retry: %v", err)
	}

	if got := actorKind(t, env, s.cookies, s.chID, instID); got != string(actor.KindTool) {
		t.Fatalf("member kind = %q, want %q (frozen echo class-kind, not the request's claude/agent)",
			got, actor.KindTool)
	}
}

// TestCreateChannel_SeedAdmitFails_RollsBack pins the create-channel transaction
// fix: a failed seeding Admit (creator or boost) must tear the channel down (close
// home + delete the row) and return 5xx — never a silent 201 over a channel whose
// creator is not a member.
func TestCreateChannel_SeedAdmitFails_RollsBack(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "rollback@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)

	env.app.SetSeedAdmitFailForTest(true)
	w := env.do(t, "POST", fmt.Sprintf("/api/workspaces/%s/channels", wsID),
		map[string]any{"name": "doomed"}, cookies)
	assertStatus(t, w, http.StatusInternalServerError)
	env.app.SetSeedAdmitFailForTest(false)

	// The channel row must be rolled back — it appears in no listing.
	w = env.do(t, "GET", fmt.Sprintf("/api/workspaces/%s/channels", wsID), nil, cookies)
	assertStatus(t, w, http.StatusOK)
	chans, _ := respJSON(t, w)["channels"].([]any)
	if len(chans) != 0 {
		t.Fatalf("channel row not rolled back after seed-admit failure: %v", chans)
	}
}

// TestDeleteDaemon_RevokePersistFails_Returns5xx pins the daemon-delete fix: if the
// revocation cannot reach durable storage, the handler must return 5xx (not a false
// ok) and leave the daemon intact — never silently drop the key while reporting
// success.
func TestDeleteDaemon_RevokePersistFails_Returns5xx(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "revoke@example.com", "secret123", "Owner")

	w := env.do(t, "POST", "/api/daemons", map[string]any{"name": "box"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonID := respJSON(t, w)["id"].(string)

	env.app.SetRevokeFailForTest(true)
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies)
	assertStatus(t, w, http.StatusInternalServerError)
	env.app.SetRevokeFailForTest(false)

	// The daemon must still exist — revocation was not persisted, so not reported ok.
	w = env.do(t, "GET", "/api/daemons", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	ds, _ := respJSON(t, w)["daemons"].([]any)
	found := false
	for _, d := range ds {
		if d.(map[string]any)["id"] == daemonID {
			found = true
		}
	}
	if !found {
		t.Fatalf("daemon deleted despite revocation-persist failure (false ok)")
	}
}
