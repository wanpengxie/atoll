package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// actorPresent reports whether id is in the channel's actor roster (GET /actors,
// backed by the in-gate sysactor actor.list → membership registry).
func actorPresent(t *testing.T, env *testEnv, cookies []*http.Cookie, chID, id string) bool {
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
			return true
		}
	}
	return false
}

// TestOperate_IntroduceUserForm_Admits proves the user-form introduce动词 admits a
// user as a pure membership row (膜律: 户籍唯一写入=显式动词), reachable via the
// channel roster.
func TestOperate_IntroduceUserForm_Admits(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	target := "user:invitee"
	if actorPresent(t, env, s.cookies, s.chID, target) {
		t.Fatalf("target %q already a member before introduce", target)
	}
	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"target": target})
	if _, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    actor.ActorID("user:" + s.userID),
		Payload:   payload,
	}); err != nil {
		t.Fatalf("Introduce(user form): %v", err)
	}
	if !actorPresent(t, env, s.cookies, s.chID, target) {
		t.Fatalf("target %q not a member after user-form introduce", target)
	}
}

// TestOperate_IntroducePrivateAgent_Forbidden proves the world-layer二型律: a
// private agent may only be introduced by its owner — a non-owner principal is
// refused with error_code=forbidden, and no intent/户籍 row lands.
func TestOperate_IntroducePrivateAgent_Forbidden(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates a PRIVATE agent (default visibility).
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "secret", "looper": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &ag); err != nil {
		t.Fatalf("decode agent: %v", err)
	}
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"agent_id": agentID})
	// A DIFFERENT principal (not the owner) tries to introduce it.
	_, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    "user:someone-else",
		Payload:   payload,
	})
	var oe *platform.OperateError
	if err == nil {
		t.Fatalf("non-owner introduce of private agent succeeded, want forbidden")
	}
	if !errors.As(err, &oe) || oe.Code != "forbidden" {
		t.Fatalf("want error_code=forbidden, got %v", err)
	}
	if actorPresent(t, env, s.cookies, s.chID, "agent:"+agentID) {
		t.Fatalf("private agent embodied despite forbidden introduce")
	}
}

// TestOperate_IntroduceUnknownClass_Rejected proves the ClassKind precheck: an
// unknown engine class is rejected当场 (error_code=unknown_class) — no intent row.
func TestOperate_IntroduceUnknownClass_Rejected(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates a public agent whose default looper is a bogus class.
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "bogus", "looper": "no-such-class"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &ag)
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"agent_id": agentID})
	_, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    actor.ActorID("user:" + s.userID),
		Payload:   payload,
	})
	var oe *platform.OperateError
	if err == nil || !errors.As(err, &oe) || oe.Code != "unknown_class" {
		t.Fatalf("want error_code=unknown_class, got %v", err)
	}
}
