package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
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
	payload, _ := json.Marshal(map[string]any{"principal": strings.TrimPrefix(target, "user:")})
	got, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    s.actorID,
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("Introduce(user form): %v", err)
	}
	admitted := string(got.(map[string]any)["admitted"].(actor.ActorID))
	if !actorPresent(t, env, s.cookies, s.chID, admitted) {
		t.Fatalf("minted target %q not a member after user-form introduce", admitted)
	}
}

// TestOperate_IntroducePrivateAgent_Forbidden proves the world-layer二型律: a
// private agent may only be introduced by its owner — a non-owner principal is
// refused with error_code=forbidden, and no intent/户籍 row lands.
func TestOperate_IntroducePrivateAgent_Forbidden(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates a PRIVATE agent (default visibility).
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "secret", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &ag); err != nil {
		t.Fatalf("decode agent: %v", err)
	}
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"decl_id": agentID})
	// A DIFFERENT principal (not the owner) tries to introduce it.
	_, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    "user:someone-else",
		Payload:   payload,
	})
	var oe *home.OperateError
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
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "bogus", "class": "no-such-class"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &ag)
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"decl_id": agentID})
	_, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    s.actorID,
		Payload:   payload,
	})
	var oe *home.OperateError
	if err == nil || !errors.As(err, &oe) || oe.Code != "unknown_class" {
		t.Fatalf("want error_code=unknown_class, got %v", err)
	}
}

// TestOperate_IntroduceInvalidPlacement_Rejected proves the placement闭集 guard (#5):
// an explicit garbage placement is fail-closed (error_code=invalid_placement) — the
// same posture as unknown_class — and NO channel_composition row lands (rejected before the
// INSERT). Empty placement still defaults to daemon (unaffected).
func TestOperate_IntroduceInvalidPlacement_Rejected(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates a real-class agent (owner may introduce its own regardless of
	// visibility), so the only reason to reject is the bad placement value.
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "ok", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &ag)
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()
	payload, _ := json.Marshal(map[string]any{"decl_id": agentID, "placement": "foo"})
	_, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID),
		Sender:    s.actorID,
		Payload:   payload,
	})
	var oe *home.OperateError
	if err == nil || !errors.As(err, &oe) || oe.Code != "invalid_placement" {
		t.Fatalf("want error_code=invalid_placement, got %v", err)
	}
	// Not persisted: fail-closed BEFORE the INSERT, so the agent never became a member.
	if actorPresent(t, env, s.cookies, s.chID, "agent:"+agentID) {
		t.Fatalf("agent embodied despite invalid placement (row landed)")
	}
}

// TestOperate_ConfigChange_DaemonPlacedTakesEffect is the config-effect arm's
// placement-neutrality regression guard (F-2, P2-2). A config change on an
// already-composed row takes effect through operateExecutor.Introduce's
// configChanged branch, which calls the placement-neutral Home.Restart for a
// daemon-placed row exactly as it would for a server one. (Driven on the operate
// executor directly, as the sysactor gate would after authorising the sender —
// the HTTP introduce垫片 does not forward config, so the config-effect arm is only
// reachable via a frame-borne payload, which this test stands in for.)
//
// It introduces a DAEMON-placed row, then re-introduces it with a CHANGED config,
// and asserts the config-effect path ran to success FOR THE DAEMON ROW: no error
// (a placement-gated / rebuild_failed Restart would surface as *home.OperateError),
// created=false, placement=daemon, config_updated=true.
//
// Guard boundary: the innermost Home.Restart *invocation* for a daemon row has no
// observable production seam here without a real attached daemon (the platform-side
// TestRestartDaemonPlacedActor_RebuildsAcrossWire owns the actual cross-wire
// rebuild). This guards that the config-effect branch is NOT gated on placement at
// its entry and completes for a daemon-placed row.
func TestOperate_ConfigChange_DaemonPlacedTakesEffect(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Owner creates a real-class agent (owner may introduce its own).
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "cfg", "class": "go-kimi"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &ag)
	agentID := ag["id"].(string)

	face := env.app.OperateFaceForTest()

	// First introduce: NO placement → default policy = daemon, WITH an initial config.
	p1, _ := json.Marshal(map[string]any{"decl_id": agentID, "config": map[string]any{"tone": "calm"}})
	got1, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID), Sender: s.actorID, Payload: p1,
	})
	if err != nil {
		t.Fatalf("first introduce: %v", err)
	}
	m1 := got1.(map[string]any)
	if m1["placement"] != "daemon" || m1["created"] != true {
		t.Fatalf("first introduce = %+v, want daemon/created (the guarded case)", m1)
	}

	// Re-introduce the SAME row with a CHANGED config → the configChanged branch:
	// UPDATE channel_composition.config_json + the placement-neutral Home.Restart for the
	// daemon row. A placement-gated effect Restart would fail here (rebuild_failed)
	// or never mark config_updated.
	p2, _ := json.Marshal(map[string]any{"decl_id": agentID, "config": map[string]any{"tone": "brisk"}})
	got2, err := face.Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(s.chID), Sender: s.actorID, Payload: p2,
	})
	if err != nil {
		t.Fatalf("config-change re-introduce for a daemon-placed row failed: %v", err)
	}
	m2 := got2.(map[string]any)
	if m2["created"] != false {
		t.Fatalf("re-introduce should report created=false: %+v", m2)
	}
	if m2["placement"] != "daemon" {
		t.Fatalf("re-introduce placement = %v, want daemon (config-effect must reach the daemon row)", m2["placement"])
	}
	if m2["config_updated"] != true {
		t.Fatalf("config_updated = %v, want true — the config-effect branch did not run/complete for the daemon-placed row (F-2 regression)", m2["config_updated"])
	}
}
