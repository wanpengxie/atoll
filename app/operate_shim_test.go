package app_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// operate_shim_test.go is the DoD for the HTTP垫片 (NP-1=c): the four channel-control
// endpoints replay the session user through the door and reflect the door's terminal
// reply. Each type gets one end-to-end case that asserts (a) the shim's HTTP result
// matches the door receipt and (b) the request in the log is 笔为 user:X with a
// completed terminal. Plus the non-member 403 (膜律) and timeout 202 branches.

// channelMessages reads the whole channel log through GET /messages (the raw
// read surface), returning the {seq, is_terminal, envelope} rows.
func channelMessages(t *testing.T, env *testEnv, cookies []*http.Cookie, chID string) []map[string]any {
	t.Helper()
	w := env.do(t, "GET", "/api/channels/"+chID+"/messages?limit=1000", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	var rows []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	return rows
}

// assertDoorTerminal asserts the log holds a control request of type msgType
// authored by user:userID (笔为 user:X) and its completed terminal (parent_id match)
// — the door actually processed the shim's submission.
func assertDoorTerminal(t *testing.T, env *testEnv, cookies []*http.Cookie, chID, userID, msgType string) {
	t.Helper()
	rows := channelMessages(t, env, cookies, chID)
	reqID := ""
	for _, r := range rows {
		e, _ := r["envelope"].(map[string]any)
		if e == nil || e["kind"] != "request" || e["type"] != msgType {
			continue
		}
		sender, _ := e["sender"].(map[string]any)
		if sender["id"] != "user:"+userID {
			t.Fatalf("%s request sender = %v, want user:%s", msgType, sender["id"], userID)
		}
		reqID, _ = e["id"].(string)
	}
	if reqID == "" {
		t.Fatalf("no %s request from user:%s in the log", msgType, userID)
	}
	for _, r := range rows {
		e, _ := r["envelope"].(map[string]any)
		if e == nil || e["kind"] != "response" || e["parent_id"] != reqID {
			continue
		}
		if term, _ := r["is_terminal"].(bool); !term {
			continue
		}
		payload, _ := e["payload"].(map[string]any)
		if payload["status"] != "completed" {
			t.Fatalf("%s terminal status = %v, want completed", msgType, payload["status"])
		}
		return
	}
	t.Fatalf("no completed terminal for %s request %s", msgType, reqID)
}

// createOwnedAgent creates an agent owned by the session user and returns its id.
func createOwnedAgent(t *testing.T, env *testEnv, cookies []*http.Cookie, name, looper string) string {
	t.Helper()
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": name, "looper": looper}, cookies)
	assertStatus(t, w, http.StatusCreated)
	return respJSON(t, w)["id"].(string)
}

// TestShim_IntroduceThroughDoor: POST /channels/:id/agents replays the user through
// channel.introduce_actor; the composition row lands + the door terminal is 笔为 user:X.
func TestShim_IntroduceThroughDoor(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	agentID := createOwnedAgent(t, env, s.cookies, "Rev", "go-kimi")

	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", s.chID),
		map[string]any{"agent_id": agentID, "placement": "server"}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	if got := respJSON(t, w)["instance_id"]; got != "agent:"+agentID {
		t.Fatalf("introduce instance_id = %v, want agent:%s", got, agentID)
	}
	assertDoorTerminal(t, env, s.cookies, s.chID, s.userID, "channel.introduce_actor")
}

// TestShim_SetDefaultThroughDoor: PUT /channels/:id/default_agent replays the user
// through channel.set_default_agent; the pointer moves + terminal is 笔为 user:X.
func TestShim_SetDefaultThroughDoor(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	agentID := createOwnedAgent(t, env, s.cookies, "Def", "go-kimi")
	instID := "agent:" + agentID
	env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", s.chID),
		map[string]any{"agent_id": agentID, "placement": "server"}, s.cookies)

	w := env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", s.chID),
		map[string]any{"instance_id": instID}, s.cookies)
	assertStatus(t, w, http.StatusOK)
	if got := respJSON(t, w)["default_agent"]; got != instID {
		t.Fatalf("default_agent = %v, want %s", got, instID)
	}
	assertDoorTerminal(t, env, s.cookies, s.chID, s.userID, "channel.set_default_agent")
}

// TestShim_RemoveThroughDoor: DELETE /channels/:id/actors/:instanceID replays the
// user through channel.remove_actor; the actor leaves the roster + terminal 笔为 user:X.
func TestShim_RemoveThroughDoor(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	agentID := createOwnedAgent(t, env, s.cookies, "Rm", "go-kimi")
	instID := "agent:" + agentID
	env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", s.chID),
		map[string]any{"agent_id": agentID, "placement": "server"}, s.cookies)
	if !actorPresent(t, env, s.cookies, s.chID, instID) {
		t.Fatalf("agent not admitted before remove")
	}

	w := env.do(t, "DELETE", fmt.Sprintf("/api/channels/%s/actors/%s", s.chID, instID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	if got := respJSON(t, w)["removed"]; got != instID {
		t.Fatalf("removed = %v, want %s", got, instID)
	}
	if actorPresent(t, env, s.cookies, s.chID, instID) {
		t.Fatalf("agent still in roster after remove")
	}
	assertDoorTerminal(t, env, s.cookies, s.chID, s.userID, "channel.remove_actor")
}

// TestShim_RestartThroughDoor: POST /agents/:id/restart replays each per-channel
// restart through channel.restart_actor; count >= 1 + the door terminal is 笔为 user:X.
func TestShim_RestartThroughDoor(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	agentID := createOwnedAgent(t, env, s.cookies, "Res", "claude")
	env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", s.chID),
		map[string]any{"agent_id": agentID, "placement": "server", "make_default": true}, s.cookies)

	w := env.do(t, "POST", "/api/agents/"+agentID+"/restart", nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	if n, _ := respJSON(t, w)["restarted"].(float64); n < 1 {
		t.Fatalf("restarted = %v, want >= 1", n)
	}
	assertDoorTerminal(t, env, s.cookies, s.chID, s.userID, "channel.restart_actor")
}

// TestShim_NonMemberForbidden (膜律): a workspace member who is NOT a channel member
// is refused by the door's户籍校验 — the shim NEVER admits as a fallback (严禁 Admit 兜底).
func TestShim_NonMemberForbidden(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	outsider, cookies2 := register(t, env, "outsider@example.com", "secret123", "Outsider")
	if err := env.app.AddWorkspaceMemberForTest(s.wsID, outsider["id"].(string)); err != nil {
		t.Fatalf("add ws member: %v", err)
	}

	// Passes the workspace ACL (requireChannelAccess) but the door rejects: not a
	// channel member → 403, and no membership is minted as a side effect.
	w := env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", s.chID),
		map[string]any{"instance_id": ""}, cookies2)
	assertStatus(t, w, http.StatusForbidden)
}

// TestShim_TimeoutReturns202: when the door does not settle within the bounded wait,
// the shim returns 202 + request_id (前端语义不变). A near-zero timeout forces it.
func TestShim_TimeoutReturns202(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	env.app.SetControlShimTimeoutForTest(time.Nanosecond)

	w := env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", s.chID),
		map[string]any{"instance_id": ""}, s.cookies)
	assertStatus(t, w, http.StatusAccepted)
	if rid, _ := respJSON(t, w)["request_id"].(string); rid == "" {
		t.Fatalf("202 response missing request_id: %s", w.Body.String())
	}
}
