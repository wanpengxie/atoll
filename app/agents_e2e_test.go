package app_test

import (
	"fmt"
	"net/http"
	"testing"
)

// TestAgentsAPI_CreateIntroduceRestartDelete exercises the agent creation and
// control surface: create a claude-looper agent (declaration), introduce it to a channel (which
// inserts the composition row + spawns it live via the stub "agent" class),
// restart it (rebuild + Spawn), then soft-delete it (gone from the list +
// composition). Proves the agents table + two-layer/looper composition + the
// control API end to end.
func TestAgentsAPI_CreateIntroduceRestartDelete(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "agents@example.com", "secret123", "AgentOwner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	// create a claude-looper agent
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "Researcher", "looper": "claude"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agent := respJSON(t, w)
	agentID, _ := agent["id"].(string)
	if agentID == "" || agent["looper"] != "claude" {
		t.Fatalf("create agent = %+v", agent)
	}

	// list returns it
	w = env.do(t, "GET", "/api/agents", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	if got := len(respJSON(t, w)["agents"].([]any)); got != 1 {
		t.Fatalf("list agents = %d, want 1", got)
	}

	// introduce to channel → composition row + live stub spawn
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
		map[string]any{"agent_id": agentID, "make_default": true}, cookies)
	assertStatus(t, w, http.StatusCreated)
	intro := respJSON(t, w)
	if intro["instance_id"] != "agent:"+agentID || intro["looper"] != "claude" {
		t.Fatalf("introduce = %+v", intro)
	}
	if intro["live"] != true {
		t.Fatalf("introduce should spawn live via the stub agent class: %+v", intro)
	}

	// restart rebuilds the server-placed cell(s)
	w = env.do(t, "POST", "/api/agents/"+agentID+"/restart", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	if n, _ := respJSON(t, w)["restarted"].(float64); n < 1 {
		t.Fatalf("restart count = %v, want >= 1", n)
	}

	// soft-delete: gone from the list + composition
	w = env.do(t, "DELETE", "/api/agents/"+agentID, nil, cookies)
	assertStatus(t, w, http.StatusOK)
	w = env.do(t, "GET", "/api/agents", nil, cookies)
	if got := len(respJSON(t, w)["agents"].([]any)); got != 0 {
		t.Fatalf("after delete, list = %d, want 0", got)
	}
}

// TestSetDefaultAgentAPI exercises the "repoint the default brain" endpoint:
// re-point default_agent to an instance that IS in the channel composition (ok),
// reject one that is NOT (the pointer may only target a composition member), and
// clear it. Pairs with the routing failover (boost floor) that consumes it.
func TestSetDefaultAgentAPI(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "setdef@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	// create + introduce an agent (server placement → live stub, lands in channel_actors)
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "Alice", "looper": "go-kimi"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)
	instID := "agent:" + agentID
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
		map[string]any{"agent_id": agentID}, cookies) // not make_default
	assertStatus(t, w, http.StatusCreated)

	// 1) repoint default_agent to Alice (a composition member) → ok + persisted
	w = env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", chID),
		map[string]any{"instance_id": instID}, cookies)
	assertStatus(t, w, http.StatusOK)
	w = env.do(t, "GET", "/api/channels/"+chID, nil, cookies)
	assertStatus(t, w, http.StatusOK)
	if da := respJSON(t, w)["default_agent"]; da != instID {
		t.Fatalf("default_agent not repointed/persisted: got %v want %s", da, instID)
	}

	// 2) point at a non-member instance → 400 (pointer must target the composition)
	w = env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", chID),
		map[string]any{"instance_id": "agent:ghost"}, cookies)
	assertStatus(t, w, http.StatusBadRequest)

	// 3) clear (empty instance_id) → ok + NULLed
	w = env.do(t, "PUT", fmt.Sprintf("/api/channels/%s/default_agent", chID),
		map[string]any{"instance_id": ""}, cookies)
	assertStatus(t, w, http.StatusOK)
	w = env.do(t, "GET", "/api/channels/"+chID, nil, cookies)
	if da := respJSON(t, w)["default_agent"]; da != "" {
		t.Fatalf("default_agent should be cleared, got %v", da)
	}
}
