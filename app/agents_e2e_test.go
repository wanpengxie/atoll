package app_test

import (
	"fmt"
	"net/http"
	"testing"
)

// TestAgentsAPI_CreateIntroduceRestartDelete exercises the §五 创建与控制 face:
// create a claude-looper agent (declaration), introduce it to a channel (which
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
