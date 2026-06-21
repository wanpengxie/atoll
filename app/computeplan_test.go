package app_test

import (
	"fmt"
	"net/http"
	"testing"
)

// TestComputePlan_DaemonAssignmentOnly verifies the daemon pull endpoint
// (daemon-composition spec §3 / acceptance D4 + B1/B2/B3): introduce a claude
// agent with placement='daemon' and GET /compute/plan returns EXACTLY that
// instance (engine=class → class "claude"); the server-placed boost is NOT in
// it (placement filtering). This is the data the daemon builds — no blind-build.
func TestComputePlan_DaemonAssignmentOnly(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "plan@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	// create a claude agent + introduce it as a DAEMON-placed instance.
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "Reviewer", "looper": "claude"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)
	instID := "agent:" + agentID

	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
		map[string]any{"agent_id": agentID, "placement": "daemon", "make_default": true}, cookies)
	assertStatus(t, w, http.StatusCreated)

	// create a daemon + bind it to the channel (so it may pull).
	w = env.do(t, "POST", "/api/daemons", map[string]any{"name": "mybox"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	dResp := respJSON(t, w)
	daemonID := dResp["id"].(string)
	apiKey := dResp["api_key"].(string)
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons/attach", chID),
		map[string]any{"daemon_ids": []string{daemonID}}, cookies)
	assertStatus(t, w, http.StatusOK)

	// pull the plan (auth by ?key=, no cookie).
	w = env.do(t, "GET", fmt.Sprintf("/compute/plan?key=%s&channel=%s", apiKey, chID), nil, nil)
	assertStatus(t, w, http.StatusOK)
	asgs, ok := respJSON(t, w)["assignments"].([]any)
	if !ok {
		t.Fatalf("assignments missing/not array: %s", w.Body.String())
	}
	if len(asgs) != 1 {
		t.Fatalf("want exactly 1 daemon assignment (the claude), got %d: %v", len(asgs), asgs)
	}
	a0 := asgs[0].(map[string]any)
	if a0["instance_id"] != instID {
		t.Fatalf("assignment instance_id = %v, want %s", a0["instance_id"], instID)
	}
	if a0["class"] != "claude" {
		t.Fatalf("assignment class = %v, want claude (engine=class)", a0["class"])
	}
	for _, a := range asgs {
		if a.(map[string]any)["instance_id"] == "agent:boost" {
			t.Fatalf("server-placed boost must NOT be in the daemon plan: %v", a)
		}
	}
}

// TestComputePlan_ServerOnly_EmptyPlan: a channel with only the server-placed
// boost (no daemon-placed rows) yields an EMPTY daemon plan — the daemon then
// runs nothing (correct: nothing auto-runs).
func TestComputePlan_ServerOnly_EmptyPlan(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "planempty@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	w := env.do(t, "POST", "/api/daemons", map[string]any{"name": "mybox"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	dResp := respJSON(t, w)
	apiKey := dResp["api_key"].(string)
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons/attach", chID),
		map[string]any{"daemon_ids": []string{dResp["id"].(string)}}, cookies)
	assertStatus(t, w, http.StatusOK)

	w = env.do(t, "GET", fmt.Sprintf("/compute/plan?key=%s&channel=%s", apiKey, chID), nil, nil)
	assertStatus(t, w, http.StatusOK)
	asgs, _ := respJSON(t, w)["assignments"].([]any)
	if len(asgs) != 0 {
		t.Fatalf("want empty daemon plan (only server boost exists), got %v", asgs)
	}
}

// TestComputePlan_Auth: missing key → 401; bad/unbound key → 403 (no oracle).
func TestComputePlan_Auth(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "planauth@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	w := env.do(t, "GET", fmt.Sprintf("/compute/plan?channel=%s", chID), nil, nil)
	assertStatus(t, w, http.StatusUnauthorized)

	w = env.do(t, "GET", fmt.Sprintf("/compute/plan?key=bogus&channel=%s", chID), nil, nil)
	assertStatus(t, w, http.StatusForbidden)
}
