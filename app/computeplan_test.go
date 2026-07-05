package app_test

import (
	"fmt"
	"net/http"
	"testing"
)

// TestComputePlan_DaemonAssignmentOnly verifies the daemon pull endpoint:
// introduce a claude agent with placement='daemon' and GET /compute/plan
// returns EXACTLY that instance (engine=class → class "claude"); the
// server-placed boost is NOT in it (placement filtering). This is the data
// the daemon builds — no blind-build.
func TestComputePlan_DaemonAssignmentOnly(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "plan@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	// create a claude agent.
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "Reviewer", "looper": "claude"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)
	instID := "agent:" + agentID

	// create a daemon + bind it to the channel (so it may pull).
	w = env.do(t, "POST", "/api/daemons", map[string]any{"name": "mybox"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	dResp := respJSON(t, w)
	daemonID := dResp["id"].(string)
	apiKey := dResp["api_key"].(string)
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons/attach", chID),
		map[string]any{"daemon_ids": []string{daemonID}}, cookies)
	assertStatus(t, w, http.StatusOK)

	// introduce the claude as a DAEMON-placed instance assigned to THIS daemon.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
		map[string]any{"agent_id": agentID, "placement": "daemon", "desired_host": daemonID, "make_default": true}, cookies)
	assertStatus(t, w, http.StatusCreated)

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

// TestComputePlan_DesiredHostMutex (G4): two daemons bound to one channel each
// pull ONLY the daemon-placed rows assigned to their own id (desired_host); a
// pool row (desired_host='') is delivered to NEITHER.
func TestComputePlan_DesiredHostMutex(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "g4@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	mkAgent := func(name string) string {
		w := env.do(t, "POST", "/api/agents", map[string]any{"name": name, "looper": "claude"}, cookies)
		assertStatus(t, w, http.StatusCreated)
		return respJSON(t, w)["id"].(string)
	}
	mkDaemon := func(name string) (id, key string) {
		w := env.do(t, "POST", "/api/daemons", map[string]any{"name": name}, cookies)
		assertStatus(t, w, http.StatusCreated)
		d := respJSON(t, w)
		w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons/attach", chID),
			map[string]any{"daemon_ids": []string{d["id"].(string)}}, cookies)
		assertStatus(t, w, http.StatusOK)
		return d["id"].(string), d["api_key"].(string)
	}
	introduce := func(agentID, desiredHost string) {
		w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
			map[string]any{"agent_id": agentID, "placement": "daemon", "desired_host": desiredHost}, cookies)
		assertStatus(t, w, http.StatusCreated)
	}
	planInstances := func(key string) map[string]bool {
		w := env.do(t, "GET", fmt.Sprintf("/compute/plan?key=%s&channel=%s", key, chID), nil, nil)
		assertStatus(t, w, http.StatusOK)
		got := map[string]bool{}
		if asgs, ok := respJSON(t, w)["assignments"].([]any); ok {
			for _, a := range asgs {
				got[a.(map[string]any)["instance_id"].(string)] = true
			}
		}
		return got
	}

	d1ID, d1Key := mkDaemon("box1")
	d2ID, d2Key := mkDaemon("box2")
	a1 := mkAgent("A1")
	a2 := mkAgent("A2")
	aPool := mkAgent("APool")
	introduce(a1, d1ID)
	introduce(a2, d2ID)
	introduce(aPool, "") // unassigned pool row

	p1 := planInstances(d1Key)
	if !p1["agent:"+a1] || p1["agent:"+a2] || p1["agent:"+aPool] || len(p1) != 1 {
		t.Fatalf("daemon1 plan should be exactly {agent:%s}, got %v", a1, p1)
	}
	p2 := planInstances(d2Key)
	if !p2["agent:"+a2] || p2["agent:"+a1] || p2["agent:"+aPool] || len(p2) != 1 {
		t.Fatalf("daemon2 plan should be exactly {agent:%s}, got %v", a2, p2)
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
