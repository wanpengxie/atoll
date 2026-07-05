package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// TestDaemonComposition_E2E is the daemon-composition acceptance test: it runs
// the FULL daemon flow against a real server —
//
//	introduce claude (placement='daemon')
//	  → PULL the assignment from GET /compute/plan         (what daemon does 1st)
//	  → BUILD decls from it via registry.Build             (no blind-build)
//	  → ATTACH over a real /compute link (platform.RunCompute)
//	  → the agent becomes a LIVE channel member (actor_registry / ListActors)
//
// The app_test stub stands in for the claude engine (registry.Build("claude")
// → stub; no CLI), exactly the seam the unit tests use. This proves pull → build
// → attach → member wired end to end; the attach→member leg is the existing,
// unchanged link path.
func TestDaemonComposition_E2E(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.handler)
	defer srv.Close()

	_, cookies := register(t, env, "e2e@example.com", "secret123", "Owner")
	wsBody, cookies := createWorkspace(t, env, cookies, "WS")
	wsID := wsBody["id"].(string)
	chBody, cookies := createChannel(t, env, cookies, wsID, "CH")
	chID := chBody["id"].(string)

	// claude agent.
	w := env.do(t, "POST", "/api/agents", map[string]any{"name": "Rev", "looper": "claude"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)
	instID := "agent:" + agentID

	// create + bind a daemon (one call) → id + api_key.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons", chID),
		map[string]any{"name": "mybox"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	dResp := respJSON(t, w)
	daemonID := dResp["id"].(string)
	apiKey := dResp["api_key"].(string)

	// introduce the claude as a DAEMON-placed default assigned to THIS daemon.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/agents", chID),
		map[string]any{"agent_id": agentID, "placement": "daemon", "desired_host": daemonID, "make_default": true}, cookies)
	assertStatus(t, w, http.StatusCreated)
	// 膜律 (v1.8 问①): daemon attach no longer mints membership — the daemon-placed
	// actor must be admitted by the introduce door first. The old HTTP introduce
	// path writes intent only; the operate-face executor (CORE1③) is what Admits in
	// production. Stand in for that door here (S5b rewires the HTTP path onto it).
	if err := env.app.AdmitForTest(chID, actor.ActorID(instID), actor.KindAgent); err != nil {
		t.Fatalf("pre-admit daemon-placed agent: %v", err)
	}

	// 1) PULL the assignment.
	w = env.do(t, "GET", fmt.Sprintf("/compute/plan?key=%s&channel=%s", apiKey, chID), nil, nil)
	assertStatus(t, w, http.StatusOK)
	var planResp struct {
		Assignments []struct {
			InstanceID string          `json:"instance_id"`
			Class      string          `json:"class"`
			Config     json.RawMessage `json:"config"`
		} `json:"assignments"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &planResp); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if len(planResp.Assignments) != 1 {
		t.Fatalf("want 1 assignment, got %d", len(planResp.Assignments))
	}

	// 2) BUILD decls from the assignment (registry.Build — stub stands in for claude).
	var decls []platform.ActorDecl
	for _, asg := range planResp.Assignments {
		decl, err := registry.Build(asg.Class, registry.InstanceSpec{
			ID:     actor.ActorID(asg.InstanceID),
			Config: asg.Config,
		}, registry.Deps{ChannelID: channel.ID(chID)})
		if err != nil {
			t.Fatalf("build %s: %v", asg.Class, err)
		}
		decls = append(decls, decl)
	}

	// 3) ATTACH over a real /compute link.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), chID, apiKey)
	desired, builder := staticActorCompute(decls)
	go func() {
		runErr <- platform.RunCompute(ctx, platform.ComputeConfig{ServerWS: serverWS, Desired: desired, Builder: builder})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})

	// 4) the daemon-placed agent becomes a LIVE channel member.
	deadline := time.Now().Add(3 * time.Second)
	for {
		w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/actors", chID), nil, cookies)
		assertStatus(t, w, http.StatusOK)
		found := false
		if acts, ok := respJSON(t, w)["actors"].([]any); ok {
			for _, a := range acts {
				m := a.(map[string]any)
				if m["id"] == instID && m["kind"] == "agent" {
					found = true
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon-placed agent %s never became a live member", instID)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
