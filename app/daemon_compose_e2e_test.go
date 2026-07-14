package app_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type e2eLinkPlan struct {
	mu       sync.Mutex
	desired  []actorrt.DesiredMember
	builders map[actor.ActorID]platform.ActorFactory
	chID     channel.ID
}

func (p *e2eLinkPlan) Members(context.Context) ([]actorrt.DesiredMember, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]actorrt.DesiredMember(nil), p.desired...), nil
}

func (p *e2eLinkPlan) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.builders[id]
	return f, ok
}

func (p *e2eLinkPlan) ApplyPlan(rows []platform.PlanActor) error {
	desired := make([]actorrt.DesiredMember, 0, len(rows))
	builders := make(map[actor.ActorID]platform.ActorFactory, len(rows))
	for _, row := range rows {
		decl, err := registry.Build(row.Class, registry.InstanceSpec{ID: row.InstanceID, Config: row.Config}, registry.Deps{ChannelID: p.chID})
		if err != nil {
			return err
		}
		desired = append(desired, actorrt.DesiredMember{ID: row.InstanceID, Kind: row.Kind, Lifecycle: actorrt.LifecycleAlwaysOn, Epoch: row.Epoch})
		builders[row.InstanceID] = decl.Factory
	}
	p.mu.Lock()
	p.desired, p.builders = desired, builders
	p.mu.Unlock()
	return nil
}

// TestDaemonComposition_E2E is the daemon-composition acceptance test: it runs
// the FULL daemon flow against a real server —
//
//	introduce claude (placement='daemon')
//	  → PULL the assignment from GET /compute/plan         (what daemon does 1st)
//	  → BUILD decls from it via registry.Build             (no blind-build)
//	  → ATTACH over a real /compute link (compute.Run)
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
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "Rev", "class": "claude"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)

	// create + bind a daemon (one call) → id + api_key.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons", chID),
		map[string]any{"name": "mybox"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	dResp := respJSON(t, w)
	daemonID := dResp["id"].(string)
	apiKey := dResp["api_key"].(string)

	// introduce the claude as a DAEMON-placed default assigned to THIS daemon.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/actors", chID),
		map[string]any{"decl_id": agentID, "placement": "daemon", "desired_host": daemonID, "make_default": true}, cookies)
	assertStatus(t, w, http.StatusCreated)
	instID := respJSON(t, w)["instance_id"].(string)
	// 膜律 (v1.8 问①): daemon attach no longer mints membership — the daemon-placed
	// actor must be admitted by the introduce door first. The old HTTP introduce
	// path writes intent only; the operate-face executor (CORE1③) is what Admits in
	// production. Stand in for that door here (S5b rewires the HTTP path onto it).

	// ATTACH over a real /compute link. The first reconcile pulls the authenticated
	// plan on stream 0, atomically publishes desired+builder, then declares/builds.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), chID, apiKey)
	plan := &e2eLinkPlan{chID: channel.ID(chID), builders: map[actor.ActorID]platform.ActorFactory{}}
	go func() {
		runErr <- compute.Run(ctx, compute.Config{ServerWS: serverWS, Desired: plan, Builder: plan, PlanSink: plan, Poll: 20 * time.Millisecond})
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
