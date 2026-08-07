package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// e2eLinkPlan is the daemon-side factory source, the exact shape cmd/daemon's
// production classFactories has: resolve at body-build time via registry.Build
// from the spec the daemon Host's desired carries, refuse an id-drifting
// constructor. It keeps no plan snapshot — the pulled desired is the one plan
// ledger — so plan propagation is observed through what it provably causes:
// bodies being built (and rebuilt on a fresh attempt).
type e2eLinkPlan struct {
	chID channel.ID
}

func (p *e2eLinkPlan) BuildClass(
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	decl, err := registry.Build(class, registry.InstanceSpec{ID: id, Config: config}, registry.Deps{ChannelID: p.chID})
	if err != nil || decl.ID != id {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

// TestDaemonComposition_E2E is the daemon-composition acceptance test: it runs
// the FULL daemon flow against a real server —
//
//	introduce test-agent (placement='daemon')
//	  → PULL the assignment over the authenticated link    (what daemon does 1st)
//	  → BUILD decls from it via registry.Build             (no blind-build)
//	  → ATTACH over a real /compute link (compute.Run)
//	  → the agent becomes a LIVE channel member (Home's canonical view)
//
// The app_test stub is built through the normal registry path (registry.Build("test-agent")
// → stub; no CLI), exactly the seam the unit tests use. This proves pull → build
// → attach → member wired end to end; the attach→member leg is the existing,
// unchanged link path.
func TestDaemonComposition_E2E(t *testing.T) {
	env := setupTestApp(t)
	baseBuilder := testAgentBuilder
	var builds sync.Map
	testAgentBuilder = func(chID channel.ID, id actor.ActorID) (actorbase.Proc, error) {
		counter, _ := builds.LoadOrStore(id, &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		return baseBuilder(chID, id)
	}
	buildCount := func(id actor.ActorID) int32 {
		counter, ok := builds.Load(id)
		if !ok {
			return 0
		}
		return counter.(*atomic.Int32).Load()
	}
	srv := httptest.NewServer(env.handler)
	defer srv.Close()

	_, cookies := register(t, env, "e2e@example.com", "secret123", "Owner")
	chBody, cookies := createChannel(t, env, cookies, "CH")
	chID := chBody["id"].(string)

	// Deterministic test agent.
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "Rev", "class": "test-agent"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	agentID := respJSON(t, w)["id"].(string)

	// create + bind a daemon (one call) → id + api_key.
	dResp := createAndBindDaemon(t, env, chID, "mybox", cookies)
	apiKey := dResp["api_key"].(string)

	introduced := env.do(t, "POST", "/api/channels/"+chID+"/actors", map[string]any{"decl_id": agentID}, cookies)
	assertStatus(t, introduced, http.StatusCreated)
	instID := respJSON(t, introduced)["actor_id"].(string)

	// ATTACH over a real /compute link. The first reconcile pulls the authenticated
	// plan on stream 0, atomically publishes desired+builder, then declares/builds.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	serverWS := fmt.Sprintf("ws://%s/compute", srv.Listener.Addr())
	plan := &e2eLinkPlan{chID: channel.ID(chID)}
	go func() {
		runErr <- compute.Run(ctx, daemonComputeConfig(t, serverWS, apiKey, plan, nil))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})

	// Durable identity truth exists as soon as the intent is introduced, so it is
	// not a liveness oracle. Wait for the actual factory construction: the
	// factory source is stateless, so a build is proof the pulled desired
	// carried this actor's row.
	waitDaemonComposition(t, func() bool {
		return buildCount(actor.ActorID(instID)) >= 1
	}, "daemon never built the introduced actor")
	factsBeforeRestart, member, err := env.app.ActorFactsForTest(channel.ID(chID), actor.ActorID(instID))
	if err != nil {
		t.Fatal(err)
	}
	if !member {
		t.Fatalf("built daemon actor %s has no canonical membership", instID)
	}
	// Placement is actor-control authority, not legacy registry metadata. The
	// accepted daemon-scoped Plan above is the observable projection of that
	// authority; registry membership intentionally carries no host assignment.

	// An unchanged periodic full-set resync must not replace the already-live
	// daemon body. Absence of a rebuild can only be observed over a window:
	// give the 100ms poll several rounds, then require the build count stayed
	// at one.
	time.Sleep(400 * time.Millisecond)
	if got := buildCount(actor.ActorID(instID)); got != 1 {
		t.Fatalf("unchanged resync rebuilt actor: build count = %d, want 1", got)
	}

	// A channel overlay update replaces the carrier without mutating membership
	// identity. A second build IS the proof a fresh AttemptKey travelled: the
	// Host never rebuilds an unchanged desired (the window above pinned that),
	// so only a new attempt can raise the count.
	restart := env.do(t, "PUT", "/api/channels/"+chID+"/decls/"+agentID+"/config", map[string]any{"config": map[string]any{}}, cookies)
	assertStatus(t, restart, http.StatusOK)
	waitDaemonComposition(t, func() bool {
		return buildCount(actor.ActorID(instID)) == 2
	}, "version restart did not replace the daemon body")
	factsAfterRestart, member, err := env.app.ActorFactsForTest(channel.ID(chID), actor.ActorID(instID))
	if err != nil {
		t.Fatal(err)
	}
	if !member || factsAfterRestart != factsBeforeRestart {
		t.Fatalf("version restart changed membership identity:\n before: %#v\n  after: %#v (member=%v)",
			factsBeforeRestart, factsAfterRestart, member)
	}

	removed := env.do(t, "DELETE", "/api/actor-decls/"+agentID, nil, cookies)
	assertStatus(t, removed, http.StatusOK)
	// Soft deletion stops future supply only; the existing instance keeps its
	// membership and its running body until the explicit remove word ends it.
	time.Sleep(300 * time.Millisecond)
	if _, member, err := env.app.ActorFactsForTest(channel.ID(chID), actor.ActorID(instID)); err != nil || !member {
		t.Fatalf("soft delete ended the existing instance: member=%v err=%v", member, err)
	}
	if got := buildCount(actor.ActorID(instID)); got != 2 {
		t.Fatalf("soft delete disturbed the running body: build count = %d, want 2", got)
	}
	removed = env.do(t, "DELETE", "/api/channels/"+chID+"/actors/"+instID, nil, cookies)
	assertStatus(t, removed, http.StatusOK)
	waitDaemonComposition(t, func() bool {
		_, member, err := env.app.ActorFactsForTest(channel.ID(chID), actor.ActorID(instID))
		return err == nil && !member
	}, "membership survived the explicit remove")
}

// Binding truth and live attachment are deliberately independent axes. The
// focused detach convergence cases live with the sysop forward e2e tests.
func waitDaemonComposition(t *testing.T, condition func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
