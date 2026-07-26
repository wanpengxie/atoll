package app_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type e2eLinkPlan struct {
	mu       sync.Mutex
	builders map[actor.ActorID]platform.ActorFactory
	chID     channel.ID
	rows     []platform.PlanActor
	applies  int

	lookupBlockID actor.ActorID
	lookupEntered chan struct{}
	lookupRelease chan struct{}
	lookupOnce    sync.Once
}

func (p *e2eLinkPlan) LookupExact(
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	spec actorhost.ExecutionSpec,
) (platform.ActorFactory, bool) {
	p.mu.Lock()
	f, ok := p.builders[id]
	if ok {
		ok = false
		for _, row := range p.rows {
			rowSpec := actorhost.ExecutionSpec{
				Kind: row.Kind, Class: row.Class, Config: row.Config,
			}
			if row.ActorID == id && row.AttemptKey == attempt && rowSpec.Equal(spec) {
				ok = true
				break
			}
		}
	}
	p.mu.Unlock()
	if id == p.lookupBlockID && p.lookupEntered != nil && p.lookupRelease != nil {
		p.lookupOnce.Do(func() { close(p.lookupEntered) })
		<-p.lookupRelease
	}
	return f, ok
}

func (p *e2eLinkPlan) ApplyPlan(rows []platform.PlanActor) error {
	builders := make(map[actor.ActorID]platform.ActorFactory, len(rows))
	for _, row := range rows {
		decl, err := registry.Build(row.Class, registry.InstanceSpec{ID: row.ActorID, Config: row.Config}, registry.Deps{ChannelID: p.chID})
		if err != nil {
			return err
		}
		if decl.ID != row.ActorID {
			return fmt.Errorf("plan actor %s built mismatched id %s", row.ActorID, decl.ID)
		}
		builders[row.ActorID] = decl.Factory
	}
	p.mu.Lock()
	p.builders = builders
	p.rows = append([]platform.PlanActor(nil), rows...)
	p.applies++
	p.mu.Unlock()
	return nil
}

func (p *e2eLinkPlan) snapshot() ([]platform.PlanActor, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]platform.PlanActor(nil), p.rows...), p.applies
}

// TestDaemonComposition_E2E is the daemon-composition acceptance test: it runs
// the FULL daemon flow against a real server —
//
//	introduce claude (placement='daemon')
//	  → PULL the assignment over the authenticated link    (what daemon does 1st)
//	  → BUILD decls from it via registry.Build             (no blind-build)
//	  → ATTACH over a real /compute link (compute.Run)
//	  → the agent becomes a LIVE channel member (Home's canonical view)
//
// The app_test stub stands in for the claude engine (registry.Build("claude")
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

	// claude agent.
	w := env.do(t, "POST", "/api/actor-decls", map[string]any{"name": "Rev", "class": "claude"}, cookies)
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
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), chID, apiKey)
	plan := &e2eLinkPlan{chID: channel.ID(chID), builders: map[actor.ActorID]platform.ActorFactory{}}
	if rows, applies := plan.snapshot(); len(rows) != 0 || applies != 0 {
		t.Fatalf("initial plan = (%v, %d applies), want empty unapplied baseline", rows, applies)
	}
	go func() {
		runErr <- compute.Run(ctx, compute.Config{
			ServerWS: serverWS, PlanSource: plan,
			Poll: 100 * time.Millisecond,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})

	// Durable identity truth exists as soon as the intent is introduced, so it is
	// not a liveness oracle. Wait for the daemon's applied snapshot and actual
	// factory construction before taking the membership identity baseline.
	buildDeadline := time.Now().Add(3 * time.Second)
	for {
		rows, applies := plan.snapshot()
		if len(rows) == 1 && rows[0].ActorID == actor.ActorID(instID) && buildCount(actor.ActorID(instID)) >= 1 {
			break
		}
		if time.Now().After(buildDeadline) {
			t.Fatalf("daemon never applied and built actor: rows=%#v applies=%d builds=%d", rows, applies, buildCount(actor.ActorID(instID)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	planBeforeRestart, applyBaseline := plan.snapshot()
	if len(planBeforeRestart) != 1 || planBeforeRestart[0].ActorID != actor.ActorID(instID) {
		t.Fatalf("initial applied plan = %#v, want exactly %s", planBeforeRestart, instID)
	}
	actorsBeforeRestart, err := env.app.ActorsForTest(channel.ID(chID))
	if err != nil {
		t.Fatal(err)
	}
	var memberBeforeRestart storespec.ActorRecord
	for _, rec := range actorsBeforeRestart {
		if rec.ID == actor.ActorID(instID) {
			memberBeforeRestart = rec
			break
		}
	}
	if memberBeforeRestart.ID == "" {
		t.Fatalf("built daemon actor %s has no canonical membership row", instID)
	}
	// Placement is actor-control authority, not legacy registry metadata. The
	// accepted daemon-scoped Plan above is the observable projection of that
	// authority; registry membership intentionally carries no host assignment.

	// An unchanged periodic full-set resync must re-apply/redeclare the same
	// snapshot without replacing the already-live daemon body.
	waitDaemonComposition(t, func() bool {
		rows, applies := plan.snapshot()
		return applies >= applyBaseline+2 && reflect.DeepEqual(rows, planBeforeRestart)
	}, "unchanged plan was not reapplied by periodic resync")
	if got := buildCount(actor.ActorID(instID)); got != 1 {
		t.Fatalf("unchanged resync rebuilt actor: build count = %d, want 1", got)
	}

	// A channel overlay update replaces the carrier without mutating membership
	// identity. The next
	// plan keeps the actor identity and carries a fresh AttemptKey.
	restart := env.do(t, "PUT", "/api/channels/"+chID+"/decls/"+agentID+"/config", map[string]any{"config": map[string]any{}}, cookies)
	assertStatus(t, restart, http.StatusOK)
	waitDaemonComposition(t, func() bool {
		rows, _ := plan.snapshot()
		return len(rows) == 1 && rows[0].ActorID == planBeforeRestart[0].ActorID &&
			rows[0].AttemptKey != planBeforeRestart[0].AttemptKey && buildCount(actor.ActorID(instID)) == 2
	}, "version restart did not replace the daemon body")
	actorsAfterRestart, err := env.app.ActorsForTest(channel.ID(chID))
	if err != nil {
		t.Fatal(err)
	}
	var memberAfterRestart storespec.ActorRecord
	for _, rec := range actorsAfterRestart {
		if rec.ID == actor.ActorID(instID) {
			memberAfterRestart = rec
			break
		}
	}
	if memberAfterRestart.ID != memberBeforeRestart.ID ||
		memberAfterRestart.Kind != memberBeforeRestart.Kind ||
		memberAfterRestart.Principal != memberBeforeRestart.Principal ||
		memberAfterRestart.CreatedAt != memberBeforeRestart.CreatedAt {
		t.Fatalf("version restart changed membership identity:\n before: %#v\n  after: %#v", memberBeforeRestart, memberAfterRestart)
	}

	removed := env.do(t, "DELETE", "/api/actor-decls/"+agentID, nil, cookies)
	assertStatus(t, removed, http.StatusOK)
	// Soft deletion stops future supply only; the existing instance keeps its
	// last channel snapshot until the explicit remove word ends membership.
	if rows, _ := plan.snapshot(); len(rows) != 1 || rows[0].ActorID != actor.ActorID(instID) {
		t.Fatalf("soft delete changed existing plan: %#v", rows)
	}
	removed = env.do(t, "DELETE", "/api/channels/"+chID+"/actors/"+instID, nil, cookies)
	assertStatus(t, removed, http.StatusOK)
	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, _ := plan.snapshot()
		if len(rows) == 0 {
			plan.mu.Lock()
			_, ok := plan.builders[actor.ActorID(instID)]
			plan.mu.Unlock()
			if ok {
				t.Fatal("shrunk plan retained the removed factory")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated plan did not shrink after removing %s", instID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Binding truth and live attachment are deliberately independent axes. The
// focused detach convergence cases live with the admission tests.
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
