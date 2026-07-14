package app_test

import (
	"context"
	"encoding/json"
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
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type e2eLinkPlan struct {
	mu       sync.Mutex
	desired  []actorrt.DesiredMember
	builders map[actor.ActorID]platform.ActorFactory
	chID     channel.ID
	rows     []platform.PlanActor
	applies  int
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

	user, cookies := register(t, env, "e2e@example.com", "secret123", "Owner")
	userID := user["id"].(string)
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

	// Drive the canonical channel-control executor with the authenticated human
	// subject; there is no parallel HTTP channel-control route.
	sender, err := env.app.ResolvePrincipalForTest(chID, actor.KindHuman, userID)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"decl_id": agentID, "placement": "daemon", "desired_host": daemonID, "make_default": true,
	})
	introduced, err := env.app.OperateFaceForTest().Introduce(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(chID), Sender: sender, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	instID := introduced.(map[string]any)["instance_id"].(string)

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
			Poll: time.Hour, Resync: 100 * time.Millisecond,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})

	// Durable membership exists as soon as the intent is introduced, so it is
	// not a liveness oracle. Wait for the daemon's applied snapshot and actual
	// factory construction before taking the membership identity baseline.
	buildDeadline := time.Now().Add(3 * time.Second)
	for {
		rows, applies := plan.snapshot()
		if len(rows) == 1 && rows[0].InstanceID == actor.ActorID(instID) && buildCount(actor.ActorID(instID)) >= 1 {
			break
		}
		if time.Now().After(buildDeadline) {
			t.Fatalf("daemon never applied and built actor: rows=%#v applies=%d builds=%d", rows, applies, buildCount(actor.ActorID(instID)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	planBeforeRestart, applyBaseline := plan.snapshot()
	if len(planBeforeRestart) != 1 || planBeforeRestart[0].InstanceID != actor.ActorID(instID) {
		t.Fatalf("initial applied plan = %#v, want exactly %s", planBeforeRestart, instID)
	}
	actorsBeforeRestart, err := env.app.ActorsForTest(channel.ID(chID))
	if err != nil {
		t.Fatal(err)
	}
	var memberBeforeRestart storespec.Record
	for _, rec := range actorsBeforeRestart {
		if rec.ID == actor.ActorID(instID) {
			memberBeforeRestart = rec
			break
		}
	}
	if memberBeforeRestart.ID == "" {
		t.Fatalf("built daemon actor %s has no canonical membership row", instID)
	}
	if memberBeforeRestart.Host != daemonID {
		t.Fatalf("initial member host = %q, want %q", memberBeforeRestart.Host, daemonID)
	}

	// An unchanged periodic full-set resync must re-apply/redeclare the same
	// snapshot without replacing the already-live daemon body.
	waitDaemonComposition(t, func() bool {
		rows, applies := plan.snapshot()
		return applies >= applyBaseline+2 && reflect.DeepEqual(rows, planBeforeRestart)
	}, "unchanged plan was not reapplied by periodic resync")
	if got := buildCount(actor.ActorID(instID)); got != 1 {
		t.Fatalf("unchanged resync rebuilt actor: build count = %d, want 1", got)
	}

	// Restart is an epoch transition of the same desired/member identity. The
	// next resync must expose epoch+1 and replace the daemon body exactly once.
	restartPayload, _ := json.Marshal(map[string]any{"instance_id": instID})
	if _, err := env.app.OperateFaceForTest().Restart(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(chID), Sender: sender, Payload: restartPayload,
	}); err != nil {
		t.Fatal(err)
	}
	waitDaemonComposition(t, func() bool {
		rows, _ := plan.snapshot()
		return len(rows) == 1 && rows[0].Epoch == planBeforeRestart[0].Epoch+1 && buildCount(actor.ActorID(instID)) == 2
	}, "epoch restart did not replace the daemon body")
	actorsAfterRestart, err := env.app.ActorsForTest(channel.ID(chID))
	if err != nil {
		t.Fatal(err)
	}
	var memberAfterRestart storespec.Record
	for _, rec := range actorsAfterRestart {
		if rec.ID == actor.ActorID(instID) {
			memberAfterRestart = rec
			break
		}
	}
	if !reflect.DeepEqual(memberAfterRestart, memberBeforeRestart) {
		t.Fatalf("epoch restart changed membership identity:\n before: %#v\n  after: %#v", memberBeforeRestart, memberAfterRestart)
	}

	removePayload, _ := json.Marshal(map[string]any{"instance_id": instID})
	if _, err := env.app.OperateFaceForTest().Remove(context.Background(), home.OperateRequest{
		ChannelID: channel.ID(chID), Sender: sender, Payload: removePayload,
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		members, err := plan.Members(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(members) == 0 {
			if _, ok := plan.Lookup(actor.ActorID(instID)); ok {
				t.Fatal("shrunk plan retained the removed factory")
			}
			if rows, _ := plan.snapshot(); len(rows) != 0 {
				t.Fatalf("shrunk applied plan retained rows: %#v", rows)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated plan did not shrink after removing %s", instID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

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
