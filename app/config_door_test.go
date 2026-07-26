package app_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// config_door_test.go is the S8 DoD (spec §7 DoD 10): ctx.Config is the read-only
// per-instance snapshot that reaches an actor via registry.InstanceSpec.Config in
// the Proc model, never through actorcaps.Caps (that half is the
// TestConfigNotInCaps archtest), and 改配置 flows door → Controller replacement → a fresh
// incarnation over the new snapshot.

// configSink records the "model" each incarnation of an id was BUILT with — the
// live proof of which snapshot the constructor closure captured. Keyed by id;
// overwritten on every (re)build, so a post-reconcile-replace read sees the new value.
var configSink = struct {
	mu sync.Mutex
	m  map[actor.ActorID]string
}{m: map[actor.ActorID]string{}}

func recordConfig(id actor.ActorID, model string) {
	configSink.mu.Lock()
	configSink.m[id] = model
	configSink.mu.Unlock()
}

func readConfig(id actor.ActorID) (string, bool) {
	configSink.mu.Lock()
	defer configSink.mu.Unlock()
	v, ok := configSink.m[id]
	return v, ok
}

// waitConfig polls configSink until id's recorded model == want (a rebuild is
// async in the ring path; reconcile-replace builds the closure synchronously but the
// cell start is scheduled).
func waitConfig(t *testing.T, id actor.ActorID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if v, ok := readConfig(id); ok && v == want {
			return
		}
		if time.Now().After(deadline) {
			got, _ := readConfig(id)
			t.Fatalf("config snapshot for %s = %q, want %q (door → Spawn-replace did not embody the new snapshot)", id, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// configModel extracts the "model" knob from a config snapshot (the one field
// this test's classes read — a stand-in for any per-instance parameter).
func configModel(raw json.RawMessage) string {
	var c struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(raw, &c)
	return c.Model
}

func init() {
	// The constructor closure captures spec.Config into the Def
	// (Constructor(spec,deps) → Def → New() per incarnation), never into caps.
	proc := func(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
		model := configModel(spec.Config)
		id := spec.ID
		return platform.ActorDecl{
			ID:   id,
			Kind: actor.KindAgent,
			Factory: platform.ActorFactory{Proc: actorbase.Def{
				Doc: "s8 config snapshot proc",
				New: func() (actorbase.Proc, error) {
					recordConfig(id, model) // Def carries the snapshot into the incarnation
					return func(sys actorbase.Sys) error {
						for {
							if _, err := sys.Recv(); err != nil {
								return err
							}
						}
					}, nil
				},
			}},
		}, nil
	}
	registry.Register("s8cfg-proc", registry.ClassDecl{Kind: actor.KindAgent, New: proc})
}

func TestConfigDoor_ProcEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	w := env.do(t, "POST", "/api/actor-decls",
		map[string]any{"name": "cfgbot", "class": "s8cfg-proc", "config": map[string]any{"model": "v1"}},
		s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var declaration map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &declaration); err != nil {
		t.Fatalf("decode declaration: %v", err)
	}
	declID := declaration["id"].(string)
	daemonID := createAndBindDaemon(t, env, s.chID, "config-host", s.cookies)["id"].(string)

	introducedResp := env.do(t, "POST", "/api/channels/"+s.chID+"/actors", map[string]any{"decl_id": declID}, s.cookies)
	assertStatus(t, introducedResp, http.StatusCreated)
	introduced := respJSON(t, introducedResp)
	instanceID := actor.ActorID(introduced["actor_id"].(string))
	_ = daemonID
	waitDeclaredConfig(t, env, channel.ID(s.chID), declID, instanceID, "v1")

	updated := env.do(t, "PATCH", "/api/actor-decls/"+declID, map[string]any{"config": map[string]any{"model": "v2"}}, s.cookies)
	assertStatus(t, updated, http.StatusOK)
	waitDeclaredConfig(t, env, channel.ID(s.chID), declID, instanceID, "v2")
}

// waitDeclaredConfig is the app-level half of declaration convergence: the
// resolved declaration the runtime pull loop reads reaches `want`, and the
// declaration still has exactly the one instance (a config change is a new term
// on the SAME record, never a second instance). What the Controller then does
// with that definition — mint a new term on change, no-op on an equal value —
// is proven in platform/home, where the projection lives; the business membrane
// deliberately exposes no definition.
func waitDeclaredConfig(t *testing.T, env *testEnv, chID channel.ID, declID string, instanceID actor.ActorID, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		facts, err := env.app.ResolvedDeclarationForTest(context.Background(), chID, declID)
		if err == nil && configModel(facts.Config) == want {
			ids, instErr := env.app.DeclaredInstancesForTest(chID, declID)
			if instErr != nil {
				t.Fatalf("declared instances: %v", instErr)
			}
			if len(ids) != 1 || ids[0] != instanceID {
				t.Fatalf("declaration instances=%v, want exactly [%s]", ids, instanceID)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("actor %s did not converge to model %q", instanceID, want)
}

// TestConfigSnapshot_ProcForm proves the Proc form reads the snapshot too: a Proc
// class built with a config captures it into its Def (readable when the
// incarnation is constructed). Same constructor-closure承载, different actor form.
func TestConfigSnapshot_ProcForm(t *testing.T) {
	id := actor.ActorID("agent:proc-cfg")
	decl, err := registry.Build("s8cfg-proc", registry.InstanceSpec{
		ID:     id,
		Config: json.RawMessage(`{"model":"pv1"}`),
	}, registry.Deps{ChannelID: channel.ID("c-proc")})
	if err != nil {
		t.Fatalf("build proc class: %v", err)
	}
	if decl.Factory.Proc.New == nil {
		t.Fatalf("proc factory has no Def.New — config form is not the Proc shape")
	}
	// Constructing the incarnation (Def.New) captures the snapshot from the closure.
	if _, err := decl.Factory.Proc.New(); err != nil {
		t.Fatalf("proc Def.New: %v", err)
	}
	got, ok := readConfig(id)
	if !ok || got != "pv1" {
		t.Fatalf("proc-form config snapshot = %q (ok=%v), want %q", got, ok, "pv1")
	}
}
