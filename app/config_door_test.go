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
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// config_door_test.go is the S8 DoD (spec §7 DoD 10): ctx.Config is the read-only
// per-instance snapshot that reaches an actor via registry.InstanceSpec.Config in
// BOTH forms (Legacy / Proc), never through actorcaps.Caps (that half is the
// TestConfigNotInCaps archtest), and 改配置 flows door → Spawn-replace → a fresh
// incarnation over the new snapshot.

// configSink records the "model" each incarnation of an id was BUILT with — the
// live proof of which snapshot the constructor closure captured. Keyed by id;
// overwritten on every (re)build, so a post-Spawn-replace read sees the new value.
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
// async in the ring path; Spawn-replace builds the closure synchronously but the
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
	// Legacy form: the constructor parses spec.Config and closes over the result in
	// the func(pen) factory closure — config rides the constructor, not caps.
	legacy := func(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
		model := configModel(spec.Config)
		id := spec.ID
		return platform.ActorDecl{
			ID:   id,
			Kind: actor.KindAgent,
			Factory: platform.ActorFactory{Legacy: func(pen harness.Pen) actorrt.Actor {
				recordConfig(id, model) // runs when THIS incarnation is built
				return &cfgLegacyActor{pen: pen}
			}},
		}, nil
	}
	registry.Register("s8cfg-legacy", registry.ClassDecl{Kind: actor.KindAgent, New: legacy})

	// Proc form: the same constructor closure captures spec.Config into the Def
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
					return func(sys actorbase.Sys) error { return nil }, nil
				},
			}},
		}, nil
	}
	registry.Register("s8cfg-proc", registry.ClassDecl{Kind: actor.KindAgent, New: proc})
}

type cfgLegacyActor struct{ pen harness.Pen }

func (a *cfgLegacyActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestConfigDoor_LegacyEndToEnd is the end-to-end DoD: introduce a server-placed
// agent (snapshot v1 from its global config), then change config through the door
// (introduce upsert carrying config) and observe the new snapshot v2 embodied via
// Spawn-replace.
func TestConfigDoor_LegacyEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Global agent declaration carries snapshot v1 (looper = the s8cfg-legacy class).
	w := env.do(t, "POST", "/api/actor-decls",
		map[string]any{"name": "cfgbot", "class": "s8cfg-legacy", "config": map[string]any{"model": "v1"}},
		s.cookies)
	assertStatus(t, w, http.StatusCreated)
	var ag map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &ag); err != nil {
		t.Fatalf("decode agent: %v", err)
	}
	agentID := ag["id"].(string)
	instanceID := actor.ActorID("agent:" + agentID)

	face := env.app.OperateFaceForTest()
	sender := actor.ActorID("user:" + s.userID)

	// Introduce server-placed (no per-channel config → global v1 is the snapshot).
	p1, _ := json.Marshal(map[string]any{"decl_id": agentID, "placement": "server"})
	if _, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID), Sender: sender, Payload: p1,
	}); err != nil {
		t.Fatalf("introduce v1: %v", err)
	}
	if !env.app.WaitLiveForTest(s.chID, instanceID, 2*time.Second) {
		t.Fatalf("instance %s never embodied after introduce", instanceID)
	}
	waitConfig(t, instanceID, "v1", 2*time.Second)

	// 改配置门: re-introduce carrying config v2 → UPDATE composition row's config
	// field → Spawn-replace → new snapshot embodied.
	p2, _ := json.Marshal(map[string]any{"decl_id": agentID, "config": map[string]any{"model": "v2"}})
	res, err := face.Introduce(context.Background(), platform.OperateRequest{
		ChannelID: channel.ID(s.chID), Sender: sender, Payload: p2,
	})
	if err != nil {
		t.Fatalf("introduce v2 (config change): %v", err)
	}
	m, _ := res.(map[string]any)
	if created, _ := m["created"].(bool); created {
		t.Fatalf("config-change introduce reported created=true (should update existing row)")
	}
	if updated, _ := m["config_updated"].(bool); !updated {
		t.Fatalf("config-change introduce did not report config_updated")
	}
	waitConfig(t, instanceID, "v2", 2*time.Second)
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
