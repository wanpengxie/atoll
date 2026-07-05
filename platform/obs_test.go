package platform

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// obsPublisherActor is a minimal in-proc cell that publishes a device-presence
// snapshot on its first Receive — driving the local-cell arm of the obs axis
// (G8: PublishObs from an in-process cell used to fall on the floor, only the
// daemon-attach arm folded into the home device-presence fold).
type obsPublisherActor struct {
	self actorrt.ActorContext
}

func (a *obsPublisherActor) Start(_ context.Context, self actorrt.ActorContext) error {
	a.self = self
	return nil
}

func (a *obsPublisherActor) Receive(context.Context, *message.Envelope) error {
	a.self.PublishObs(actorrt.ObsKind(introspect.ObsDevicePresence), introspect.MarshalDevicePresence(true))
	return nil
}

// TestHome_PublishObs_FoldsIntoDevicePresence (DoD §7.6, G8 coral regression):
// an in-process cell's PublishObs reaches the home's own device-presence fold
// (no daemon attach involved) — View.DevicePresence lights up.
func TestHome_PublishObs_FoldsIntoDevicePresence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := Open(HomeConfig{ChannelID: channel.ID("test-obs-local"), DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	id := actor.ActorID("obs-publisher")
	pub := &obsPublisherActor{}
	if err := h.Spawn(ctx, id, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor { return pub })); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if _, known := h.View().DevicePresence(id); known {
		t.Fatalf("DevicePresence known before any publish")
	}

	if _, err := h.channel.Deliverer().Deliver([]actor.ActorID{id}, &message.Envelope{
		ID: message.ID("trigger"), Sender: message.Sender{ID: actor.SystemActorID}, Audience: []actor.ActorID{id},
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap, known := h.View().DevicePresence(id); known {
			p, ok := introspect.ParseDevicePresence(snap)
			if !ok || !p.Online {
				t.Fatalf("ParseDevicePresence = %+v, ok=%v, want online", p, ok)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("View.DevicePresence never lit up after PublishObs")
}

// TestHome_WatchObs_DedupesAcrossRepeatBuildCaps (DoD §7.6): buildCaps is the
// convergence point for every local birth path; a repeated build for the same
// id (e.g. a SpawnIfAbsent/Fork CAS loser rebuilding caps before losing the
// race) must not leave a duplicate runtime registration — the obsReg map is
// the dedup guarantee.
func TestHome_WatchObs_DedupesAcrossRepeatBuildCaps(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := Open(HomeConfig{ChannelID: channel.ID("test-obs-dedup"), DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	id := actor.ActorID("dedup-actor")
	h.watchObs(id)
	h.watchObs(id)
	h.watchObs(id)

	h.obsMu.Lock()
	n := len(h.obsReg)
	reg := h.obsReg[id]
	h.obsMu.Unlock()
	if !reg {
		t.Fatalf("obsReg[%q] = false, want true after watchObs", id)
	}
	if n != 1 {
		t.Fatalf("obsReg has %d entries after repeated watchObs(same id), want 1", n)
	}
}

// TestHome_BuildCaps_RegistersObs_AcrossBirthPaths (DoD §7.6): buildCaps is the
// single caps-assembly seam shared by Home.Spawn, the reconcile ring's eager
// 补臂, homeReviver, and spawnHandle.Fork (all four hold this method value) —
// exercising it directly for distinct ids covers the convergence point every
// birth path funnels through, without re-driving each path end to end.
func TestHome_BuildCaps_RegistersObs_AcrossBirthPaths(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := Open(HomeConfig{ChannelID: channel.ID("test-obs-buildcaps"), DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	rt := h.channel.Cells()
	ids := []actor.ActorID{"birth-spawn", "birth-reconcile", "birth-reviver", "birth-fork"}
	for _, id := range ids {
		rt.Spawn(id, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
			h.buildCaps(id, actor.KindAgent, inc)
			return statTestActor{}
		})
	}

	h.obsMu.Lock()
	defer h.obsMu.Unlock()
	for _, id := range ids {
		if !h.obsReg[id] {
			t.Errorf("obsReg[%q] = false after buildCaps, want true", id)
		}
	}
}

// countingObsWatcher counts OnObs invocations per actor — the fanout-doubling
// probe: a birth path that accidentally left a duplicate runtime-level
// WatchObs registration (buildCaps's dedup is obsReg-map-based, per id, not
// per-registration — see feedback_no_kernel_babysitting-adjacent H5 reasoning
// in accept.go/scheduler.go) would show up here as count()==2 after exactly
// one PublishObs, not count()==1.
type countingObsWatcher struct {
	mu    sync.Mutex
	calls map[actor.ActorID]int
}

func newCountingObsWatcher() *countingObsWatcher {
	return &countingObsWatcher{calls: map[actor.ActorID]int{}}
}

func (w *countingObsWatcher) OnObs(_ context.Context, id actor.ActorID, _ actorrt.ObsKind, _ actorrt.ObsValue) {
	w.mu.Lock()
	w.calls[id]++
	w.mu.Unlock()
}

func (w *countingObsWatcher) count(id actor.ActorID) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[id]
}

// deliverTrigger sends one no-op request envelope at id — the standard "wake
// this cell's Receive once" idiom used across obs tests (obsPublisherActor
// publishes on ITS Receive, so delivering one message is what drives one
// PublishObs call).
func deliverTrigger(t *testing.T, h *Home, id actor.ActorID) {
	t.Helper()
	if _, err := h.channel.Deliverer().Deliver([]actor.ActorID{id}, &message.Envelope{
		ID: message.ID("trigger-" + string(id)), Sender: message.Sender{ID: actor.SystemActorID}, Audience: []actor.ActorID{id},
	}); err != nil {
		t.Fatalf("deliver to %q: %v", id, err)
	}
}

// awaitCount polls until watcher.count(id) is non-zero (or the deadline
// expires), then asserts it is EXACTLY 1 — one publish, one fanout, no more.
func awaitCount(t *testing.T, w *countingObsWatcher, id actor.ActorID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n := w.count(id); n > 0 {
			if n != 1 {
				t.Fatalf("countingObsWatcher saw %d OnObs calls for %q after one publish, want exactly 1", n, id)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("countingObsWatcher never observed a PublishObs from %q", id)
}

// TestObsFanout_HomeSpawn_OncePerPublish (DoD §7.6, S5 review fix P2-1): the
// Home.Spawn birth path — real PublishObs, real counting watcher — fans out
// exactly once per publish (not zero, not duplicated).
func TestObsFanout_HomeSpawn_OncePerPublish(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := Open(HomeConfig{ChannelID: channel.ID("test-obs-fanout-spawn"), DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	const id = actor.ActorID("obs-fanout-spawn")
	if err := h.Spawn(ctx, id, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return &obsPublisherActor{}
	})); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	w := newCountingObsWatcher()
	h.channel.Cells().WatchObs(id, w)
	deliverTrigger(t, h, id)
	awaitCount(t, w, id)
}

// TestObsFanout_ReconcileActivationRevive_OncePerPublish (DoD §7.6, S5 review
// fix P2-1): the reconcile ring's eager 补臂 (Home.reconcileActivation reviving
// an absent always-on desired member) is a real birth path through buildCaps —
// its revived actor's PublishObs fans out exactly once.
func TestObsFanout_ReconcileActivationRevive_OncePerPublish(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:obs-fanout-reconcile")
	builder.byID[id] = CapsFactory(func(actorcaps.Caps) actorrt.Actor { return &obsPublisherActor{} })

	h := openActivationHome(t, desired, builder)
	admit(t, h, id, actor.KindAgent)

	if live(h, id) {
		t.Fatal("member live before it was ever desired")
	}
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("desired always-on member was NOT revived by reconcile")
	}

	w := newCountingObsWatcher()
	h.channel.Cells().WatchObs(id, w)
	deliverTrigger(t, h, id)
	awaitCount(t, w, id)
}

// TestObsFanout_HomeReviver_OncePerPublish (DoD §7.6, S5 review fix P2-1):
// homeReviver.EnsureLive — the identity-timer activation seam, driven directly
// (the same call the schedule engine's wake-first Reviver step makes) — is a
// real birth path through buildCaps; the revived actor's PublishObs fans out
// exactly once.
func TestObsFanout_HomeReviver_OncePerPublish(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{} // empty: no eager reconcile competing for this id
	builder := newTestBuilder()
	const id = actor.ActorID("agent:obs-fanout-reviver")
	builder.byID[id] = CapsFactory(func(actorcaps.Caps) actorrt.Actor { return &obsPublisherActor{} })

	h := openActivationHome(t, desired, builder)

	// Seed durable membership WITHOUT a live cell (factory=nil — Home.Spawn's
	// two-phase contract: membership row lands, no rt.Spawn runs).
	if err := h.Spawn(ctx, id, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if live(h, id) {
		t.Fatal("member live before EnsureLive ran — precondition broken")
	}

	if err := (homeReviver{h: h}).EnsureLive(ctx, id); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	if !live(h, id) {
		t.Fatal("member not live after EnsureLive")
	}

	w := newCountingObsWatcher()
	h.channel.Cells().WatchObs(id, w)
	deliverTrigger(t, h, id)
	awaitCount(t, w, id)
}

// TestObsFanout_Fork_OncePerPublish (DoD §7.6, S5 review fix P2-1):
// spawnHandle.Fork — the ephemeral child-birth path — is a real birth path
// through buildCaps (via h.assemble); the forked child's PublishObs fans out
// exactly once.
func TestObsFanout_Fork_OncePerPublish(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const parent = actor.ActorID("agent:obs-fanout-fork-parent")
	builder.byID[parent] = builder.recordFactory(parent)
	builder.byClass["obs-worker"] = CapsFactory(func(actorcaps.Caps) actorrt.Actor { return &obsPublisherActor{} })

	h := openActivationHome(t, desired, builder)
	admit(t, h, parent, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, parent) {
		t.Fatal("parent not revived")
	}
	caps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	childID, err := caps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "obs-worker", NameHint: "w1"})
	if err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	if !live(h, childID) {
		t.Fatal("fork child not live after Fork")
	}

	w := newCountingObsWatcher()
	h.channel.Cells().WatchObs(childID, w)
	deliverTrigger(t, h, childID)
	awaitCount(t, w, childID)
}
