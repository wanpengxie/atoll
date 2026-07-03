package platform

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const activationTestChannelID = channel.ID("test-activation")

// recordActor is a no-op cell that captures the caps bundle its factory was
// handed — the reconcile/revive tests read it back to prove the welded caps.
type recordActor struct{ caps actorcaps.Caps }

func (recordActor) Receive(context.Context, *message.Envelope) error { return nil }

// testDesired is an in-memory, swappable DesiredSource (the intent half the app
// assembly root injects for real, over channel_actors).
type testDesired struct {
	mu      sync.Mutex
	members []actorrt.DesiredMember
}

func (d *testDesired) set(ms ...actorrt.DesiredMember) {
	d.mu.Lock()
	d.members = ms
	d.mu.Unlock()
}

func (d *testDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]actorrt.DesiredMember, len(d.members))
	copy(out, d.members)
	return out, nil
}

// testBuilder is the platform CapsFactoryBuilder double: id-keyed for activation,
// class-keyed for fork. Each factory records the caps it received.
type testBuilder struct {
	mu      sync.Mutex
	byID    map[actor.ActorID]func(actorcaps.Caps) actorrt.Actor
	byClass map[string]func(actorcaps.Caps) actorrt.Actor
	seen    map[actor.ActorID]actorcaps.Caps
}

func newTestBuilder() *testBuilder {
	return &testBuilder{
		byID:    map[actor.ActorID]func(actorcaps.Caps) actorrt.Actor{},
		byClass: map[string]func(actorcaps.Caps) actorrt.Actor{},
		seen:    map[actor.ActorID]actorcaps.Caps{},
	}
}

// recordFactory returns a factory that records the caps under id and returns a
// recordActor. Registered on byID (activation) and/or byClass (fork).
func (b *testBuilder) recordFactory(id actor.ActorID) func(actorcaps.Caps) actorrt.Actor {
	return func(c actorcaps.Caps) actorrt.Actor {
		b.mu.Lock()
		b.seen[id] = c
		b.mu.Unlock()
		return recordActor{caps: c}
	}
}

func (b *testBuilder) capsFor(id actor.ActorID) (actorcaps.Caps, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.seen[id]
	return c, ok
}

func (b *testBuilder) Lookup(id actor.ActorID) (func(actorcaps.Caps) actorrt.Actor, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.byID[id]
	return f, ok
}

func (b *testBuilder) LookupByClass(class string) (func(actorcaps.Caps) actorrt.Actor, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.byClass[class]
	return f, ok
}

func openActivationHome(t *testing.T, desired actorrt.DesiredSource, builder CapsFactoryBuilder) *Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "activation.sqlite")
	// A long ReconcileInterval keeps the ticker out of the way — these tests drive
	// reconcileActivation synchronously for determinism.
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func live(h *Home, id actor.ActorID) bool {
	_, ok := h.channel.Cells().Stat(id)
	return ok
}

// Test 1 — desired absent member → Builder revives it (eager activation) and the
// revived incarnation carries a usable State cap (create+read round-trips).
func TestReconcileActivation_RevivesAbsentDesiredMemberWithState(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:always")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)

	if live(h, id) {
		t.Fatal("member live before it was ever desired")
	}
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if !live(h, id) {
		t.Fatal("desired always-on member was NOT revived by reconcile")
	}
	caps, ok := builder.capsFor(id)
	if !ok {
		t.Fatal("builder factory never ran — member not built through the caps seam")
	}
	if caps.Pen == nil || caps.Access == nil || caps.State == nil || caps.Schedule == nil || caps.Spawn == nil {
		t.Fatalf("revived member got an incomplete caps bundle: %+v", caps)
	}
	// state 读回: the revived incarnation's own actor-scoped state door round-trips
	// (proves the State cap is wired end to end, not merely non-nil).
	const key = resource.ResourceID("cursor")
	if _, err := caps.State.Invoke(ctx, access.OpCreate, key, []byte("v1"), nil); err != nil {
		t.Fatalf("revived member State create: %v", err)
	}
	out, err := caps.State.Invoke(ctx, access.OpRead, key, nil, nil)
	if err != nil {
		t.Fatalf("revived member State read: %v", err)
	}
	if string(out.Value) != "v1" {
		t.Fatalf("State read back %q, want v1", out.Value)
	}
}

// Test 2 — a member this ring previously minted, no longer desired, is deactivated
// via DespawnID (the prevEagerDesired − currentDesired 削 arm).
func TestReconcileActivation_DespawnsNoLongerDesired(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:transient")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("member not revived on first tick")
	}

	desired.set() // no longer desired
	h.reconcileActivation(ctx)
	if live(h, id) {
		t.Fatal("no-longer-desired member was NOT deactivated (DespawnID 削 arm)")
	}
}

// Test 3 — 反误杀: live actors OUTSIDE the eager-managed set (the intrinsic system
// cell, an admitted human, a fork child) survive a reconcile tick that does not
// desire them. Only prevEagerDesired − currentDesired is ever DespawnID'd, and
// none of these ever enter prevEagerDesired.
func TestReconcileActivation_DoesNotKillUnmanagedLiveActors(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const parent = actor.ActorID("agent:always")
	builder.byID[parent] = builder.recordFactory(parent)
	builder.byClass["worker"] = builder.recordFactory("agent:always/w1")

	h := openActivationHome(t, desired, builder)

	// A human, admitted as a real cell (durable member) — never in desired.
	const human = actor.ActorID("user:alice")
	if err := h.Spawn(ctx, human, actor.KindHuman, func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}); err != nil {
		t.Fatalf("Spawn human: %v", err)
	}

	// The eager-managed parent, revived by reconcile.
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, parent) {
		t.Fatal("parent not revived")
	}
	// A fork child owned by the parent's incarnation (ephemeral, never a durable
	// member, never desired).
	caps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	childID, err := caps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: "w1"})
	if err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	if !live(h, childID) {
		t.Fatal("fork child not live after Fork")
	}

	// Reconcile again with the SAME desired set (only parent). system/human/fork
	// child are all live and NONE are in prevEagerDesired, so the 削 arm must touch
	// none of them.
	h.reconcileActivation(ctx)

	if !live(h, actor.SystemActorID) {
		t.Fatal("intrinsic system cell was killed by reconcile (反误杀 broken)")
	}
	if !live(h, human) {
		t.Fatal("admitted human was killed by reconcile (反误杀 broken)")
	}
	if !live(h, childID) {
		t.Fatal("fork child was killed by reconcile (反误杀 broken)")
	}
	if !live(h, parent) {
		t.Fatal("eager-managed parent was killed while still desired")
	}
}

// Test 4 — boot-order: an identity timer whose author has NO live incarnation
// fires by reviving the author first (the Reviver seam), THEN appending — with no
// reconcile tick involved at all (the wake path is self-sufficient from the first
// instant, before any eager reconcile).
func TestReconcileActivation_IdentityTimerFireRevivesThenAppends(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{} // deliberately empty: no eager reconcile for this author
	builder := newTestBuilder()
	const author = actor.ActorID("agent:sleeper")
	builder.byID[author] = builder.recordFactory(author)

	h := openActivationHome(t, desired, builder)

	// Seed durable membership (no cell) so the FireSink can resolve the author's
	// kind and the harness accepts the fired envelope — but leave it UN-embodied.
	if err := h.Spawn(ctx, author, actor.KindAgent, nil); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if live(h, author) {
		t.Fatal("author already live before the timer fired — precondition broken")
	}

	// Schedule a past-due identity timer directly through the schedule minter (the
	// same handle a caps-injected cell would hold). The engine — already Started in
	// Open, Reviver wired — must revive the author and append the fire.
	handle := h.schedMinter.Mint(author)
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: h.nowMs() - 1, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	wantID := message.ID("timer:" + string(id))
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		found := false
		for _, r := range rows {
			if r.Envelope.ID == wantID {
				found = true
				if r.Envelope.Sender.ID != author {
					t.Fatalf("fire sender = %q, want %q (pen-welded author)", r.Envelope.Sender.ID, author)
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("identity timer fire never landed in truth (Reviver-then-append boot path broken)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live(h, author) {
		t.Fatal("author not live after fire — the Reviver did not activate an embodiment before append")
	}
}

// Test 5 — F1 regression: an identity-timer fire for an ALREADY-LIVE author must
// succeed even when NO Builder is wired. EnsureLive checks liveness BEFORE the
// builder gate — the builder is only needed to activate an ABSENT author. Before
// the fix a live author + nil builder returned ReviveRejected{no_builder}, poison-
// deleting a legitimate live author's timer row.
func TestReviver_LiveAuthorWithNilBuilderIsNotPoisoned(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "revive-nobuilder.sqlite")
	// Desired + Builder deliberately nil: a legal nil-builder home.
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	const author = actor.ActorID("agent:live-nobuilder")
	if err := h.Spawn(ctx, author, actor.KindAgent, func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}); err != nil {
		t.Fatalf("Spawn live author: %v", err)
	}
	if !live(h, author) {
		t.Fatal("author not live after Spawn — precondition broken")
	}

	// Live author, nil builder → idempotent no-op, NOT a ReviveRejected poison.
	if err := (homeReviver{h: h}).EnsureLive(ctx, author); err != nil {
		t.Fatalf("EnsureLive(live author, nil builder) = %v, want nil (must not poison a live author's timer)", err)
	}

	// Contrast: an ABSENT author with nil builder is still a ReviveRejected — the
	// builder gate correctly guards the activation (absent) path.
	var rejected schedule.ReviveRejected
	if err := (homeReviver{h: h}).EnsureLive(ctx, actor.ActorID("agent:absent")); !errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(absent author, nil builder) = %v, want ReviveRejected", err)
	}
}

// Test 6 — F2 teardown ordering: Close quiesces schedule PRODUCERS (cells) before
// the schedule engine, and completes without deadlock even with an in-memory
// (incarnation-bind) timer parked in the engine. Regression guard for the reorder
// (producers-before-consumer): the old "engine first" order left a window where a
// still-live cell could Schedule() an in-memory timer into a dead run loop.
func TestClose_ProducersBeforeEngineNoDeadlock(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:sched")
	builder.byID[id] = builder.recordFactory(id)

	dbPath := filepath.Join(t.TempDir(), "close-order.sqlite")
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// NB: Close is called explicitly below (not via t.Cleanup) — a second Close
	// would double-close the engine's stop channel and panic.

	// Bring a cell live and park a far-future incarnation-bind timer in the engine's
	// in-memory family (so the engine holds live schedule state at Close time).
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("author not revived")
	}
	if _, err := h.schedMinter.Mint(id).Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIncarnation, FireAt: h.nowMs() + 1_000_000, Type: "demo.tick",
	}); err != nil {
		t.Fatalf("Schedule incarnation timer: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- h.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete within 10s — teardown deadlock")
	}
}
